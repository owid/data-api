import json
import urllib.error
from pathlib import Path
from typing import Any, Dict, cast, List, Tuple, Set

import pandas as pd
from collections.abc import Generator
import structlog
import typer
from duckdb_models import MetaTableModel, MetaDatasetModel, MetaVariableModel, db_init
from owid.catalog import RemoteCatalog, Table, DatasetMeta, VariableMeta
from owid.catalog.catalogs import CatalogFrame, CatalogSeries
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session

from contextlib import contextmanager

REQUIRED_DIMENSIONS = {"year", "entity_name", "entity_code", "entity_id"}

log = structlog.get_logger()


def _load_catalog_frame(channels=()) -> CatalogFrame:
    frame = RemoteCatalog(channels=channels).frame
    # TODO: move dimension parsing into CatalogFrame
    frame["dimensions"] = frame["dimensions"].map(json.loads)

    # only public data
    frame = frame.loc[frame["is_public"]]

    return frame


def _omit_nullable_values(d: dict) -> dict:
    return {k: v for k, v in d.items() if v or v == 0}


def _load_table_data_into_db(m: MetaTableModel, table: Table, con):
    table_path = m.path
    # size_mb = table_path.stat().st_size / 1e6
    log.info(
        "loading_table",
        path=table_path,
        # size=f"{size_mb:.2f} MB",
        shape=table.shape,
    )

    df = pd.DataFrame(table).reset_index()

    # duckdb does not support NaN in categories
    df.entity_code = df.entity_code.cat.add_categories("-").fillna("-")

    # unsigned integers are not supported, convert them to signed with higher range
    # NOTE: this a workaround, it worked with loading from feather object, but it does
    #  not work for pandas for some reason. It would be nice to keep them as unsigned
    #  integers
    DTYPE_MAP = {
        "UInt32": "Int64",
        "UInt16": "Int32",
        "UInt8": "Int16",
    }
    for dtype_from, dtype_to in DTYPE_MAP.items():
        df = df.astype({c: dtype_to for c in df.select_dtypes(dtype_from).columns})

    con.execute("register", ("t", df))
    con.execute(f"CREATE OR REPLACE TABLE {m.table_db_name} AS SELECT * FROM t")


def _variable_types(con, table_name) -> dict:
    mf = pd.read_sql(f"PRAGMA table_info('{table_name}')", con)
    return mf.set_index("name")["type"].to_dict()


def _tables_sync_actions(
    engine: Engine, frame: CatalogFrame
) -> Tuple[Set[str], Set[str]]:
    q = """
    select
        path,
        checksum
    from meta_tables
    """
    try:
        df = pd.read_sql(q, engine)
    except RuntimeError as e:
        if e.args[0].startswith(
            "Catalog Error: Table with name meta_tables does not exist"
        ):
            df = pd.DataFrame(columns=["path", "checksum"])
        else:
            raise e

    # compute ids consisting of checksum and table name to know which ones to delete
    db_ids = df.path + df.checksum
    catalog_ids = frame.path + frame.checksum

    table_paths_to_delete = set(df.path[~db_ids.isin(catalog_ids)])
    table_paths_to_create = set(frame.path[~catalog_ids.isin(db_ids)])

    return table_paths_to_delete, table_paths_to_create


def _parse_meta_variable(
    var_meta: VariableMeta,
    m: MetaTableModel,
    short_name: str,
    variable_type: str,
    dataset_short_name: str,
    engine: Engine,
) -> MetaVariableModel:
    assert var_meta.additional_info
    v = dict(
        title=var_meta.title,
        description=var_meta.description,
        # TODO: how to convert the entire `VariableMeta` object to JSON/dict?
        sources=[s.to_dict() for s in var_meta.sources],
        licenses=var_meta.licenses,
        unit=var_meta.unit,
        short_unit=var_meta.short_unit,
        display=var_meta.display,
        grapher_meta=var_meta.additional_info["grapher_meta"],
        # TODO: refactor these custom attributes
        short_name=short_name,
        table_path=m.path,
        table_db_name=m.table_db_name,
        variable_type=variable_type,
        dataset_short_name=dataset_short_name,
    )
    v["variable_id"] = v["grapher_meta"].get("id")  # type: ignore

    # TODO: we end up with lot of duplicates across variables (especially for the huge backported datasets)
    #   how about we only do it for every dataset? (we'd be returning even years for which the given variable
    #   doesn't have any data)
    # get used years from a dataframe
    q = f"""
    select distinct year from {m.table_db_name} where {short_name} is not null
    """
    mf = pd.read_sql(q, engine)
    v["years_values"] = json.dumps(mf.year.tolist())

    # entities_values
    # NOTE: `read_sql` does not support categorical type, so we have to convert to varchar
    q = f"""
    select distinct
        entity_id,
        entity_code::VARCHAR as entity_code,
        entity_name::VARCHAR as entity_name
    from {m.table_db_name} where {short_name} is not null
    """
    mf = pd.read_sql(q, engine)
    v["entities_values"] = json.dumps(mf.to_dict(orient="list"))

    v["sources"] = json.dumps(v.get("sources", []))
    v["licenses"] = json.dumps(v.get("licenses", []))
    v["grapher_meta"] = json.dumps(v.get("grapher_meta", {}))
    v["display"] = json.dumps(v.get("display", {}))

    # assert that we don't have any extra columns
    extra_keys = v.keys() - set(MetaVariableModel._sa_class_manager.keys())
    assert not extra_keys, f"Extra keys {extra_keys}"

    return MetaVariableModel(**_omit_nullable_values(v))


def _delete_tables(table_path: str, session: Session) -> None:
    session.query(MetaTableModel).filter_by(path=table_path).delete()
    session.query(MetaVariableModel).filter_by(table_path=table_path).delete()


def _upsert_dataset(ds: DatasetMeta, session: Session) -> None:
    """Update dataset in DB."""
    assert ds.additional_info

    session.query(MetaDatasetModel).filter_by(short_name=ds.short_name).delete()
    session.commit()
    d = MetaDatasetModel(
        short_name=ds.short_name,
        namespace=ds.namespace,
        title=ds.title,
        description=ds.description,
        sources=ds.sources,
        licenses=ds.licenses,
        is_public=ds.is_public,
        source_checksum=ds.source_checksum,
        grapher_meta=json.dumps(ds.additional_info["grapher_meta"]),
        version=ds.version,
    )
    session.add(d)
    session.commit()


def main(
    duckdb_path: Path = Path("duck.db"), dataset_id: List[int] = [], force: bool = False
) -> None:
    """Bake ETL catalog into DuckDB."""
    engine = db_init(duckdb_path)

    frame = _load_catalog_frame(channels=("backport",))

    if dataset_id:
        all_ids = frame.dataset.str.extract("dataset_(\d+)_", expand=False).astype(int)
        frame = frame.loc[all_ids.isin(dataset_id)]

    if force:
        table_paths_to_delete = table_paths_to_create = set(frame.path)
    else:
        # which tables to delete and which to create
        table_paths_to_delete, table_paths_to_create = _tables_sync_actions(
            engine, frame
        )

    # if using specific dataset ids, don't delete any other datasets
    if dataset_id:
        table_paths_to_delete = table_paths_to_delete & table_paths_to_create

    log.info(
        "duckdb.actions",
        delete_tables=len(table_paths_to_delete),
        create_tables=len(table_paths_to_create),
    )

    frame = frame.loc[frame.path.isin(table_paths_to_create)]

    for i, (_, t) in enumerate(frame.iterrows()):
        log.info(
            "table.create", path=t.path, table=t.table, progress=f"{i + 1}/{len(frame)}"
        )

        t = cast(CatalogSeries, t)

        missing_dims = REQUIRED_DIMENSIONS - set(t["dimensions"])
        # TODO: this should raise an error, but we need to be deleting zombie data first
        # assert not missing_dims, f"Missing dimensions: {missing_dims}"
        if missing_dims:
            log.warning(
                "table.missing_dimensions",
                path=t.path,
            )
            continue

        # download data and metadata from remote catalog
        log.info("table.download.start", path=t.path)
        try:
            table = t.load()
        except urllib.error.HTTPError as e:
            # TODO: this should happen very rarely only if data is not synced with catalog
            # we should raise an exception once we turn on backporting and make it fast enough
            if e.code == 403:
                log.warning(
                    "table.private_dataset",
                    path=t.path,
                )
                continue
            else:
                raise e

        log.info("table.download.end", path=t.path)

        # delete table and variables if they exist
        if t.path in table_paths_to_delete:
            with new_session(engine) as session:
                _delete_tables(t.path, session)
            table_paths_to_delete.remove(t.path)

        # rename some columns to match DuckDB schema
        t = t.rename(
            {
                "table": "table_name",
                "dataset": "dataset_name",
            }
        )

        # save dataset metadata alongside table, we could also create a separate table for datasets
        ds = table.metadata.dataset
        assert ds is not None
        assert ds.short_name

        with new_session(engine) as session:
            m = MetaTableModel(**t.to_dict())
            session.add(m)

            # update dataset by deleting and recreating new one
            _upsert_dataset(ds, session)

        # load data into DuckDB
        _load_table_data_into_db(m, table, engine)

        # get variable types from DB
        # TODO: we could get it easily from `table`, but perhaps it is better from DB?
        variable_types = _variable_types(engine, m.table_db_name)

        with new_session(engine) as session:
            # table with variables
            for variable_short_name, variable_meta in table._fields.items():
                if variable_short_name in REQUIRED_DIMENSIONS:
                    continue

                v = _parse_meta_variable(
                    variable_meta,
                    m,
                    variable_short_name,
                    variable_types[variable_short_name],
                    ds.short_name,
                    engine,
                )
                log.info(
                    "table.variable.create", path=m.path, variable=variable_short_name
                )
                session.add(v)
                break

    # delete the rest of the tables
    if table_paths_to_delete:
        log.info("table.delete_tables", n=len(table_paths_to_delete))
        with new_session(engine) as session:
            for table_path in table_paths_to_delete:
                _delete_tables(table_path, session)


@contextmanager
def new_session(engine) -> Generator[Session, None, None]:
    """Open new session and commit at the end without expiring objects.

    I couldn't make this work with creating only one session per table (tables did not have data for
    unknown reasons), so I'm creating new session for each operation which works. Feel free to fix
    this and make it transactional or switch to a different ORM.
    """
    with Session(engine, expire_on_commit=False) as session:
        yield session
        session.commit()


if __name__ == "__main__":
    typer.run(main)
