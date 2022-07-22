# TODO: rename this file to crawl.py (do it in a single commit to avoid conflicts)
import urllib.error
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Set, Tuple, cast

import pandas as pd
import structlog
import typer
from duckdb_models import MetaDatasetModel, MetaTableModel, MetaVariableModel, db_init
from owid.catalog import DatasetMeta, RemoteCatalog, Table, VariableMeta
from owid.catalog.catalogs import CatalogFrame, CatalogSeries
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session

from full_text_index import main as create_full_text_index

log = structlog.get_logger()


# duckdb does not support NaN in categories, use a special symbol instead
CATEGORY_NAN = "-"


def _load_catalog_frame(channels=()) -> CatalogFrame:
    frame = RemoteCatalog(channels=channels).frame

    # only public data
    frame = frame.loc[frame["is_public"]]

    return frame


def _fillna_for_categories(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes("category").columns:
        if df[col].isnull().any():
            df[col] = df[col].cat.add_categories(CATEGORY_NAN).fillna(CATEGORY_NAN)
    return df


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

    df = _fillna_for_categories(df)

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

    # NOTE: Int64 type is stored in DuckDB as NULLABLE BIGINT, because it is nullable
    # calling `fetch_df` on a query converts it to float64 instead of Int64 which is confusing

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
    # sometimes `unit` is missing, but there is display.unit
    if (var_meta.unit == "") or pd.isnull(var_meta):
        var_meta.unit = (var_meta.display or {}).get("unit")

    v = MetaVariableModel(
        title=var_meta.title,
        description=var_meta.description,
        licenses=[license.to_dict() for license in var_meta.licenses],
        sources=[source.to_dict() for source in var_meta.sources],
        unit=var_meta.unit,
        short_unit=var_meta.short_unit,
        display=var_meta.display,
        grapher_meta=var_meta.additional_info["grapher_meta"]
        if var_meta.additional_info
        else None,
        variable_id=var_meta.additional_info["grapher_meta"]["id"]
        if var_meta.additional_info
        else None,
        short_name=short_name,
        table_path=m.path,
        table_db_name=m.table_db_name,
        variable_type=variable_type,
        dataset_short_name=dataset_short_name,
    )

    # NOTE: `read_sql` does not support categorical type, so we have to convert to varchar
    cols = [f"{dim}::VARCHAR as {dim}" for dim in m.dimensions]

    # TODO: we end up with lot of duplicates across variables (especially for the huge backported datasets)
    #   how about we only do it for every dataset? (we'd be returning even years for which the given variable
    #   doesn't have any data)
    q = f"""
    select distinct
        {','.join(cols)}
    from {m.table_db_name} where {short_name} is not null
    """

    mf = pd.read_sql(q, engine)
    v.dimension_values = {
        k: sorted(set(v)) for k, v in mf.to_dict(orient="list").items()
    }

    return v


def _delete_tables(table_path: str, session: Session) -> None:
    session.query(MetaTableModel).filter_by(path=table_path).delete()
    session.query(MetaVariableModel).filter_by(table_path=table_path).delete()


def _upsert_dataset(ds: DatasetMeta, channel: str, session: Session) -> None:
    """Update dataset in DB."""
    session.query(MetaDatasetModel).filter_by(short_name=ds.short_name).delete()
    session.commit()
    d = MetaDatasetModel.from_DatasetMeta(ds, channel)
    session.add(d)
    session.commit()


def _tables_updates(
    engine: Engine, frame: CatalogFrame, force: bool, include: Optional[str]
) -> Tuple[Set[str], Set[str]]:
    if force:
        table_paths_to_delete = table_paths_to_create = set(frame.path)
    else:
        # which tables to delete and which to create
        table_paths_to_delete, table_paths_to_create = _tables_sync_actions(
            engine, frame
        )

    # if using specific include pattern, don't delete any other datasets
    if include:
        table_paths_to_delete = table_paths_to_delete & table_paths_to_create

    return table_paths_to_delete, table_paths_to_create


def _load_data_from_catalog(catalog_row: CatalogSeries) -> Table:
    log.info("table.download.start", path=catalog_row["path"])
    try:
        data_table = catalog_row.load()
    except urllib.error.HTTPError as e:
        # TODO: this should happen very rarely only if data is not synced with catalog
        # we should raise an exception once we turn on backporting and make it fast enough
        assert (
            e.code != 403
        ), f"Dataset {catalog_row['path']} is private and returning 403"
        raise e

    log.info("table.download.end", path=catalog_row["path"])
    return data_table


def main(
    duckdb_path: Path = Path("duck.db"),
    include: Optional[str] = typer.Option(
        None, help="Include datasets matching this regex"
    ),
    force: bool = False,
) -> None:
    """Bake ETL catalog into DuckDB."""
    engine = db_init(duckdb_path)

    frame = _load_catalog_frame(channels=("backport", "garden"))

    # TODO: hotfix, remove after we fix this in ETL
    # NOTE: this should be fixed, right?
    # frame = cast(CatalogFrame, frame.dropna(subset=["dataset"]))

    if include:
        frame = frame.loc[frame.dataset.str.contains(include)]

    table_paths_to_delete, table_paths_to_create = _tables_updates(
        engine, frame, force, include
    )
    log.info(
        "duckdb.actions",
        delete_tables=len(table_paths_to_delete),
        create_tables=len(table_paths_to_create),
    )

    frame = frame.loc[frame.path.isin(table_paths_to_create)]

    for i, (_, catalog_row) in enumerate(frame.iterrows()):
        catalog_row = cast(CatalogSeries, catalog_row)

        t = MetaTableModel.from_CatalogSeries(catalog_row)

        log.info(
            "table.create",
            path=t.path,
            table_name=t.table_name,
            progress=f"{i + 1}/{len(frame)}",
        )

        data_table = _load_data_from_catalog(catalog_row)

        # delete table and variables before creating them if they exist
        # TODO: remove dataset too if there are no remaining tables
        if t.path in table_paths_to_delete:
            with new_session(engine) as session:
                _delete_tables(t.path, session)
            table_paths_to_delete.remove(t.path)

        with new_session(engine) as session:
            # add table
            session.add(t)

            # save dataset metadata alongside table, we could also create a separate table for datasets
            ds = data_table.metadata.dataset
            assert ds is not None

            # exceptions for backported channel
            if catalog_row.channel == "backport":
                ds.version = "latest"
                # all backported datasets are currently saved under `owid` namespace, we could be saving them in their
                # real namespaces, but that would imply non-trivial changes to backporting code in ETL
                ds.namespace = "owid"

            assert ds.short_name
            assert ds.version is not None

            # update dataset by deleting and recreating new one
            # TODO: channel should be ideally property of DatasetMeta
            _upsert_dataset(ds, str(t.channel), session)

            # load data into DuckDB
            _load_table_data_into_db(t, data_table, engine)

            # get variable types from DB
            # TODO: we could get it easily from `table`, but perhaps it is better from DB?
            variable_types = _variable_types(engine, t.table_db_name)

            # table with variables
            for variable_short_name, variable_meta in data_table._fields.items():
                if variable_short_name in t.dimensions:
                    continue

                v = _parse_meta_variable(
                    variable_meta,
                    t,
                    variable_short_name,
                    variable_types[variable_short_name],
                    ds.short_name,
                    engine,
                )
                log.info(
                    "table.variable.create", path=t.path, variable=variable_short_name
                )
                session.add(v)

        # delete the rest of the tables
        if table_paths_to_delete:
            log.info("table.delete_tables", n=len(table_paths_to_delete))
            for table_path in table_paths_to_delete:
                _delete_tables(table_path, session)

    # recreate full-text search index (this has to be run on every new dataset)
    create_full_text_index(duckdb_path)


@contextmanager
def new_session(engine) -> Generator[Session, None, None]:
    """Open new session and commit at the end without expiring objects.

    I couldn't make this work with creating only one session per table (tables did not have data for
    unknown reasons), so I'm creating new session for each operation which works. Feel free to fix
    this and make it transactional or switch to a different ORM.
    """
    # NOTE: should I do it with transaction, i.e. `with session.begin():`?
    #   there would be problems with commits in _upsert_dataset
    with Session(engine, expire_on_commit=False) as session:
        yield session
        session.commit()


if __name__ == "__main__":
    typer.run(main)
