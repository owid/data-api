import os
import urllib.error
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Set, Tuple, cast

import pandas as pd
import structlog
import typer
from duckdb_models import MetaDatasetModel, MetaTableModel, MetaVariableModel, db_init
from full_text_index import main as create_full_text_index
from owid.catalog import RemoteCatalog, Table, VariableMeta
from owid.catalog.catalogs import CatalogFrame, CatalogSeries
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session

log = structlog.get_logger()


# duckdb does not support NaN in categories, use a special symbol instead
CATEGORY_NAN = "-"


def _load_catalog_frame(channels=()) -> CatalogFrame:
    frame = RemoteCatalog(channels=channels).frame

    # only public data
    frame = frame.loc[frame["is_public"]]

    # add dataset path
    frame["dataset_path"] = frame.path.map(os.path.dirname)

    # TODO: exclude large datasets (we need to improve their performance)
    frame = frame[~frame.path.str.contains("garden/faostat/2022-05-17")]
    # frame = frame[~frame.path.str.contains("garden/un_sdg/2022-07-07/un_sdg")]

    # TODO: exclude datasets with missing versions
    frame = frame[~frame.path.str.contains("garden/faostat/2021-04-09")]
    frame = frame[~frame.path.str.contains("garden/owid/latest/key_indicators")]
    frame = frame[~frame.path.str.contains("garden/owid/latest/population_density")]
    frame = frame[
        ~frame.path.str.contains("garden/sdg/latest/sdg/sustainable_development_goal")
    ]
    frame = frame[~frame.path.str.contains("garden/worldbank_wdi/2022-05-26/wdi/wdi")]

    # TODO: weird error
    frame = frame[
        ~frame.path.str.contains("garden/shift/2022-07-18/fossil_fuel_production")
    ]

    # TODO: exclude special datasets for now
    frame = frame[~frame.path.str.contains("garden/reference/")]

    return frame


def _fillna_for_categories(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes("category").columns:
        if df[col].isnull().any():
            df[col] = df[col].cat.add_categories(CATEGORY_NAN).fillna(CATEGORY_NAN)
    return df


def _load_table_data_into_db(m: MetaTableModel, table: Table, con) -> None:
    table_path = m.path
    # size_mb = table_path.stat().st_size / 1e6
    log.info(
        "loading_table.start",
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

    log.info(
        "loading_table.end",
    )


def _variable_types(con, table_name) -> dict:
    mf = pd.read_sql(f"PRAGMA table_info('{table_name}')", con)
    return mf.set_index("name")["type"].to_dict()


def _dataset_sync_actions(
    engine: Engine, ds_path_to_checksum: dict[str, str]
) -> Tuple[Set[str], Set[str]]:
    q = """
    select
        path,
        checksum
    from meta_datasets
    """
    try:
        df = pd.read_sql(q, engine)
    except RuntimeError as e:
        if e.args[0].startswith(
            "Catalog Error: Table with name meta_datasets does not exist"
        ):
            df = pd.DataFrame(columns=["path", "checksum"])
        else:
            raise e

    # compute ids consisting of checksum and table name to know which ones to delete
    db_ids = {r.path: r.checksum for r in df.itertuples()}

    dataset_paths_to_delete = {
        path
        for path, checksum in db_ids.items()
        if checksum != ds_path_to_checksum.get(path)
    }
    dataset_paths_to_create = {
        path
        for path, checksum in ds_path_to_checksum.items()
        if checksum != db_ids.get(path)
    }

    return dataset_paths_to_delete, dataset_paths_to_create


def _parse_meta_variable(
    var_meta: VariableMeta,
    m: MetaTableModel,
    short_name: str,
    variable_type: str,
    dataset_short_name: str,
    data_table: Table,
    dataset_path: str,
) -> MetaVariableModel:
    # sometimes `unit` is missing, but there is display.unit
    if (var_meta.unit == "") or pd.isnull(var_meta):
        var_meta.unit = (var_meta.display or {}).get("unit")

    # TODO: we end up with lot of duplicates across variables (especially for the huge backported datasets)
    #   how about we only do it for every dataset? (we'd be returning even years for which the given variable
    #   doesn't have any data)
    data = data_table[short_name].dropna()
    dimension_values = {
        dim: sorted(set(data.index.get_level_values(dim).dropna()))
        for dim in data.index.names
    }

    return MetaVariableModel(
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
        dataset_path=dataset_path,
        dimension_values=dimension_values,
    )


def _delete_dataset(path: str, session: Session) -> None:
    session.query(MetaDatasetModel).filter_by(path=path).delete()
    session.query(MetaTableModel).filter_by(dataset_path=path).delete()
    session.query(MetaVariableModel).filter_by(dataset_path=path).delete()


def _datasets_updates(
    engine: Engine, frame: CatalogFrame, force: bool, include: Optional[str]
) -> Tuple[Set[str], Set[str]]:
    # dataset path to checksum from frame
    ds_path_to_checksum = {r.dataset_path: r.checksum for r in frame.itertuples()}

    if force:
        dataset_paths_to_delete = dataset_paths_to_create = set(
            ds_path_to_checksum.keys()
        )
    else:
        # which tables to delete and which to create
        dataset_paths_to_delete, dataset_paths_to_create = _dataset_sync_actions(
            engine, ds_path_to_checksum
        )

    # if using specific include pattern, don't delete any other datasets
    if include:
        dataset_paths_to_delete = dataset_paths_to_delete & dataset_paths_to_create

    return dataset_paths_to_delete, dataset_paths_to_create


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
    full_text_search: bool = True,
) -> None:
    """Bake ETL catalog into DuckDB."""
    engine = db_init(duckdb_path)

    frame = _load_catalog_frame(channels=("backport", "garden"))

    if include:
        frame = frame.loc[frame.dataset_path.str.contains(include)]

    dataset_paths_to_delete, dataset_paths_to_create = _datasets_updates(
        engine, frame, force, include
    )
    log.info(
        "duckdb.actions",
        delete_datasets=len(dataset_paths_to_delete),
        create_datasets=len(dataset_paths_to_create),
    )

    frame = frame.loc[frame.dataset_path.isin(dataset_paths_to_create)]

    for i, (dataset_path, dataset_frame) in enumerate(frame.groupby("dataset_path")):
        log.info(
            "dataset.create",
            path=dataset_path,
            progress=f"{i + 1}/{len(frame)}",
        )
        if dataset_path in dataset_paths_to_delete:
            # delete everything related to a dataset before recreating them
            with new_session(engine) as session:
                _delete_dataset(dataset_path, session)

        # NOTE: we need to grab from the first table we load, only insert the dataset
        # when we process the first table
        dataset_inserted = False

        for i, (_, catalog_row) in enumerate(dataset_frame.iterrows()):

            catalog_row = cast(CatalogSeries, catalog_row)

            t = MetaTableModel.from_CatalogSeries(catalog_row)

            log.info(
                "table.create",
                path=t.path,
                table_name=t.table_name,
            )

            data_table = _load_data_from_catalog(catalog_row)

            with new_session(engine) as session:
                # save dataset metadata alongside table, we could also create a separate table for datasets
                ds = data_table.metadata.dataset
                assert ds is not None

                # exceptions for backported channel
                if catalog_row.channel == "backport":
                    # backported datasets are missing version
                    ds.version = "latest"
                    # all backported datasets are currently saved under `owid` namespace, we could be saving them in their
                    # real namespaces, but that would imply non-trivial changes to backporting code in ETL
                    ds.namespace = "owid"

                assert ds.short_name
                if not ds.version:
                    log.error("missing.version", path=catalog_row["path"])
                    continue

                # add table
                session.add(t)

                # create dataset
                # TODO: channel should be ideally property of DatasetMeta
                if not dataset_inserted:
                    session.add(
                        MetaDatasetModel.from_DatasetMeta(
                            ds, dataset_path, dataset_checksum=catalog_row.checksum
                        )
                    )
                    dataset_inserted = True

                _load_table_data_into_db(t, data_table, engine)

                # get variable types from DB
                # NOTE: should we get it from data or from DB?
                variable_types = _variable_types(engine, t.table_db_name)
                # variable_types = data_table.reset_index().dtypes.astype(str).to_dict()

                # table with variables
                variables = []
                for variable_short_name, variable_meta in data_table._fields.items():
                    if variable_short_name in t.dimensions:
                        continue

                    variables.append(
                        _parse_meta_variable(
                            variable_meta,
                            t,
                            variable_short_name,
                            variable_types[variable_short_name],
                            ds.short_name,
                            data_table,
                            dataset_path,
                        )
                    )
                    log.info(
                        "table.variable.create",
                        variable=variable_short_name,
                    )

                session.add_all(variables)

    # delete the rest of the datasets
    if dataset_paths_to_delete:
        log.info("dataset.delete_datasets", n=len(dataset_paths_to_delete))
        with new_session(engine) as session:
            for dataset_path in dataset_paths_to_delete:
                _delete_dataset(dataset_path, session)

    if full_text_search:
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
