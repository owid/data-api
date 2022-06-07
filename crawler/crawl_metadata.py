import json
from pathlib import Path
from typing import Any, Dict, cast, List

import pandas as pd
import structlog
import typer
from duckdb_models import MetaTableModel, MetaVariableModel, db_init
from owid.catalog import RemoteCatalog, Table
from owid.catalog.catalogs import CatalogFrame, CatalogSeries
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session

REQUIRED_DIMENSIONS = {"year", "entity_name", "entity_code", "entity_id"}

log = structlog.get_logger()


def _load_catalog_frame(channels=()) -> CatalogFrame:
    frame = RemoteCatalog(channels=channels).frame
    # TODO: move dimension parsing into CatalogFrame
    frame["dimensions"] = frame["dimensions"].map(json.loads)
    return frame


def _omit_nullable_values(d: dict) -> dict:
    return {k: v for k, v in d.items() if v or v == 0}


def _load_table_data_into_db(t: CatalogSeries, table: Table, table_db_name: str, con):
    table_path = t.path
    # size_mb = table_path.stat().st_size / 1e6
    log.info(
        "loading_table",
        path=table_path,
        # size=f"{size_mb:.2f} MB",
        shape=table.shape,
    )

    # TODO: remove these comments once we make sure we're able to parse large files
    # if size_mb >= 10:
    #     log.info("skipping_large_table", path=table_path)
    #     continue

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
    }
    for dtype_from, dtype_to in DTYPE_MAP.items():
        df = df.astype({c: dtype_to for c in df.select_dtypes(dtype_from).columns})

    con.execute("register", ("t", df))
    con.execute(f"CREATE OR REPLACE TABLE {table_db_name} AS SELECT * FROM t")


def _variable_types(con, table_name) -> dict:
    mf = pd.read_sql(f"PRAGMA table_info('{table_name}')", con)
    return mf.set_index("name")["type"].to_dict()


def _tables_sync_actions(engine, frame):
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
    v: dict,
    short_name: str,
    variable_type: str,
    table_db_name: str,
    table_path: str,
    engine: Engine,
) -> MetaVariableModel:
    v["grapher_meta"] = v.pop("additional_info", {}).get("grapher_meta", {})
    v["variable_id"] = v["grapher_meta"].get("id")
    v["short_name"] = short_name
    v["table_path"] = table_path
    v["table_db_name"] = table_db_name
    v["variable_type"] = variable_type

    # TODO: we end up with lot of duplicates across variables (especially for the huge backported datasets)
    #   how about we only do it for every dataset? (we'd be returning even years for which the given variable
    #   doesn't have any data)
    # get used years from a dataframe
    q = f"""
    select distinct year from {table_db_name} where {short_name} is not null
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
    from {table_db_name} where {short_name} is not null
    """
    mf = pd.read_sql(q, engine)
    v["entities_values"] = json.dumps(mf.to_dict(orient="list"))

    v["sources"] = json.dumps(v.get("sources", []))
    v["grapher_meta"] = json.dumps(v.get("grapher_meta", {}))

    return MetaVariableModel(**_omit_nullable_values(v))


def _delete_table(table_path: str, session: Session):
    session.query(MetaTableModel).filter_by(path=table_path).delete()
    session.query(MetaVariableModel).filter_by(table_path=table_path).delete()


def main(duckdb_path: Path = Path("duck.db"), dataset_id: List[int] = []) -> None:
    """Bake ETL catalog into DuckDB."""
    engine = db_init(duckdb_path)
    session = Session(bind=engine)

    frame = _load_catalog_frame(channels=("backport",))

    if dataset_id:
        all_ids = frame.dataset.str.extract("dataset_(\d+)_", expand=False).astype(int)
        frame = frame.loc[all_ids.isin(dataset_id)]

    # which tables to delete and which to create
    table_paths_to_delete, table_paths_to_create = _tables_sync_actions(engine, frame)

    # if using specific dataset ids, don't delete any other datasets
    if dataset_id:
        table_paths_to_delete = table_paths_to_delete & table_paths_to_create

    log.info(
        "duckdb.actions",
        delete_tables=len(table_paths_to_delete),
        create_tables=len(table_paths_to_create),
    )

    for _, t in frame[frame.path.isin(table_paths_to_create)].iterrows():
        log.info("table.create", path=t.path, table=t.table)

        t = cast(CatalogSeries, t)

        missing_dims = REQUIRED_DIMENSIONS - set(t["dimensions"])
        assert not missing_dims, f"Missing dimensions: {missing_dims}"

        # download data and metadata from remote catalog
        log.info("table.download.start", path=t.path)
        table = t.load()
        log.info("table.download.end", path=t.path)

        # delete table and variables if they exist
        if t.path in table_paths_to_delete:
            _delete_table(t.path, session)
            table_paths_to_delete.remove(t.path)

        # TODO: this could be autocomputed property on the model
        # TODO: path could be very long, but how do we guarantee uniqueness of table name
        #   across datasets? or should we just go with table name and use full path only
        #   for non-unique table names?
        t["table_db_name"] = t.path.replace("/", "__")

        m = MetaTableModel(**t)
        session.add(m)

        # load data into DuckDB
        _load_table_data_into_db(t, table, t["table_db_name"], engine)

        # get variable types from DB
        # TODO: we could get it easily from `table`
        variable_types = _variable_types(engine, t["table_db_name"])

        # table with variables
        for variable_short_name, variable_meta in table._fields.items():
            if variable_short_name in REQUIRED_DIMENSIONS:
                continue

            m = _parse_meta_variable(
                # TODO: use VariableMeta directly
                variable_meta.to_dict(),
                variable_short_name,
                variable_types[variable_short_name],
                t["table_db_name"],
                t["path"],
                engine,
            )
            session.add(m)

        # commit changes for one table
        session.commit()

    # delete the rest of the tables
    if table_paths_to_delete:
        log.info("table.delete_tables", n=len(table_paths_to_delete))
        for table_path in table_paths_to_delete:
            _delete_table(table_path, session)

    session.close()


if __name__ == "__main__":
    typer.run(main)
