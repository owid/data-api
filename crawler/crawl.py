import json
import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional, Set, Tuple, cast

import pandas as pd
import pyarrow.parquet as pq
import structlog
import typer
from owid.catalog import RemoteCatalog, TableMeta, VariableMeta
from owid.catalog.catalogs import CatalogFrame, CatalogSeries
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session

from crawler.duckdb_models import (
    MetaDatasetModel,
    MetaTableModel,
    MetaVariableModel,
    db_init,
)
from crawler.full_text_index import main as create_full_text_index

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


def _variable_types(con, parquet_path) -> dict:
    q = f"""
    select
        name,
        type
    from parquet_schema('{parquet_path}')
    """
    mf = pd.read_sql(q, con)
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
    dataset_path: str,
) -> MetaVariableModel:
    # sometimes `unit` is missing, but there is display.unit
    if (var_meta.unit == "") or pd.isnull(var_meta):
        var_meta.unit = (var_meta.display or {}).get("unit")

    # if there is backported variable in non-backported dataset, remove its grapher
    # metadata to make sure we don't have duplicate variable ids in DB
    channel = dataset_path.split("/")[0]
    if channel != "backport" and var_meta.additional_info:
        var_meta.additional_info.pop("grapher_meta", None)

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
        variable_type=variable_type,
        dataset_short_name=dataset_short_name,
        dataset_path=dataset_path,
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


def _extract_dimension_values(
    parquet_path: str, dims_to_process: Set[str], engine
) -> dict[str, Any]:
    dimension_values = {}

    q = f"""
    select distinct {",".join(list(dims_to_process))}
    from read_parquet('{parquet_path}')
    """
    df = pd.read_sql(q, engine)

    # entities belong together and has to be stored as tuple `entity_id|entity_name|entity_code`
    # NOTE: this might be generalized to any column name with `_id`, `_name`, `_code` suffix
    if {"entity_id", "entity_name", "entity_code"} <= dims_to_process:
        dims_to_process = dims_to_process - {"entity_id", "entity_name", "entity_code"}
        index_vals = sorted(
            set(
                zip(
                    df["entity_id"],
                    df["entity_name"],
                    df["entity_code"],
                )
            )
        )

        dimension_values = {
            "entity_zip": sorted(["|".join(map(str, x)) for x in index_vals]),
        }

    for dim in dims_to_process:
        dimension_values[dim] = sorted(set(df[dim]))

    return dimension_values


def _read_parquet_metadata(
    parquet_path: Path,
) -> tuple[TableMeta, dict[str, VariableMeta]]:
    meta = pq.read_metadata(parquet_path)
    table_meta = TableMeta.from_json(meta.metadata[b"owid_table"])  # type: ignore

    owid_fields = json.loads(meta.metadata[b"owid_fields"])
    fields_meta = {f: VariableMeta.from_dict(v) for f, v in owid_fields.items()}

    return table_meta, fields_meta


def main(
    duckdb_path: Path = Path("duck.db"),
    owid_catalog_dir: Path = Path("../etl/data"),
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
            dataset_paths_to_delete.remove(dataset_path)

        # NOTE: we need to grab from the first table we load, only insert the dataset
        # when we process the first table
        dataset_inserted = False

        for i, (_, catalog_row) in enumerate(dataset_frame.iterrows()):

            catalog_row = cast(CatalogSeries, catalog_row)

            log.info(
                "table.load_from_catalog",
                path=catalog_row.path,
            )

            parquet_path = (owid_catalog_dir / catalog_row.path).with_suffix(".parquet")

            table_meta, fields_meta = _read_parquet_metadata(parquet_path)

            # NOTE: this requires reading parquet file, which could be slow. We could instead write
            # dimensions values to metadata when generating the parquet file.
            dimension_values = _extract_dimension_values(
                parquet_path, set(catalog_row.dimensions), engine
            )

            t = MetaTableModel.from_CatalogSeries(catalog_row, dimension_values)

            log.info(
                "table.create",
                path=t.path,
            )

            with new_session(engine) as session:
                # save dataset metadata alongside table, we could also create a separate table for datasets
                ds = table_meta.dataset
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

                # get variable types from DB
                assert t.path
                variable_types = _variable_types(engine, parquet_path)

                # table with variables
                variables = []
                for variable_short_name, variable_meta in fields_meta.items():
                    if variable_short_name in t.dimensions:
                        continue

                    variables.append(
                        _parse_meta_variable(
                            variable_meta,
                            t,
                            variable_short_name,
                            variable_types[variable_short_name],
                            ds.short_name,
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


def main_cli():
    return typer.run(main)


if __name__ == "__main__":
    main_cli()
