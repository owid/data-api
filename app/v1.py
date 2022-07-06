import functools
import json
import threading
from typing import Any, Dict, cast

import duckdb
import numpy as np
import pandas as pd
import structlog
from fastapi import FastAPI, HTTPException

from app import utils
from app.core.config import settings
from app.schemas.v1 import (
    Dimension,
    DimensionProperties,
    VariableDataResponse,
    VariableMetadataResponse,
    VariableSource,
)
from crawler.utils import sanitize_table_path

log = structlog.get_logger()


# NOTE: duckdb also supports python relations, would it be helpful?
# https://github.com/duckdb/duckdb/blob/master/examples/python/duckdb-python.py


v1 = FastAPI(default_response_class=utils.ORJSONResponse)


@functools.cache
def get_readonly_connection(thread_id: int) -> duckdb.DuckDBPyConnection:
    # duckdb connection is not threadsafe, we have to create one connection per thread
    log.info("duckdb.new_connection", thread_id=thread_id)
    return duckdb.connect(database=settings.DUCKDB_PATH.as_posix(), read_only=True)


def _assert_single_variable(n, variable_id):
    if n == 0:
        raise HTTPException(
            status_code=404, detail=f"variable_id {variable_id} not found"
        )
    elif n > 1:
        # raise internal error
        raise Exception(
            f"multiple variables found for variable_id {variable_id}, this should not happen"
        )


# QUESTION: how about /variable/{variable_id}/data?
@v1.get(
    "/variableById/data/{variable_id}",
    response_model=VariableDataResponse,
    response_model_exclude_unset=True,
)
def data_for_backported_variable(variable_id: int, limit: int = 1000000000):
    """Fetch data for a single variable."""

    con = get_readonly_connection(threading.get_ident())

    # get meta about variable
    q = """
    select
        variable_id,
        short_name,
        table_db_name
    from meta_variables
    where variable_id = (?)
    """
    df = cast(pd.DataFrame, con.execute(q, parameters=[variable_id]).fetch_df())
    _assert_single_variable(df.shape[0], variable_id)
    r = dict(df.iloc[0])

    # TODO: DuckDB / SQLite doesn't allow parameterized table or column names, how do we escape it properly?
    # is it even needed if we get them from our DB and it is read-only?
    q = f"""
    select
        year,
        entity_name,
        entity_id,
        entity_code,
        {r["short_name"]} as value
    from {r["table_db_name"]}
    where {r["short_name"]} is not null
    limit (?)
    """
    df = cast(pd.DataFrame, con.execute(q, parameters=[limit]).fetch_df())
    return df.to_dict(orient="list")


@v1.get(
    "/dataset/data/{channel}/{namespace}/{version}/{dataset}/{table}",
)
def data_for_etl_variable(
    channel: str,
    namespace: str,
    version: str,
    dataset: str,
    table: str,
    limit: int = 1000000000,
):
    """Fetch data for a single variable."""

    con = get_readonly_connection(threading.get_ident())

    table_db_name = sanitize_table_path(
        f"{channel}/{namespace}/{version}/{dataset}/{table}"
    )

    q = f"""
    select
        *
    from {table_db_name}
    limit (?)
    """
    df = cast(pd.DataFrame, con.execute(q, parameters=[limit]).fetch_df())
    # TODO: converting to lists and then ormjson is slow, we could instead
    # convert to numpy arrays on which ormjson is super fast
    return df.to_dict(orient="list")


def omit_nullable_values(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None and not pd.isna(v)}


def _parse_dimension_values(dimension_values: Any) -> Dict[str, Dimension]:
    dimensions = {}

    # NOTE: we have inconsistency with plurals - even though the dimension name is
    # singular, we use plural in the API (but not for custom dimensions)
    if "year" in dimension_values:
        dimensions["years"] = Dimension(
            type="int",
            values=[DimensionProperties(id=y) for y in dimension_values.pop("year")],
        )

    # special case of entities backported variables with entities and their codes
    if {"entity_id", "entity_name", "entity_code"} <= set(dimension_values.keys()):
        dimensions["entities"] = Dimension(
            type="int",
            values=[
                DimensionProperties(id=int(e[0]), name=e[1], code=e[2])
                for e in zip(
                    dimension_values.pop("entity_id"),
                    dimension_values.pop("entity_name"),
                    dimension_values.pop("entity_code"),
                )
            ],
        )

    # process remaining dimensions
    for dim in dimension_values.keys():
        raise NotImplementedError()
        # dimensions[dim] = Dimension(
        #     type=variable_type,
        #     values=[DimensionProperties(id=y) for y in set(dimension_values.pop(dim))],
        # )

    return dimensions


def _metadata_etl_variables(con, table_path):
    q = """
    SELECT
        -- variables (commented columns are not relevant for ETL tables)
        v.title,
        v.description,
        v.licenses,
        v.sources,
        v.unit,
        v.short_unit,
        -- v.display,
        -- v.grapher_meta,
        -- v.variable_id,
        v.short_name,
        v.table_path,
        v.table_db_name,
        v.dataset_short_name,
        v.variable_type,
        -- TODO: should we include `dimension_values` in response or do we only need it for backported variables?
        -- v.dimension_values,
    FROM meta_variables as v
    WHERE v.table_path = (?)
    """

    # TODO: this is a hacky and slow way to do it, use ORM or proper dataclass instead
    vf = cast(pd.DataFrame, con.execute(q, parameters=[table_path]).fetch_df())

    # convert JSON to dict (should be done automatically once we switch to ORM)
    for col in ("licenses", "sources"):
        vf[col] = vf[col].apply(json.loads)
    return vf


def _metadata_etl_table(con, table_path):
    q = """
    SELECT
        table_name,
        dataset_name,
        table_db_name,
        version,
        namespace,
        channel,
        checksum,
        dimensions,
        path,
        format,
        is_public,
    FROM meta_tables as t
    WHERE path = (?)
    """

    # TODO: this is a hacky and slow way to do it, use ORM or proper dataclass instead
    tf = cast(pd.DataFrame, con.execute(q, parameters=[table_path]).fetch_df())

    for col in ("dimensions",):
        tf[col] = tf[col].apply(json.loads)
    return tf


def _metadata_etl_dataset(con, channel, namespace, version, dataset):
    q = """
    SELECT
        channel,
        namespace,
        short_name,
        title,
        description,
        sources,
        licenses,
        is_public,
        source_checksum,
        version,
        -- grapher_meta
    FROM meta_datasets as d
    -- TODO: we might want to use path instead of separate columns
    WHERE channel = (?) and namespace = (?) and version = (?) and short_name = (?)
    """

    df = cast(
        pd.DataFrame,
        con.execute(
            q,
            parameters=[
                channel,
                namespace,
                version,
                dataset,
            ],
        ).fetch_df(),
    )

    for col in ("sources", "licenses"):
        df[col] = df[col].apply(json.loads)

    return df


@v1.get(
    "/dataset/metadata/{channel}/{namespace}/{version}/{dataset}/{table}",
    # response_model=VariableMetadataResponse,
    # response_model_exclude_unset=True,
)
def metadata_for_etl_variable(
    channel: str,
    namespace: str,
    version: str,
    dataset: str,
    table: str,
):
    table_path = f"{channel}/{namespace}/{version}/{dataset}/{table}"

    con = get_readonly_connection(threading.get_ident())

    vf = _metadata_etl_variables(con, table_path)
    tf = _metadata_etl_table(con, table_path)
    df = _metadata_etl_dataset(con, channel, namespace, version, dataset)

    return {
        "variables": vf.to_dict(orient="records"),
        "table": tf.iloc[0].to_dict(),
        "dataset": df.iloc[0].to_dict(),
    }


# QUESTION: how about `/variable/{variable_id}/metadata` naming?
@v1.get(
    "/variableById/metadata/{variable_id}",
    response_model=VariableMetadataResponse,
    response_model_exclude_unset=True,
)
def metadata_for_backported_variable(variable_id: int):
    """Fetch metadata for a single variable from database.
    This function is identical to Variables.getVariableData in owid-grapher repository
    """
    q = """
    SELECT
        -- variables
        v.grapher_meta->>'$.name' as name,
        v.grapher_meta->>'$.unit' as unit,
        v.grapher_meta->>'$.description' as description,
        v.grapher_meta->>'$.createdAt' as createdAt,
        v.grapher_meta->>'$.updatedAt' as updatedAt,
        v.grapher_meta->>'$.code' as code,
        v.grapher_meta->>'$.coverage' as coverage,
        v.grapher_meta->>'$.timespan' as timespan,
        (v.grapher_meta->>'$.datasetId')::integer as datasetId,
        (v.grapher_meta->>'$.sourceId')::integer as sourceId,
        v.grapher_meta->>'$.shortUnit' as shortUnit,
        v.grapher_meta->>'$.display' as display,
        (v.grapher_meta->>'$.columnOrder')::integer as columnOrder,
        v.grapher_meta->>'$.originalMetadata' as originalMetadata,
        v.grapher_meta->>'$.grapherConfig' as grapherConfig,
        -- dataset
        d.grapher_meta->>'$.name' as datasetName,
        IF(d.grapher_meta->>'$.nonRedistributable' = 'true', true, false) as nonRedistributable,
        -- there should be always only one source for variable
        -- this is inverse of `convert_grapher_source`
        v.sources->>'$[0].name' as sourceName,
        v.sources->>'$[0].description' as sourceAdditionalInfo,
        v.sources->>'$[0].date_accessed' as sourceRetrievedDate,
        v.sources->>'$[0].url' as sourceLink,
        v.sources->>'$[0].publisher_source' as sourceDataPublisherSource,
        v.sources->>'$[0].published_by' as sourceDataPublishedBy,
    FROM meta_variables as v
    JOIN meta_datasets as d ON d.short_name = v.dataset_short_name
    WHERE v.variable_id = (?)
    """
    con = get_readonly_connection(threading.get_ident())

    # TODO: this is a hacky and slow way to do it, use ORM or proper dataclass instead
    df = cast(pd.DataFrame, con.execute(q, parameters=[variable_id]).fetch_df())

    # null values in JSON string functions end up as "null" string, fix that
    df = df.replace("null", np.nan)
    row = df.iloc[0].to_dict()

    source = VariableSource(
        id=row.pop("sourceId"),
        name=row.pop("sourceName"),
        dataPublishedBy=row.pop("sourceDataPublishedBy", ""),
        dataPublisherSource=row.pop("sourceDataPublisherSource", ""),
        link=row.pop("sourceLink", ""),
        retrievedDate=row.pop("sourceRetrievedDate", ""),
        additionalInfo=row.pop("sourceAdditionalInfo", ""),
    )

    nonRedistributable = row.pop("nonRedistributable")
    displayJson = row.pop("display")
    variable = omit_nullable_values(row)

    # get variable types from duckdb (all metadata would be eventually retrieved in duckdb)
    # NOTE: getting these is a bit of a pain, we have a lot of duplicate information
    # in our DB
    q = """
    select
        variable_type,
        dimension_values
    from meta_variables where variable_id = (?)
    """
    variable_type, dimension_values = con.execute(q, parameters=[variable_id]).fetchone()  # type: ignore

    dimensions = _parse_dimension_values(json.loads(dimension_values))

    return VariableMetadataResponse(
        nonRedistributable=bool(nonRedistributable),
        display=json.loads(displayJson),
        source=source,
        type=variable_type,
        dimensions=dimensions,
        **variable,
    )
