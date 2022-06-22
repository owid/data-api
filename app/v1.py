import functools
import json
import threading
from typing import cast

import duckdb
import pandas as pd
import structlog
from fastapi import FastAPI, HTTPException

from app.core.config import settings
from app.schemas.v1 import (
    Dimension,
    DimensionProperties,
    Dimensions,
    VariableDataResponse,
    VariableMetadataResponse,
    VariableSource,
)

log = structlog.get_logger()


# NOTE: duckdb also supports python relations, would it be helpful?
# https://github.com/duckdb/duckdb/blob/master/examples/python/duckdb-python.py


v1 = FastAPI()


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
def data_for_variable(variable_id: int, limit: int = 1000000000):
    """Fetch data for a single variable."""

    con = get_readonly_connection(threading.get_ident())

    # get meta about variable
    q = f"""
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


def omit_nullable_values(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def _get_dimensions():
    # get variable types from duckdb (all metadata would be eventually retrieved in duckdb)
    con = get_readonly_connection(threading.get_ident())
    q = """
    select
        variable_type,
        entities_values,
        years_values
    from meta_variables where variable_id
    """
    variable_type, entities_values, years_values = con.execute(q).fetchone()  # type: ignore

    years_values = json.loads(years_values)
    entities_values = json.loads(entities_values)

    entities_values = zip(
        entities_values["entity_id"],
        entities_values["entity_name"],
        entities_values["entity_code"],
    )

    dimensions = Dimensions(
        years=Dimension(
            type="int", values=[DimensionProperties(id=y) for y in years_values]
        ),
        entities=Dimension(
            type="int",
            values=[
                DimensionProperties(id=e[0], name=e[1], code=e[2])
                for e in entities_values
            ],
        ),
    )

    return dimensions, variable_type


# QUESTION: how about `/variable/{variable_id}/metadata` naming?
@v1.get(
    "/variableById/metadata/{variable_id}",
    response_model=VariableMetadataResponse,
    response_model_exclude_unset=True,
)
def metadata_for_variable(variable_id: int):
    """Fetch metadata for a single variable from database."""
    q = """
    SELECT
        -- variables
        v.grapher_meta->>'$.id' as id,
        v.grapher_meta->>'$.name' as name,
        v.grapher_meta->>'$.unit' as unit,
        v.grapher_meta->>'$.description' as description,
        v.grapher_meta->>'$.createdAt' as createdAt,
        v.grapher_meta->>'$.updatedAt' as updatedAt,
        v.grapher_meta->>'$.code' as code,
        v.grapher_meta->>'$.coverage' as coverage,
        v.grapher_meta->>'$.timespan' as timespan,
        v.grapher_meta->>'$.datasetId' as datasetId,
        v.grapher_meta->>'$.sourceId' as sourceId,
        v.grapher_meta->>'$.shortUnit' as shortUnit,
        v.grapher_meta->>'$.display' as display,
        v.grapher_meta->>'$.columnOrder' as columnOrder,
        v.grapher_meta->>'$.originalMetadata' as originalMetadata,
        v.grapher_meta->>'$.grapherConfig' as grapherConfig,
        -- dataset
        d.grapher_meta->>'$.name' as datasetName,
        d.grapher_meta->>'$.nonRedistributable' as nonRedistributable,
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
    -- JOIN sources ON v.sourceId = sources.id
    WHERE v.variable_id = (?)
    """
    con = get_readonly_connection(threading.get_ident())

    # TODO: this is a hacky and slow way to do it, use ORM or proper dataclass instead
    df = cast(pd.DataFrame, con.execute(q, parameters=[variable_id]).fetch_df())
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

    # omit `id`, this should be explicit from the query rather than calling `variables.*`
    variable.pop("id")

    # NOTE: getting these is a bit of a pain, we have a lot of duplicate information
    # in our DB
    dimensions, variable_type = _get_dimensions()

    return VariableMetadataResponse(
        nonRedistributable=bool(nonRedistributable),
        display=json.loads(displayJson),
        source=source,
        type=variable_type,
        dimensions=dimensions,
        **variable,
    )
