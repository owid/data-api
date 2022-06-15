import datetime as dt
import functools
import json
import threading
from typing import Any, Dict, List, Optional

import pandas as pd
import duckdb
import structlog
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.core.config import settings
from app.database import engine
from app.schemas.v1 import (
    VariableDataResponse,
    VariableMetadataResponse,
    Dimension,
    Dimensions,
    DimensionProperties,
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
@v1.get("/variableById/data/{variable_id}", response_model=VariableDataResponse)
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
    df = con.execute(q, parameters=[variable_id]).fetch_df()
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
    df = con.execute(q, parameters=[limit]).fetch_df()
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
    variable_type, entities_values, years_values = con.execute(q).fetchone()

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
    # TODO: data is fetched from grapher DB, but it will be eventally fetched from catalog
    q = """
    SELECT
        variables.*,
        datasets.name AS datasetName,
        datasets.nonRedistributable AS nonRedistributable,
        sources.name AS sourceName,
        sources.description AS sourceDescription
    FROM variables
    JOIN datasets ON variables.datasetId = datasets.id
    JOIN sources ON variables.sourceId = sources.id
    WHERE variables.id = %(variable_id)s
    """
    df = pd.read_sql(q, engine, params={"variable_id": variable_id})
    row = df.iloc[0].to_dict()

    sourceId = row.pop("sourceId")
    sourceName = row.pop("sourceName")
    sourceDescription = row.pop("sourceDescription")
    nonRedistributable = row.pop("nonRedistributable")
    displayJson = row.pop("display")
    partialSource = json.loads(sourceDescription)
    variable = omit_nullable_values(row)

    # NOTE: getting these is a bit of a pain, we have a lot of duplicate information
    # in our DB
    dimensions, variable_type = _get_dimensions()

    return VariableMetadataResponse(
        nonRedistributable=bool(nonRedistributable),
        display=json.loads(displayJson),
        source=VariableSource(
            id=sourceId,
            name=sourceName,
            dataPublishedBy=partialSource["dataPublishedBy"] or "",
            dataPublisherSource=partialSource["dataPublisherSource"] or "",
            link=partialSource["link"] or "",
            retrievedDate=partialSource["retrievedDate"] or "",
            additionalInfo=partialSource["additionalInfo"] or "",
        ),
        type=variable_type,
        dimensions=dimensions,
        **variable,
    )
