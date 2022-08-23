import datetime as dt
import json
import threading
import time
from typing import Any, Dict, Optional, cast

import numpy as np
import pandas as pd
import structlog
from fastapi import APIRouter, Header, HTTPException, Response
from pydantic import BaseModel

from app import utils
from app.utils import omit_nullable_values
from app.v1.schemas import (
    Dimension,
    DimensionProperties,
    VariableMetadataResponse,
    VariableSource,
)

from .data import _fetch_variable_from_catalog, _fetch_variable_from_data_values
from .db import engine

log = structlog.get_logger()


router = APIRouter()


# class VariableRow(BaseModel):
#     id: int
#     name: str
#     code: Optional[str]
#     unit: str
#     shortUnit: Optional[str]
#     description: Optional[str]
#     createdAt: dt.datetime
#     updatedAt: dt.datetime
#     datasetId: int
#     sourceId: int
#     # TODO: use better type for display
#     display: Any
#     coverage: Optional[str]
#     timespan: Optional[str]
#     columnOrder: Optional[int]


@router.get(
    "/variableById/metadata/{variable_id}",
    response_model=VariableMetadataResponse,
    response_model_exclude_unset=True,
)
def metadata_for_variable_id(
    response: Response,
    variable_id: int,
    # if_none_match: Optional[str] = Header(default=None),
):
    """Fetch metadata for a single variable from database.
    This function is similar to Variables.getVariableData in owid-grapher repository
    """

    # TODO: cache control with `set_cache_control`

    sql = """
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
    df = pd.read_sql(sql, engine, params={"variable_id": variable_id})
    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"variableId `{variable_id}` not found"
        )
    # row = VariableRow(**df.iloc[0].to_dict())
    row = df.iloc[0].to_dict()

    variable = row
    sourceId = row.pop("sourceId")
    sourceName = row.pop("sourceName")
    sourceDescription = row.pop("sourceDescription")
    nonRedistributable = row.pop("nonRedistributable")
    displayJson = row.pop("display")

    display = json.loads(displayJson)
    partialSource = json.loads(sourceDescription)
    variableMetadata = dict(
        **omit_nullable_values(variable),
        type="mixed",  # precise type will be updated further down
        nonRedistributable=bool(nonRedistributable),
        display=display,
        source=dict(
            id=sourceId,
            name=sourceName,
            dataPublishedBy=partialSource.get("dataPublishedBy") or "",
            dataPublisherSource=partialSource.get("dataPublisherSource") or "",
            link=partialSource.get("link") or "",
            retrievedDate=partialSource.get("retrievedDate") or "",
            additionalInfo=partialSource.get("additionalInfo") or "",
        ),
    )

    if variable["catalogPath"]:
        results = _fetch_variable_from_catalog(row["catalogPath"], row["shortName"])
    else:
        # TODO: this is inefficient as we need to get all data_values just to get
        # variable type (and dimensions, but these can be at least fetched separately
        # with `distinct`). Ideally we'd have variable type already in `variables` table
        results = _fetch_variable_from_data_values(variable_id)

    entityArray = (
        results[["entityId", "entityName", "entityCode"]]
        .drop_duplicates(["entityId"])
        .rename(columns={"entityId": "id", "entityName": "name", "entityCode": "code"})
        .set_index("id", drop=False)
        .to_dict(orient="records")
    )

    yearArray = (
        results[["year"]]
        .drop_duplicates(["year"])
        .rename(columns={"year": "id"})
        .set_index("id", drop=False)
        .to_dict(orient="records")
    )

    variableData = results[["year", "entityId", "value"]].rename(
        columns={"year": "years", "entityId": "entities", "value": "values"}
    )

    # improve type detection
    variableData["values"] = pd.to_numeric(variableData["values"])

    inferred_type = pd.api.types.infer_dtype(variableData["values"])
    if inferred_type == "floating":
        variableMetadata["type"] = "float"
    else:
        raise NotImplementedError()

    # if encounteredFloatDataValues and encounteredStringDataValues:
    #     variableMetadata["type"] = "mixed"
    # elif encounteredFloatDataValues:
    #     variableMetadata["type"] = "float"
    # elif encounteredIntDataValues:
    #     variableMetadata["type"] = "int"
    # elif encounteredStringDataValues:
    #     variableMetadata["type"] = "string"

    # remove fields to be compatible with v1 schema, but perhaps we should
    # include it?
    variableMetadata.pop("id")
    variableMetadata.pop("shortName", None)
    variableMetadata.pop("catalogPath", None)

    return VariableMetadataResponse(
        **variableMetadata,
        dimensions=dict(
            # TODO: type does not make much sense here, remove it here and in v1
            years={"values": yearArray, "type": "int"},
            entities={"values": entityArray, "type": "int"},
        ),
    )

    # return dict(
    #     **variableMetadata,
    #     dimensions=dict(
    #         years={"values": yearArray},
    #         entities={"values": entityArray},
    #     ),
    # )


@router.get("/datasetById/metadata/{dataset_id}")
def metadata_for_dataset_id(dataset_id: int):
    sql = """
    select
        datasets.*,
        sources.name AS sourceName,
        sources.description AS sourceDescription
    from datasets
    JOIN sources ON datasets.id = sources.datasetId
    where datasets.id = %(dataset_id)s
    """
    dataset_df = pd.read_sql(sql, engine, params={"dataset_id": dataset_id})
    if dataset_df.empty:
        raise HTTPException(
            status_code=404, detail=f"datasetId `{dataset_id}` not found"
        )
    assert dataset_df.shape[0] == 1, "Expected exactly one source per dataset"

    ds = dataset_df.iloc[0].to_dict()
    ds['sourceDescription'] = json.loads(ds['sourceDescription'])

    # add all dataset variables
    sql = """
    select
        id,
        shortName,
        name
    from variables where datasetId = %(dataset_id)s
    """
    variables_df = pd.read_sql(sql, engine, params={"dataset_id": dataset_id})

    ds["variables"] = variables_df.to_dict(orient="records")

    return ds
