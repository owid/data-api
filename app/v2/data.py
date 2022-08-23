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
    "/variableById/data/{variable_id}",
)
def data_for_variable_id(
    response: Response,
    variable_id: int,
    # if_none_match: Optional[str] = Header(default=None),
):
    """Fetch data for a single variable from database.
    This function is similar to Variables.getVariableData in owid-grapher repository
    """

    # TODO: cache control with `set_cache_control`

    sql = """
    SELECT
        value,
        year,
        entities.id AS entityId,
        entities.name AS entityName,
        entities.code AS entityCode
    FROM data_values
    LEFT JOIN entities ON data_values.entityId = entities.id
    WHERE data_values.variableId = %(variable_id)s
    ORDER BY
        year ASC
    """
    results = pd.read_sql(sql, engine, params={"variable_id": variable_id})

    variableData = results[["year", "entityId", "value"]].rename(
        columns={"year": "years", "entityId": "entities", "value": "values"}
    )

    variableData["values"] = pd.to_numeric(variableData["values"])

    return variableData.to_dict(orient="list")
