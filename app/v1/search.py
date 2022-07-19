import json
import threading
from typing import Any, Dict, cast
from matplotlib.pyplot import title

import numpy as np
import pandas as pd
import structlog
from fastapi import APIRouter
from enum import Enum

from app import utils

from .schemas import (
    Dimension,
    DimensionProperties,
    VariableMetadataResponse,
    VariableSource,
)

log = structlog.get_logger()


router = APIRouter()


class SearchType(str, Enum):
    table = "meta_tables"
    variable = "meta_variables"
    dataset = "meta_datasets"


@router.get("/search")
def search(term: str, type: SearchType = SearchType.variable, limit: int = 10):
    con = utils.get_readonly_connection(threading.get_ident())

    # TODO: implement search on other tables too? not sure whether we'll need it yet
    if type != SearchType.variable:
        raise NotImplementedError(f"Invalid search type {type}, only searching variables is currently supported")

    # sample search
    q = f"""
    SELECT
        v.short_name as variable_name,
        v.title as variable_title,
        v.description as variable_description,
        t.table_name,
        t.path as table_path,
        d.title as dataset_title,
        fts_main_meta_variables.match_bm25(variable_path, ?) AS match
    FROM meta_variables as v
    JOIN meta_datasets as d ON d.short_name = v.dataset_short_name
    join meta_tables as t ON t.path = v.table_path
    where match is not null
    order by match desc
    limit (?)
    """
    matches = con.execute(q, parameters=[term, limit]).fetch_df()

    matches["metadata_url"] = "/v1/dataset/metadata/" + matches["table_path"]
    matches["data_url"] = "/v1/dataset/data/" + matches["table_path"]

    matches = matches.drop(columns=["table_path"])

    return matches.to_dict(orient="records")