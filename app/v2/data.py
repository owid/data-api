import io
from typing import Literal, Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import structlog
from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from pyarrow.feather import write_feather

from app import utils
from app.core.config import settings

from .db import engine

log = structlog.get_logger()


router = APIRouter()


FORMATS = Literal["csv", "feather", "json"]


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

    # fetch variable first to see whether data is in data_values or in catalog
    sql = """
    SELECT
        id, shortName, catalogPath
    FROM variables WHERE id = %(variable_id)s
    """
    vf = pd.read_sql(sql, engine, params={"variable_id": variable_id})
    if vf.empty:
        raise HTTPException(
            status_code=404, detail=f"variableId `{variable_id}` not found"
        )
    catalog_path = vf.catalogPath.values[0]
    variable_short_name = vf.shortName.values[0]

    # TODO: create Reader class and two subclasses for reading from catalog and from data values
    if catalog_path:
        results = _fetch_variable_from_catalog(catalog_path, variable_short_name)
    else:
        results = _fetch_variable_from_data_values(variable_id)

    variableData = results[["year", "entityId", "value"]].rename(
        columns={"year": "years", "entityId": "entities", "value": "values"}
    )
    variableData["values"] = pd.to_numeric(variableData["values"])
    return variableData.to_dict(orient="list")


def _add_entity_name_and_code(df: pd.DataFrame) -> pd.DataFrame:
    # add entity names and codes, this could be available in the catalog directly to speed it up
    # or we could keep all entity codes in memory
    sql = """
    SELECT
        id as entityId,
        name as entityName,
        code as entityCode
    FROM entities
    WHERE id IN %(entity_ids)s
    """
    entities = pd.read_sql(sql, engine, params={"entity_ids": list(set(df.entityId))})

    df = df.merge(entities, on="entityId")

    # move entity columns to the beginning
    df = df[
        ["entityId", "entityName", "entityCode"]
        + [c for c in df.columns if c not in ["entityId", "entityName", "entityCode"]]
    ]
    return df


def _fetch_variable_from_catalog(catalog_path, variable_short_name) -> pd.DataFrame:
    parquet_path = (settings.OWID_CATALOG_DIR / catalog_path).with_suffix(".parquet")

    # materializing in pandas might be unnecessary, we could send byte response directly
    df = pq.read_table(
        parquet_path,
        columns=["entity_id", "year", variable_short_name],
        filters=[(variable_short_name, "!=", np.nan)],
    ).to_pandas()

    df = df.rename(
        columns={
            "entity_id": "entityId",
            "year": "year",
            variable_short_name: "value",
        }
    )

    return _add_entity_name_and_code(df)


def _fetch_variable_from_data_values(variable_id: int) -> pd.DataFrame:
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
    return pd.read_sql(sql, engine, params={"variable_id": variable_id})


def _df_to_format(df: pd.DataFrame, format: FORMATS):
    if format == "csv":
        str_stream = io.StringIO()
        df.to_csv(str_stream, index=False)
        return StreamingResponse(iter([str_stream.getvalue()]), media_type="text/csv")
    elif format == "feather":
        bytes_io = io.BytesIO()
        write_feather(df, bytes_io)
        return utils.bytes_to_response(bytes_io)
    elif format == "json":
        # NOTE: we could also do this directly from pyarrow, but it is slower than to pandas
        # for some reason
        # return con.execute(sql, parameters=parameters).fetch_arrow_table().to_pydict()

        # TODO: converting to lists and then ormjson is slow, we could instead
        # convert to numpy arrays on which ormjson is super fast
        return df.to_dict(orient="list")
    else:
        raise HTTPException(status_code=400, detail=f"unknown type {type}")


def _fetch_dataset_from_data_values(
    variable_ids: list[int], id_to_name: dict[int, str], limit: int = 1000000000
) -> pd.DataFrame:
    sql = """
    SELECT
        variableId,
        value,
        year,
        entities.id AS entityId,
        entities.name AS entityName,
        entities.code AS entityCode
    FROM data_values
    LEFT JOIN entities ON data_values.entityId = entities.id
    WHERE data_values.variableId in %(variable_ids)s
    LIMIT %(limit)s
    """
    df = pd.read_sql(sql, engine, params={"variable_ids": variable_ids, "limit": limit})
    df = df.pivot(
        index=["year", "entityId", "entityName", "entityCode"],
        columns="variableId",
        values="value",
    ).reset_index()

    # downcast types
    # NOTE: this would be easier if we had type for every variable in DB
    for col in df.columns:
        df[col] = pd.to_numeric(df[col])

    # use variable names for columns
    df.columns = df.columns.map(id_to_name)

    return df


def _fetch_dataset_from_catalog(
    catalog_path: str,
    variable_short_names: list[str],
    short_name_to_name: dict[int, str],
    limit: int = 1000000000,
) -> pd.DataFrame:
    parquet_path = (settings.OWID_CATALOG_DIR / catalog_path).with_suffix(".parquet")

    # materializing in pandas might be unnecessary, we could send byte response directly
    df = pq.read_table(
        parquet_path,
        # TODO: there should be dimensions from dataset metadata
        columns=["entity_id", "year"] + variable_short_names,
        # TODO: we could ignore rows with all missing values
        # filters=[(variable_short_name, "!=", np.nan)],
    ).to_pandas()

    # TODO: could be more efficient
    df = df.head(limit)

    df = df.rename(
        columns={
            "entity_id": "entityId",
            "year": "year",
        }
    )

    return df.rename(columns=short_name_to_name)


@router.get(
    "/datasetById/data/{dataset_id}.{format}",
)
def data_for_dataset_id(
    response: Response,
    dataset_id: int,
    format: FORMATS = "csv",
    columns: Optional[list[str]] = Query(default=None),
    limit: int = 1000000000,
    # if_none_match: Optional[str] = Header(default=None),
):
    """We don't want API to load dataset from parquet into memory, process it and return back to user.
    Instead, we would like to redirect them to the parquet file in our S3 catalog."""

    # get all variables from dataset
    sql = """
    SELECT
        id, shortName, name, catalogPath
    FROM variables WHERE datasetId = %(dataset_id)s
    """
    variables_df = pd.read_sql(sql, engine, params={"dataset_id": dataset_id})
    if variables_df.empty:
        raise HTTPException(
            status_code=404, detail=f"datasetId `{dataset_id}` not found"
        )

    # this should use short names, but we don't have them for variables from MySQL
    if columns:
        variables_df = variables_df[variables_df.name.isin(columns)]
        if variables_df.empty:
            raise HTTPException(
                status_code=404, detail=f"columns `{columns}` not found"
            )

    if variables_df.catalogPath.isnull().all():
        df = _fetch_dataset_from_data_values(
            list(variables_df.id), variables_df.set_index("id").name, limit=limit
        )
        return _df_to_format(df, format)

    elif variables_df.catalogPath.notnull().all():
        if len(set(variables_df.catalogPath)) > 1:
            # NOTE: we could fetch table by table and then merge them, but that could be
            # slow and produce large datasets
            raise NotImplementedError(
                "Datasets with multiple tables are not yet supported"
            )

        df = _fetch_dataset_from_catalog(
            variables_df.catalogPath.iloc[0],
            list(variables_df.shortName),
            variables_df.set_index("shortName").name,
            limit=limit,
        )
        return _df_to_format(df, format)

    else:
        raise AssertionError("dataset has mixed data sources")
