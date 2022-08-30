import json
import threading
from typing import Any, Dict, Optional, cast

import numpy as np
import pandas as pd
import structlog
from fastapi import APIRouter, Header, HTTPException, Response

from app import utils

from .schemas import (
    Dimension,
    DimensionProperties,
    VariableMetadataResponse,
    VariableSource,
)

log = structlog.get_logger()


router = APIRouter()

# NOTE: duckdb also supports python relations, would it be helpful?
# https://github.com/duckdb/duckdb/blob/master/examples/python/duckdb-python.py


@router.get(
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

    con = utils.get_readonly_connection(threading.get_ident())

    vf = _metadata_etl_variables(con, table_path)
    tf = _metadata_etl_table(con, table_path)
    df = _metadata_etl_dataset(con, channel, namespace, version, dataset)

    if df.empty:
        raise HTTPException(status_code=404, detail=f"table `{table_path}` not found")

    return {
        "dataset": df.iloc[0].to_dict(),
        "table": tf.iloc[0].to_dict(),
        "variables": vf.to_dict(orient="records"),
    }


# QUESTION: how about `/variable/{variable_id}/metadata` naming?
@router.get(
    "/variableById/metadata/{variable_id}",
    response_model=VariableMetadataResponse,
    response_model_exclude_unset=True,
)
def metadata_for_backported_variable(
    response: Response,
    variable_id: int,
    if_none_match: Optional[str] = Header(default=None),
):
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
        d.checksum as checksum,
    FROM meta_variables as v
    JOIN meta_datasets as d ON d.short_name = v.dataset_short_name
    join meta_tables t on v.table_path = t.path
    WHERE v.variable_id = (?)
    """
    con = utils.get_readonly_connection(threading.get_ident())

    # TODO: this is a hacky and slow way to do it, use ORM or proper dataclass instead
    df = cast(pd.DataFrame, con.execute(q, parameters=[variable_id]).fetch_df())

    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"variableId `{variable_id}` not found"
        )

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

    # this is the dataset level checksum which is the best we have
    # at the moment
    checksum = row.pop("checksum")

    response = utils.set_cache_control(response, if_none_match, checksum)

    variable = utils.omit_nullable_values(row)

    # get variable types from duckdb (all metadata would be eventually retrieved in duckdb)
    # NOTE: getting these is a bit of a pain, we have a lot of duplicate information
    # in our DB
    q = """
    select
        v.variable_type,
        t.dimension_values
    from meta_variables as v
    join meta_tables as t on t.path = v.table_path
    where variable_id = (?)
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


def _parse_dimension_values(dimension_values: Any) -> Dict[str, Dimension]:
    dimensions = {}

    # NOTE: we have inconsistency with plurals - even though the dimension name is
    # singular, we use plural in the API (but not for custom dimensions)
    if "year" in dimension_values:
        dimensions["years"] = Dimension(
            type="int",
            values=[DimensionProperties(id=y) for y in dimension_values.pop("year")],
        )

    if "entity_zip" in dimension_values:
        dimensions["entities"] = Dimension(
            type="int",
            values=[
                DimensionProperties(id=int(e[0]), name=e[1], code=e[2])
                for e in map(lambda x: x.split("|"), dimension_values.pop("entity_zip"))
            ],
        )

    assert not dimension_values, (
        "This currently works only for backported datasets with dimensions "
        '{"year", "entity_id", "entity_name", "entity_code"}'
    )

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
        -- conversion factor from display is needed for CO2 datasets, but honestly it would be
        -- better to hide it or do the calculation implicitly
        v.display,
        -- v.grapher_meta,
        -- v.variable_id,
        v.short_name,
        v.table_path,
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
    for col in ("licenses", "sources", "display"):
        vf[col] = vf[col].apply(json.loads)
    return vf


def _metadata_etl_table(con, table_path):
    q = """
    SELECT
        table_name,
        dataset_name,
        version,
        namespace,
        channel,
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
        checksum,
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
