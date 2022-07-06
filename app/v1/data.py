import threading
from typing import cast

import pandas as pd
import structlog
from fastapi import APIRouter, HTTPException

from app import utils
from crawler.utils import sanitize_table_path

from .schemas import VariableDataResponse

log = structlog.get_logger()


router = APIRouter()


# QUESTION: how about /variable/{variable_id}/data?
@router.get(
    "/variableById/data/{variable_id}",
    response_model=VariableDataResponse,
    response_model_exclude_unset=True,
)
def data_for_backported_variable(variable_id: int, limit: int = 1000000000):
    """Fetch data for a single variable."""

    con = utils.get_readonly_connection(threading.get_ident())

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


@router.get(
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

    con = utils.get_readonly_connection(threading.get_ident())

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
