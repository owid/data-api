import io
import threading
from typing import Any, Literal, Optional, cast

import pandas as pd
import pyarrow as pa
import structlog
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pyarrow.feather import write_feather

from app import utils
from crawler.utils import sanitize_table_path

from .schemas import VariableDataResponse

log = structlog.get_logger()


DATA_TYPES = Literal["csv", "feather", "feather_direct", "json"]

router = APIRouter()


def _read_sql_bytes(con, sql: str, parameters) -> io.BytesIO:
    """Execute SQL and return BytesIO object with byte data."""
    sink = io.BytesIO()

    batch_iterator = con.execute(sql, parameters=parameters).fetch_record_batch(
        chunk_size=1000
    )
    with pa.ipc.new_file(sink, batch_iterator.schema) as writer:
        for rb in batch_iterator:
            writer.write_batch(rb)

    sink.seek(0)

    return sink


def _bytes_to_response(bytes_io: io.BytesIO) -> StreamingResponse:
    # NOTE: using raw `bytes_io` should be in theory faster than `iter([bytes_io.getvalue()])`, yet
    # it is much slower for unknown reasons
    # response = StreamingResponse(bytes_io, media_type="application/octet-stream")
    response = StreamingResponse(
        iter([bytes_io.getvalue()]), media_type="application/octet-stream"
    )
    response.headers["Content-Disposition"] = "attachment; filename=owid.feather"
    return response


def _sql_to_response(
    con, sql: str, type: DATA_TYPES, parameters: list[Any] = []
) -> Any:
    # read data in feather format and return it directly in response
    # NOTE: should be the fastest in theory, but is really slow for unknown reasons
    if type == "feather_direct":
        # WARNING: this does not support categorical variables and raises `pyarrow.lib.ArrowTypeError`
        # when you try to read it with pandas (see https://github.com/duckdb/duckdb/issues/4130)
        # we'd have to either convert categoricals into strings or change format while still in arrow format
        bytes_io = _read_sql_bytes(con, sql, parameters=parameters)
        return _bytes_to_response(bytes_io)

    # read data into dataframe and then convert to feather
    elif type == "feather":
        bytes_io = io.BytesIO()
        df = con.execute(sql, parameters=parameters).fetch_df()
        write_feather(df, bytes_io)
        return _bytes_to_response(bytes_io)

    # read data into dataframe and then convert to csv
    elif type == "csv":
        df = con.execute(sql, parameters=parameters).fetch_df()

        str_stream = io.StringIO()
        df.to_csv(str_stream, index=False)

        return StreamingResponse(iter([str_stream.getvalue()]), media_type="text/csv")

    # read data into dataframe and then convert to json
    elif type == "json":
        # NOTE: we could also do this directly from pyarrow, but it is slower than to pandas
        # for some reason
        # return con.execute(sql, parameters=parameters).fetch_arrow_table().to_pydict()

        df = con.execute(sql, parameters=parameters).fetch_df()

        # TODO: converting to lists and then ormjson is slow, we could instead
        # convert to numpy arrays on which ormjson is super fast
        return df.to_dict(orient="list")

    else:
        raise HTTPException(status_code=400, detail=f"unknown type {type}")


@router.post("/sql")
def sql_query(sql: str, type: DATA_TYPES = "csv"):
    """Run arbitrary query on top of our database."""
    con = utils.get_readonly_connection(threading.get_ident())
    return _sql_to_response(con, sql, type)


# QUESTION: how about /variable/{variable_id}/data?
@router.get(
    "/variableById/data/{variable_id}",
    response_model=VariableDataResponse,
    response_model_exclude_unset=True,
)
def data_for_backported_variable(variable_id: int, limit: Optional[int] = None):
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
        year as years,
        entity_name as entity_names,
        entity_id as entities,
        entity_code as entity_codes,
        {r["short_name"]} as values
    from {r["table_db_name"]}
    where {r["short_name"]} is not null
    """
    parameters = []
    if limit:
        q += "limit (?)"
        parameters.append(limit)
    df = cast(pd.DataFrame, con.execute(q, parameters=parameters).fetch_df())
    return df.to_dict(orient="list")


# NOTE: it might be more intuitive to have paths like this
#   /dataset/{channel}/{namespace}/{version}/{dataset}/{table}/data.{type}
#  and
#   /dataset/{channel}/{namespace}/{version}/{dataset}/{table}/metadata
#  especially for browsing catalog tree in `lists.py`
@router.get(
    "/dataset/data/{channel}/{namespace}/{version}/{dataset}/{table}.{type}",
)
def data_for_etl_table(
    channel: str,
    namespace: str,
    version: str,
    dataset: str,
    table: str,
    columns: str = "*",
    limit: int = 1000000000,
    type: DATA_TYPES = "csv",
):
    """Fetch data for a table."""

    con = utils.get_readonly_connection(threading.get_ident())
    table_db_name = sanitize_table_path(
        f"{channel}/{namespace}/{version}/{dataset}/{table}"
    )
    sql = f"""
    select
        {columns}
    from {table_db_name}
    limit (?)
    """

    con = utils.get_readonly_connection(threading.get_ident())
    return _sql_to_response(con, sql, type, [limit])


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
