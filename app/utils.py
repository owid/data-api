import functools
from typing import Any, Optional

import duckdb
import orjson
import pandas as pd
import structlog
from fastapi import HTTPException, Response
from fastapi.responses import JSONResponse

from app.core.config import settings

log = structlog.get_logger()


class ORJSONResponse(JSONResponse):
    """It serializes dataclass, datetime, numpy, and UUID instances natively."""

    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)


@functools.cache
def get_readonly_connection(thread_id: int) -> duckdb.DuckDBPyConnection:
    # duckdb connection is not threadsafe, we have to create one connection per thread
    log.info("duckdb.new_connection", thread_id=thread_id)
    con = duckdb.connect(
        database=settings.DUCKDB_PATH.as_posix(),
        read_only=True,
        config={"memory_limit": settings.DUCKDB_MEMORY_LIMIT},
    )
    return con


def omit_nullable_values(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None and not pd.isna(v)}


def set_cache_control(
    response: Response, if_none_match: Optional[str], checksum: str
) -> Response:
    # if the client sent a IF-NONE-MATCH header, check if it matches the checksum
    if if_none_match == checksum:
        raise HTTPException(status_code=304, detail="Checksum match")

    # Send the checksum as the etag header and set cache-control to cache with
    # max-age of 0 (which makes the client validate with the if-none-match header)
    response.headers["ETag"] = checksum
    response.headers[
        "Cache-Control"
    ] = "max-age=0"  # We could consider allowing a certain time window
    return response
