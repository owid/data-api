import functools
from typing import Any

import duckdb
import orjson
import pandas as pd
import structlog
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
