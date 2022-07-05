from typing import Any

import orjson
from fastapi.responses import JSONResponse


class ORJSONResponse(JSONResponse):
    """It serializes dataclass, datetime, numpy, and UUID instances natively."""

    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)
