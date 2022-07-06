import functools
import json
import threading
from typing import Any, Dict, cast

import duckdb
import numpy as np
import pandas as pd
import structlog
from fastapi import FastAPI

from app import utils

from .metadata import router as metadata_router
from .data import router as data_router

log = structlog.get_logger()

v1 = FastAPI(default_response_class=utils.ORJSONResponse)

v1.include_router(metadata_router)
v1.include_router(data_router)
