from fastapi import FastAPI

from app import utils

from .data import router as data_router
from .metadata import router as metadata_router

v2 = FastAPI(default_response_class=utils.ORJSONResponse)

v2.include_router(metadata_router)
v2.include_router(data_router)
