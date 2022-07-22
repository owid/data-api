from fastapi import FastAPI

from app import utils

from .data import router as data_router
from .lists import router as lists_router
from .metadata import router as metadata_router
from .search import router as search_router

v1 = FastAPI(default_response_class=utils.ORJSONResponse)

v1.include_router(metadata_router)
v1.include_router(data_router)
v1.include_router(search_router)
v1.include_router(lists_router)
