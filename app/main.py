import threading

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.v1 import v1

log = structlog.get_logger()


def get_application():
    _app = FastAPI(title=settings.PROJECT_NAME)

    _app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return _app


app = get_application()

# mount subapplications as versions
app.mount("/v1", v1)


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "thread_id": str(threading.get_ident()),
    }
