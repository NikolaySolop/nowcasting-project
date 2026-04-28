from fastapi import FastAPI

from ingestion.api.routes import health, jobs, sources
from ingestion.core.config import settings
from ingestion.core.logging import configure_logging


def create_app() -> FastAPI:
    configure_logging(settings.log_level)
    app = FastAPI(title=settings.app_name)
    app.include_router(health.router, prefix="/health", tags=["health"])
    app.include_router(sources.router, prefix="/sources", tags=["sources"])
    app.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
    return app


app = create_app()
