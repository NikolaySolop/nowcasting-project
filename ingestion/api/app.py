from contextlib import asynccontextmanager

from fastapi import FastAPI

from ingestion.api.routes import health, jobs, sources
from ingestion.api.routes.deps import registry
from ingestion.core.config import settings
from ingestion.core.logging import configure_logging
from ingestion.scheduler.scheduler import IngestionScheduler
from storage.db.session import async_session_factory, close_db_connection


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.ingestion_scheduler = IngestionScheduler(
        registry=registry,
        settings=settings,
        session_factory=async_session_factory,
    )
    if settings.scheduler_enabled:
        app.state.ingestion_scheduler.start()
    try:
        yield
    finally:
        app.state.ingestion_scheduler.shutdown()
        await close_db_connection()


def create_app() -> FastAPI:
    configure_logging(settings.log_level)
    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.include_router(health.router, prefix="/health", tags=["health"])
    app.include_router(sources.router, prefix="/sources", tags=["sources"])
    app.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
    return app


app = create_app()
