"""FastAPI application entrypoint with lifespan-managed database pool and migrations."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.config import get_settings
from db.postgres import apply_initial_migration, close_pool, get_pool
from routers import charts, datasets, health, sources
from seeds.sources import seed_sources

logger = structlog.get_logger(__name__)


def configure_logging() -> None:
    """Configure structlog for JSON-friendly console output."""
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(20),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create DB pool, apply migrations, seed sources, and release resources on shutdown."""
    configure_logging()
    app.state.started_at = time.time()
    settings = get_settings()
    pool = await get_pool(settings.database_url)
    await apply_initial_migration(pool)
    await seed_sources()
    logger.info("application_ready")
    yield
    await close_pool()
    logger.info("application_shutdown")


def create_app() -> FastAPI:
    """Build the FastAPI application with routers and CORS."""
    app = FastAPI(
        title="Living Data Observatory API",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(health.router)
    app.include_router(datasets.router)
    app.include_router(sources.router)
    app.include_router(charts.router)
    return app


app = create_app()


@app.get("/")
async def root() -> dict[str, str]:
    """Lightweight root endpoint for uptime probes."""
    return {"service": "living-data-observatory", "docs": "/docs"}


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
