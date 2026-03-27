"""FastAPI application entrypoint with lifespan-managed database pool and migrations."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from core.config import get_settings
from db.postgres import close_pool, ensure_schema_and_seeds, get_pool
from routers import charts, datasets, health, sources

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
    await ensure_schema_and_seeds(settings.database_url)
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

    @app.exception_handler(StarletteHTTPException)
    async def _http_exception_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        """Add a hint when the path is unknown (default Starlette 404)."""
        if exc.status_code == 404 and exc.detail == "Not Found":
            return JSONResponse(
                status_code=404,
                content={
                    "detail": "Not Found",
                    "path": request.url.path,
                    "hint": (
                        "Routes are at the URL root, not under /api. "
                        "Try GET /health, GET /charts, GET /docs (OpenAPI), or GET /api for a route list."
                    ),
                },
            )
        return await http_exception_handler(request, exc)

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

    @app.get("/")
    async def root() -> dict[str, str]:
        """Lightweight root endpoint for uptime probes."""
        return {"service": "living-data-observatory", "docs": "/docs"}

    @app.get("/api")
    async def api_discovery() -> dict[str, Any]:
        """
        Common landing path — this API does not use an /api prefix on routes.

        Use the paths below (same origin, no /api prefix).
        """
        return {
            "service": "living-data-observatory",
            "note": "Endpoints are mounted at the root (e.g. /health), not /api/health.",
            "docs": "/docs",
            "openapi": "/openapi.json",
            "endpoints": {
                "health": "/health",
                "charts": "/charts",
                "sources": "/sources",
                "datasets": "/datasets",
            },
        }

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
