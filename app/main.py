from __future__ import annotations

from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.api.routes.health import router as health_router
from app.api.routes.seeds import router as seeds_router
from app.api.routes.videos import router as videos_router
from app.api.routes.jobs import router as jobs_router
from app.api.routes.briefs import router as briefs_router
from app.api.routes.stats import router as stats_router

@asynccontextmanager
async def lifespan(_: FastAPI):
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="viral-factory", lifespan=lifespan)
    app.include_router(health_router)
    app.include_router(seeds_router)
    app.include_router(videos_router)
    app.include_router(jobs_router)
    app.include_router(briefs_router)
    app.include_router(stats_router)
    return app


app = create_app()
