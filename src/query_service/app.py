from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

from fastapi import FastAPI, HTTPException, Request

Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


def create_app(lifespan: Lifespan | None = None) -> FastAPI:
    app = FastAPI(title="query-service", lifespan=lifespan)

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str, request: Request) -> dict[str, Any]:
        job = request.app.state.rm.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job

    @app.get("/jobs")
    async def list_jobs(request: Request) -> list[dict[str, Any]]:
        return request.app.state.rm.all()

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
