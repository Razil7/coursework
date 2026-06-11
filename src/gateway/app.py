from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from common.config import Settings
from common.messages import Envelope, MessageType, new_id
from common.observability import correlation, get_logger
from common.outbox import OutboxStore
from common.work import simulate_work

log = get_logger("gateway")

StatusGetter = Callable[[str], Awaitable[dict[str, Any] | None]]
Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


class SubmitRequest(BaseModel):
    data: dict[str, Any] = Field(default_factory=dict)


class SubmitResponse(BaseModel):
    job_id: str
    status: str = "accepted"


def create_app(
    *,
    settings: Settings,
    outbox: OutboxStore,
    get_status: StatusGetter,
    lifespan: Lifespan | None = None,
) -> FastAPI:
    app = FastAPI(title="gateway", lifespan=lifespan)

    @app.post("/jobs", status_code=202, response_model=SubmitResponse)
    async def submit(req: SubmitRequest) -> SubmitResponse:
        job_id = new_id()
        with correlation(job_id):
            env = Envelope(correlation_id=job_id, type=MessageType.JOB_SUBMITTED, payload=req.data)
            await outbox.add(env)
            log.info("job_accepted", job=job_id)
        return SubmitResponse(job_id=job_id)

    @app.post("/jobs/sync")
    async def submit_sync(req: SubmitRequest) -> dict[str, Any]:
        job_id = new_id()
        started = time.perf_counter()
        with correlation(job_id):
            v = await simulate_work(label="validate", duration_s=settings.validate_duration)
            p = await simulate_work(label="process", duration_s=settings.process_duration)
            f = await simulate_work(label="finalize", duration_s=settings.finalize_duration)
        elapsed = time.perf_counter() - started
        return {
            "job_id": job_id,
            "status": "COMPLETED",
            "result": {"validate": v, "process": p, "finalize": f},
            "elapsed_s": round(elapsed, 4),
        }

    @app.get("/jobs/{job_id}")
    async def status(job_id: str) -> dict[str, Any]:
        job = await get_status(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
