from __future__ import annotations

import asyncio
from typing import Any

import httpx

import coordinator.saga as coordinator
import query_service.projection as query
import worker_finalize.worker as wf
import worker_process.worker as wp
import worker_validate.worker as wv
from common.broker.base import RetryPolicy
from common.broker.in_memory import InMemoryBroker
from common.config import Settings
from common.idempotency import InMemoryIdempotencyStore
from common.outbox import InMemoryOutbox, OutboxPublisher
from gateway.app import create_app


async def _build_app(**overrides: Any) -> tuple[InMemoryBroker, OutboxPublisher, object]:
    base: dict[str, Any] = dict(
        validate_duration=0.0,
        process_duration=0.0,
        finalize_duration=0.0,
        max_attempts=3,
        retry_base_delay=0.01,
    )
    base.update(overrides)
    settings = Settings(**base)  # type: ignore[arg-type]
    broker = InMemoryBroker(
        RetryPolicy(max_attempts=settings.max_attempts, base_delay=settings.retry_base_delay)
    )
    idem = InMemoryIdempotencyStore()
    await coordinator.setup(broker)
    rm = await query.setup(broker)
    await wv.setup(broker, settings, idem)
    await wp.setup(broker, settings, idem)
    await wf.setup(broker, settings, idem)
    await broker.start()
    outbox = InMemoryOutbox()
    publisher = OutboxPublisher(outbox, broker, interval=0.01)
    await publisher.start()

    async def get_status(job_id: str) -> dict | None:
        return rm.get(job_id)

    app = create_app(settings=settings, outbox=outbox, get_status=get_status)
    return broker, publisher, app


async def _poll_status(client: httpx.AsyncClient, job_id: str, target: str) -> str | None:
    deadline = asyncio.get_running_loop().time() + 3.0
    while asyncio.get_running_loop().time() < deadline:
        r = await client.get(f"/jobs/{job_id}")
        if r.status_code == 200 and r.json()["status"] == target:
            return target
        await asyncio.sleep(0.02)
    return None


async def test_async_submit_reaches_completed() -> None:
    broker, publisher, app = await _build_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/jobs", json={"data": {"n": 1}})
        assert resp.status_code == 202
        job_id = resp.json()["job_id"]
        assert await _poll_status(client, job_id, "COMPLETED") == "COMPLETED"

    await publisher.stop()
    await broker.stop()


async def test_async_submit_reaches_failed_on_permanent_failure() -> None:
    broker, publisher, app = await _build_app(process_fail_rate=1.0, max_attempts=2)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/jobs", json={"data": {}})
        assert resp.status_code == 202
        job_id = resp.json()["job_id"]
        assert await _poll_status(client, job_id, "FAILED") == "FAILED"

    await publisher.stop()
    await broker.stop()


async def test_sync_baseline_returns_result() -> None:
    broker, publisher, app = await _build_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/jobs/sync", json={"data": {}})
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "COMPLETED"
        assert "elapsed_s" in body

    await publisher.stop()
    await broker.stop()


async def test_unknown_job_returns_404() -> None:
    broker, publisher, app = await _build_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/jobs/does-not-exist")
        assert r.status_code == 404

    await publisher.stop()
    await broker.stop()


async def test_healthz_ok() -> None:
    broker, publisher, app = await _build_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/healthz")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}

    await publisher.stop()
    await broker.stop()
