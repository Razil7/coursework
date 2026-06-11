from __future__ import annotations

import asyncio
import contextlib
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import uvicorn

import coordinator.saga as coordinator
import query_service.projection as query
import worker_finalize.worker as wf
import worker_process.worker as wp
import worker_validate.worker as wv
from common.broker.base import RetryPolicy
from common.broker.in_memory import InMemoryBroker
from common.config import load_settings
from common.idempotency import InMemoryIdempotencyStore
from common.observability import configure_logging, get_logger
from common.outbox import InMemoryOutbox, OutboxPublisher
from gateway.app import create_app

log = get_logger("runner")


async def build() -> tuple[Any, InMemoryBroker, OutboxPublisher, Any]:
    settings = load_settings()
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
    publisher = OutboxPublisher(outbox, broker)
    await publisher.start()

    async def get_status(job_id: str) -> dict[str, Any] | None:
        return rm.get(job_id)

    app = create_app(settings=settings, outbox=outbox, get_status=get_status)
    return settings, broker, publisher, app


async def main() -> None:
    configure_logging()
    settings, broker, publisher, app = await build()
    config = uvicorn.Config(
        app, host="127.0.0.1", port=settings.gateway_port, log_level="warning"
    )
    server = uvicorn.Server(config)
    log.info("stand_ready", url=f"http://127.0.0.1:{settings.gateway_port}")
    try:
        await server.serve()
    finally:
        await publisher.stop()
        await broker.stop()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
