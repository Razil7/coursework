from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI

from common.broker.base import RetryPolicy
from common.broker.rabbitmq import RabbitMQAdapter
from common.config import load_settings
from common.observability import configure_logging, get_logger
from common.outbox import InMemoryOutbox, OutboxPublisher
from gateway.app import create_app

log = get_logger("gateway.main")


def build() -> FastAPI:
    s = load_settings()
    broker = RabbitMQAdapter(
        s.amqp_url, RetryPolicy(max_attempts=s.max_attempts, base_delay=s.retry_base_delay)
    )
    outbox = InMemoryOutbox()
    publisher = OutboxPublisher(outbox, broker)
    query_url = f"http://query-service:{s.query_port}"

    async def get_status(job_id: str) -> dict[str, Any] | None:
        async with httpx.AsyncClient(timeout=2.0) as client:
            try:
                resp = await client.get(f"{query_url}/jobs/{job_id}")
            except httpx.HTTPError:
                return None
        return resp.json() if resp.status_code == 200 else None

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await broker.start()
        await publisher.start()
        log.info("ready", service="gateway")
        yield
        await publisher.stop()
        await broker.stop()

    return create_app(settings=s, outbox=outbox, get_status=get_status, lifespan=lifespan)


def main() -> None:
    configure_logging()
    s = load_settings()
    uvicorn.run(build(), host="0.0.0.0", port=s.gateway_port, log_level=s.log_level.lower())


if __name__ == "__main__":
    main()
