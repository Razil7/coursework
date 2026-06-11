from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from common.broker.base import RetryPolicy
from common.broker.rabbitmq import RabbitMQAdapter
from common.config import load_settings
from common.observability import configure_logging, get_logger
from query_service.app import create_app
from query_service.projection import setup as setup_projection

log = get_logger("query.main")


def build() -> FastAPI:
    s = load_settings()
    broker = RabbitMQAdapter(
        s.amqp_url, RetryPolicy(max_attempts=s.max_attempts, base_delay=s.retry_base_delay)
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.rm = await setup_projection(broker)
        await broker.start()
        log.info("ready", service="query-service")
        yield
        await broker.stop()

    return create_app(lifespan=lifespan)


def main() -> None:
    configure_logging()
    s = load_settings()
    uvicorn.run(build(), host="0.0.0.0", port=s.query_port, log_level=s.log_level.lower())


if __name__ == "__main__":
    main()
