from __future__ import annotations

import asyncio

import worker_validate.worker as svc
from common.broker.base import RetryPolicy
from common.broker.rabbitmq import RabbitMQAdapter
from common.config import load_settings
from common.idempotency import InMemoryIdempotencyStore
from common.observability import configure_logging, get_logger

log = get_logger("worker.validate.main")


async def main() -> None:
    configure_logging()
    s = load_settings()
    broker = RabbitMQAdapter(
        s.amqp_url, RetryPolicy(max_attempts=s.max_attempts, base_delay=s.retry_base_delay)
    )
    await svc.setup(broker, s, InMemoryIdempotencyStore())
    await broker.start()
    log.info("ready", service="worker-validate")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
