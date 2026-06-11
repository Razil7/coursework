from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest

import coordinator.saga as coordinator
import query_service.projection as query
import worker_finalize.worker as wf
import worker_process.worker as wp
import worker_validate.worker as wv
from common.broker.base import RetryPolicy
from common.broker.in_memory import InMemoryBroker
from common.config import Settings
from common.idempotency import InMemoryIdempotencyStore
from query_service.projection import ReadModel

MakeStand = Callable[[Settings], Awaitable[tuple[InMemoryBroker, ReadModel]]]


@pytest.fixture
def wait_for() -> Callable[..., Awaitable[bool]]:
    async def _wait(
        predicate: Callable[[], bool], timeout_s: float = 3.0, interval: float = 0.01
    ) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while loop.time() < deadline:
            if predicate():
                return True
            await asyncio.sleep(interval)
        return predicate()

    return _wait


@pytest.fixture
async def make_stand() -> AsyncGenerator[MakeStand, None]:
    brokers: list[InMemoryBroker] = []

    async def _make(settings: Settings) -> tuple[InMemoryBroker, ReadModel]:
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
        brokers.append(broker)
        return broker, rm

    yield _make

    for broker in brokers:
        await broker.stop()
