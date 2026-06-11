from __future__ import annotations

import asyncio

from common.broker.in_memory import InMemoryBroker
from common.messages import Envelope, MessageType
from common.outbox import InMemoryOutbox, OutboxPublisher

JOB = MessageType.JOB_SUBMITTED


def _env(**payload: object) -> Envelope:
    return Envelope(correlation_id="corr", type=JOB, payload=payload)


async def test_outbox_publishes_each_message_once(wait_for) -> None:
    broker = InMemoryBroker()
    received: list[str] = []

    async def handler(env: Envelope) -> None:
        received.append(env.message_id)

    await broker.subscribe(JOB, handler, group="g")
    await broker.start()

    outbox = InMemoryOutbox()
    publisher = OutboxPublisher(outbox, broker, interval=0.01)
    await publisher.start()

    env = Envelope(correlation_id="corr", type=JOB)
    await outbox.add(env)

    assert await wait_for(lambda: received == [env.message_id])
    await asyncio.sleep(0.05)
    assert received == [env.message_id]
    assert await outbox.fetch_unsent() == []

    await publisher.stop()
    await broker.stop()


async def test_outbox_publishes_multiple_in_order(wait_for) -> None:
    broker = InMemoryBroker()
    received: list[str] = []

    async def handler(env: Envelope) -> None:
        received.append(env.payload["i"])

    await broker.subscribe(JOB, handler, group="g")
    await broker.start()

    outbox = InMemoryOutbox()
    publisher = OutboxPublisher(outbox, broker, interval=0.01)
    await publisher.start()

    for i in ("a", "b", "c"):
        await outbox.add(_env(i=i))

    assert await wait_for(lambda: received == ["a", "b", "c"])
    await asyncio.sleep(0.05)
    assert received == ["a", "b", "c"]
    assert await outbox.fetch_unsent() == []

    await publisher.stop()
    await broker.stop()


async def test_fetch_unsent_respects_limit_and_marking() -> None:
    outbox = InMemoryOutbox()
    for i in range(5):
        await outbox.add(_env(i=i))

    first = await outbox.fetch_unsent(limit=2)
    assert [env.payload["i"] for _, env in first] == [0, 1]
    for row_id, _ in first:
        await outbox.mark_sent(row_id)
    rest = await outbox.fetch_unsent()
    assert [env.payload["i"] for _, env in rest] == [2, 3, 4]
