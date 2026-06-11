from __future__ import annotations

import asyncio

import pytest

from common.broker.base import RetryPolicy
from common.broker.in_memory import InMemoryBroker
from common.messages import Envelope, MessageType

JOB = MessageType.JOB_SUBMITTED


def _env() -> Envelope:
    return Envelope(correlation_id="corr", type=JOB)


async def test_pubsub_fanout_between_groups(wait_for) -> None:
    broker = InMemoryBroker()
    got_a: list[str] = []
    got_b: list[str] = []

    async def ha(env: Envelope) -> None:
        got_a.append(env.message_id)

    async def hb(env: Envelope) -> None:
        got_b.append(env.message_id)

    await broker.subscribe(JOB, ha, group="a")
    await broker.subscribe(JOB, hb, group="b")
    await broker.start()

    env = _env()
    await broker.publish(env)
    assert await wait_for(lambda: bool(got_a) and bool(got_b))
    assert got_a == [env.message_id]
    assert got_b == [env.message_id]
    await broker.stop()


async def test_competing_consumers_within_group(wait_for) -> None:
    broker = InMemoryBroker()
    seen: list[str] = []

    async def handler(env: Envelope) -> None:
        seen.append(env.message_id)

    await broker.subscribe(JOB, handler, group="g")
    await broker.subscribe(JOB, handler, group="g")
    await broker.start()

    ids = []
    for _ in range(4):
        env = _env()
        ids.append(env.message_id)
        await broker.publish(env)

    assert await wait_for(lambda: len(seen) == 4)
    assert sorted(seen) == sorted(ids)
    await broker.stop()


async def test_retry_then_success(wait_for) -> None:
    broker = InMemoryBroker(RetryPolicy(max_attempts=3, base_delay=0.01))
    attempts: list[int] = []

    async def handler(env: Envelope) -> None:
        attempts.append(env.attempt)
        if env.attempt == 1:
            raise RuntimeError("временный сбой")

    await broker.subscribe(JOB, handler, group="g")
    await broker.start()
    await broker.publish(_env())

    assert await wait_for(lambda: len(attempts) >= 2)
    assert attempts == [1, 2]
    assert broker.dead_letters == []
    await broker.stop()


async def test_dead_letter_after_max_attempts(wait_for) -> None:
    broker = InMemoryBroker(RetryPolicy(max_attempts=2, base_delay=0.01))

    async def handler(env: Envelope) -> None:
        raise RuntimeError("постоянный сбой")

    await broker.subscribe(JOB, handler, group="g")
    await broker.start()
    await broker.publish(_env())

    assert await wait_for(lambda: len(broker.dead_letters) == 1)
    assert broker.dead_letters[0].attempt == 2
    await asyncio.sleep(0.05)
    assert len(broker.dead_letters) == 1
    await broker.stop()


async def test_publish_without_subscribers_is_safe() -> None:
    broker = InMemoryBroker()
    await broker.start()
    await broker.publish(_env())
    await broker.stop()


async def test_group_copies_are_isolated(wait_for) -> None:
    broker = InMemoryBroker()
    seen_b: list[int] = []

    async def mutator(env: Envelope) -> None:
        env.payload["n"] = 999

    async def observer(env: Envelope) -> None:
        seen_b.append(env.payload.get("n", -1))

    await broker.subscribe(JOB, mutator, group="a")
    await broker.subscribe(JOB, observer, group="b")
    await broker.start()
    await broker.publish(Envelope(correlation_id="corr", type=JOB, payload={"n": 0}))

    assert await wait_for(lambda: len(seen_b) == 1)
    assert seen_b == [0]
    await broker.stop()


async def test_fifo_order_for_single_consumer(wait_for) -> None:
    broker = InMemoryBroker()
    order: list[int] = []

    async def handler(env: Envelope) -> None:
        order.append(env.payload["i"])

    await broker.subscribe(JOB, handler, group="g")
    await broker.start()
    for i in range(5):
        await broker.publish(Envelope(correlation_id="corr", type=JOB, payload={"i": i}))

    assert await wait_for(lambda: len(order) == 5)
    assert order == [0, 1, 2, 3, 4]
    await broker.stop()


def test_retry_policy_exponential_backoff_with_cap() -> None:
    policy = RetryPolicy(max_attempts=5, base_delay=0.1, factor=2.0, max_delay=5.0)
    assert policy.delay_for(1) == pytest.approx(0.1)
    assert policy.delay_for(2) == pytest.approx(0.2)
    assert policy.delay_for(3) == pytest.approx(0.4)
    assert policy.delay_for(10) == pytest.approx(5.0)


async def test_stop_leaves_no_running_consumer_tasks() -> None:
    broker = InMemoryBroker()

    async def handler(env: Envelope) -> None: ...

    await broker.subscribe(JOB, handler, group="g")
    await broker.start()
    await broker.stop()
    tasks = [
        t
        for by_group in broker._groups.values()
        for grp in by_group.values()
        for t in grp.tasks
    ]
    assert tasks and all(t.done() for t in tasks)
