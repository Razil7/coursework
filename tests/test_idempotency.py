from __future__ import annotations

import pytest

from common.idempotency import InMemoryIdempotencyStore, idempotent
from common.messages import Envelope, MessageType


def _env() -> Envelope:
    return Envelope(correlation_id="corr", type=MessageType.JOB_SUBMITTED)


async def test_idempotent_skips_duplicates() -> None:
    store = InMemoryIdempotencyStore()
    calls: list[str] = []

    async def handler(env: Envelope) -> None:
        calls.append(env.message_id)

    wrapped = idempotent(store, handler)
    env = _env()

    await wrapped(env)
    await wrapped(env)

    assert calls == [env.message_id]


async def test_idempotent_processes_distinct_messages() -> None:
    store = InMemoryIdempotencyStore()
    calls: list[str] = []

    async def handler(env: Envelope) -> None:
        calls.append(env.message_id)

    wrapped = idempotent(store, handler)
    e1 = _env()
    e2 = _env()

    await wrapped(e1)
    await wrapped(e2)

    assert calls == [e1.message_id, e2.message_id]


async def test_idempotent_does_not_mark_on_failure() -> None:
    store = InMemoryIdempotencyStore()
    calls: list[int] = []

    async def handler(env: Envelope) -> None:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("сбой первой попытки")

    wrapped = idempotent(store, handler)
    env = _env()

    with pytest.raises(RuntimeError):
        await wrapped(env)
    await wrapped(env)
    await wrapped(env)

    assert len(calls) == 2
