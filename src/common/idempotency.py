from __future__ import annotations

import abc

from common.broker.base import Handler
from common.messages import Envelope


class IdempotencyStore(abc.ABC):
    @abc.abstractmethod
    async def seen(self, message_id: str) -> bool: ...

    @abc.abstractmethod
    async def mark(self, message_id: str) -> None: ...


class InMemoryIdempotencyStore(IdempotencyStore):
    def __init__(self) -> None:
        self._seen: set[str] = set()

    async def seen(self, message_id: str) -> bool:
        return message_id in self._seen

    async def mark(self, message_id: str) -> None:
        self._seen.add(message_id)


def idempotent(store: IdempotencyStore, handler: Handler) -> Handler:
    async def wrapper(env: Envelope) -> None:
        if await store.seen(env.message_id):
            return
        await handler(env)
        await store.mark(env.message_id)

    return wrapper
