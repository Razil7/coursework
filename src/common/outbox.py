from __future__ import annotations

import abc
import asyncio
from dataclasses import dataclass

from common.broker.base import MessageBroker
from common.messages import Envelope
from common.observability import get_logger

log = get_logger("outbox")


class OutboxStore(abc.ABC):
    @abc.abstractmethod
    async def add(self, env: Envelope) -> None: ...

    @abc.abstractmethod
    async def fetch_unsent(self, limit: int = 100) -> list[tuple[int, Envelope]]: ...

    @abc.abstractmethod
    async def mark_sent(self, row_id: int) -> None: ...


@dataclass
class _Row:
    id: int
    env: Envelope
    sent: bool = False


class InMemoryOutbox(OutboxStore):
    def __init__(self) -> None:
        self._rows: list[_Row] = []
        self._seq = 0
        self._lock = asyncio.Lock()

    async def add(self, env: Envelope) -> None:
        async with self._lock:
            self._seq += 1
            self._rows.append(_Row(self._seq, env))

    async def fetch_unsent(self, limit: int = 100) -> list[tuple[int, Envelope]]:
        return [(r.id, r.env) for r in self._rows if not r.sent][:limit]

    async def mark_sent(self, row_id: int) -> None:
        for r in self._rows:
            if r.id == row_id:
                r.sent = True
                return


class OutboxPublisher:
    def __init__(self, store: OutboxStore, broker: MessageBroker, interval: float = 0.05) -> None:
        self._store = store
        self._broker = broker
        self._interval = interval
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _loop(self) -> None:
        while self._running:
            try:
                for row_id, env in await self._store.fetch_unsent():
                    await self._broker.publish(env)
                    await self._store.mark_sent(row_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("outbox_publish_error", error=str(exc))
            await asyncio.sleep(self._interval)
