from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Coroutine
from typing import Any

from common.broker.base import Handler, MessageBroker, RetryPolicy
from common.messages import Envelope, MessageType
from common.observability import (
    HANDLER_DURATION,
    MESSAGES_CONSUMED,
    MESSAGES_DEAD_LETTERED,
    MESSAGES_FAILED,
    MESSAGES_PUBLISHED,
    correlation,
    get_logger,
)

log = get_logger("broker.memory")


class _Group:
    def __init__(self, subject: str, group: str) -> None:
        self.subject = subject
        self.group = group
        self.queue: asyncio.Queue[Envelope] = asyncio.Queue()
        self.handlers: list[Handler] = []
        self.tasks: list[asyncio.Task[None]] = []


class InMemoryBroker(MessageBroker):
    def __init__(self, retry: RetryPolicy | None = None) -> None:
        self._groups: dict[str, dict[str, _Group]] = defaultdict(dict)
        self._retry = retry or RetryPolicy()
        self._running = False
        self._bg: set[asyncio.Task[None]] = set()
        self.dead_letters: list[Envelope] = []

    async def start(self) -> None:
        self._running = True
        for by_group in self._groups.values():
            for grp in by_group.values():
                self._ensure_consumers(grp)
        log.info("broker_started", subjects=list(self._groups))

    async def stop(self) -> None:
        self._running = False
        tasks = [
            t
            for by_group in self._groups.values()
            for grp in by_group.values()
            for t in grp.tasks
        ]
        tasks += list(self._bg)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def subscribe(self, subject: MessageType, handler: Handler, *, group: str) -> None:
        subj = subject.value
        grp = self._groups[subj].get(group)
        if grp is None:
            grp = _Group(subj, group)
            self._groups[subj][group] = grp
        grp.handlers.append(handler)
        if self._running:
            self._ensure_consumers(grp)

    async def publish(self, envelope: Envelope) -> None:
        subj = envelope.subject
        MESSAGES_PUBLISHED.labels(type=subj).inc()
        groups = self._groups.get(subj)
        if not groups:
            log.warning("no_subscribers", subject=subj)
            return
        for grp in groups.values():
            await grp.queue.put(envelope.model_copy(deep=True))


    def _ensure_consumers(self, grp: _Group) -> None:
        while len(grp.tasks) < len(grp.handlers):
            handler = grp.handlers[len(grp.tasks)]
            grp.tasks.append(asyncio.create_task(self._consume(grp, handler)))

    async def _consume(self, grp: _Group, handler: Handler) -> None:
        while True:
            env = await grp.queue.get()
            try:
                await self._dispatch(grp, handler, env)
            finally:
                grp.queue.task_done()

    async def _dispatch(self, grp: _Group, handler: Handler, env: Envelope) -> None:
        with correlation(env.correlation_id):
            try:
                with HANDLER_DURATION.labels(type=env.subject, group=grp.group).time():
                    await handler(env)
                MESSAGES_CONSUMED.labels(type=env.subject, group=grp.group).inc()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                MESSAGES_FAILED.labels(type=env.subject, group=grp.group).inc()
                await self._handle_failure(grp, env, exc)

    async def _handle_failure(self, grp: _Group, env: Envelope, exc: Exception) -> None:
        if env.attempt < self._retry.max_attempts:
            delay = self._retry.delay_for(env.attempt)
            log.warning(
                "retry",
                subject=env.subject,
                group=grp.group,
                attempt=env.attempt,
                delay=round(delay, 3),
                error=str(exc),
            )
            self._spawn(self._requeue_after(grp, env.next_attempt(), delay))
        else:
            self.dead_letters.append(env)
            MESSAGES_DEAD_LETTERED.labels(type=env.subject).inc()
            log.error(
                "dead_letter",
                subject=env.subject,
                group=grp.group,
                attempt=env.attempt,
                error=str(exc),
            )

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    async def _requeue_after(self, grp: _Group, env: Envelope, delay: float) -> None:
        await asyncio.sleep(delay)
        await grp.queue.put(env)
