from __future__ import annotations

import abc
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from common.messages import Envelope, MessageType

Handler = Callable[[Envelope], Awaitable[None]]


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 0.1
    factor: float = 2.0
    max_delay: float = 5.0

    def delay_for(self, attempt: int) -> float:
        return min(self.max_delay, self.base_delay * (self.factor ** (attempt - 1)))


class MessageBroker(abc.ABC):
    @abc.abstractmethod
    async def start(self) -> None:
        ...

    @abc.abstractmethod
    async def stop(self) -> None:
        ...

    @abc.abstractmethod
    async def publish(self, envelope: Envelope) -> None:
        ...

    @abc.abstractmethod
    async def subscribe(self, subject: MessageType, handler: Handler, *, group: str) -> None:
        ...
