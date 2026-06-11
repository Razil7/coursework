from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractIncomingMessage

from common.broker.base import Handler, MessageBroker, RetryPolicy
from common.messages import Envelope, MessageType
from common.observability import (
    MESSAGES_CONSUMED,
    MESSAGES_DEAD_LETTERED,
    MESSAGES_FAILED,
    MESSAGES_PUBLISHED,
    correlation,
    get_logger,
)

log = get_logger("broker.rabbitmq")

EXCHANGE = "ams.events"
DEAD_LETTER_EXCHANGE = "ams.dlx"


class RabbitMQAdapter(MessageBroker):
    def __init__(self, url: str, retry: RetryPolicy | None = None, prefetch: int = 16) -> None:
        self._url = url
        self._retry = retry or RetryPolicy()
        self._prefetch = prefetch
        self._conn: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._exchange: AbstractExchange | None = None
        self._subs: list[tuple[MessageType, Handler, str]] = []

    async def start(self) -> None:
        self._conn = await aio_pika.connect_robust(self._url)
        self._channel = await self._conn.channel(publisher_confirms=True)
        await self._channel.set_qos(prefetch_count=self._prefetch)
        self._exchange = await self._channel.declare_exchange(
            EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
        )
        dlx = await self._channel.declare_exchange(
            DEAD_LETTER_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
        )
        dlq = await self._channel.declare_queue("ams.dead-letter", durable=True)
        await dlq.bind(dlx, routing_key="#")
        for subject, handler, group in self._subs:
            await self._bind(subject, handler, group)
        log.info("rabbitmq_started", url=self._url)

    async def stop(self) -> None:
        if self._conn is not None:
            await self._conn.close()

    async def subscribe(self, subject: MessageType, handler: Handler, *, group: str) -> None:
        self._subs.append((subject, handler, group))
        if self._channel is not None:
            await self._bind(subject, handler, group)

    def _build_message(self, env: Envelope) -> aio_pika.Message:
        return aio_pika.Message(
            body=env.model_dump_json().encode(),
            content_type="application/json",
            message_id=env.message_id,
            correlation_id=env.correlation_id,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

    async def publish(self, envelope: Envelope) -> None:
        assert self._exchange is not None, "broker is not started"
        MESSAGES_PUBLISHED.labels(type=envelope.subject).inc()
        await self._exchange.publish(self._build_message(envelope), routing_key=envelope.subject)


    async def _bind(self, subject: MessageType, handler: Handler, group: str) -> None:
        assert self._channel is not None and self._exchange is not None
        queue = await self._channel.declare_queue(
            f"{subject.value}.{group}",
            durable=True,
            arguments={"x-dead-letter-exchange": DEAD_LETTER_EXCHANGE},
        )
        await queue.bind(self._exchange, routing_key=subject.value)
        await queue.consume(self._make_consumer(handler, group))

    def _make_consumer(
        self, handler: Handler, group: str
    ) -> Callable[[AbstractIncomingMessage], Awaitable[None]]:
        async def on_message(message: AbstractIncomingMessage) -> None:
            env = Envelope.model_validate_json(message.body)
            with correlation(env.correlation_id):
                try:
                    await handler(env)
                    MESSAGES_CONSUMED.labels(type=env.subject, group=group).inc()
                    await message.ack()
                except Exception as exc:
                    MESSAGES_FAILED.labels(type=env.subject, group=group).inc()
                    await self._on_failure(env, message, group, exc)

        return on_message

    async def _on_failure(
        self, env: Envelope, message: AbstractIncomingMessage, group: str, exc: Exception
    ) -> None:
        assert self._channel is not None
        if env.attempt < self._retry.max_attempts:
            await asyncio.sleep(self._retry.delay_for(env.attempt))
            log.warning(
                "retry", subject=env.subject, group=group, attempt=env.attempt, error=str(exc)
            )
            retry_env = env.next_attempt()
            await self._channel.default_exchange.publish(
                self._build_message(retry_env), routing_key=f"{retry_env.subject}.{group}"
            )
            await message.ack()
        else:
            MESSAGES_DEAD_LETTERED.labels(type=env.subject).inc()
            log.error("dead_letter", subject=env.subject, group=group, error=str(exc))
            await message.reject(requeue=False)
