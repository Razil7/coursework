from __future__ import annotations

from functools import partial

from common.broker.base import MessageBroker
from common.config import Settings
from common.idempotency import IdempotencyStore, idempotent
from common.messages import Envelope, MessageType
from common.observability import get_logger
from common.work import simulate_work

log = get_logger("worker.validate")
GROUP = "validate"


async def handle_validate(broker: MessageBroker, settings: Settings, env: Envelope) -> None:
    log.info("validate_start", job=env.correlation_id, attempt=env.attempt)
    result = await simulate_work(label="validate", duration_s=settings.validate_duration)
    await broker.publish(env.caused(MessageType.STEP_VALIDATED, {"validate": result}))
    log.info("validate_done", job=env.correlation_id)


async def handle_compensate(broker: MessageBroker, settings: Settings, env: Envelope) -> None:
    log.info("validate_compensate", job=env.correlation_id)
    await broker.publish(env.caused(MessageType.VALIDATE_COMPENSATED))


async def setup(broker: MessageBroker, settings: Settings, idem: IdempotencyStore) -> None:
    await broker.subscribe(
        MessageType.VALIDATE_STEP,
        idempotent(idem, partial(handle_validate, broker, settings)),
        group=GROUP,
    )
    await broker.subscribe(
        MessageType.COMPENSATE_VALIDATE,
        partial(handle_compensate, broker, settings),
        group=GROUP,
    )
