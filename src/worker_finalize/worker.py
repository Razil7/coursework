from __future__ import annotations

from functools import partial

from common.broker.base import MessageBroker
from common.config import Settings
from common.idempotency import IdempotencyStore, idempotent
from common.messages import Envelope, MessageType
from common.observability import get_logger
from common.work import simulate_work

log = get_logger("worker.finalize")
GROUP = "finalize"


async def handle_finalize(broker: MessageBroker, settings: Settings, env: Envelope) -> None:
    log.info("finalize_start", job=env.correlation_id, attempt=env.attempt)
    result = await simulate_work(label="finalize", duration_s=settings.finalize_duration)
    await broker.publish(env.caused(MessageType.JOB_COMPLETED, {"finalize": result}))
    log.info("finalize_done", job=env.correlation_id)


async def setup(broker: MessageBroker, settings: Settings, idem: IdempotencyStore) -> None:
    await broker.subscribe(
        MessageType.FINALIZE_STEP,
        idempotent(idem, partial(handle_finalize, broker, settings)),
        group=GROUP,
    )
