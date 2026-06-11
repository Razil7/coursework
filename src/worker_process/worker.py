from __future__ import annotations

from functools import partial

from common.broker.base import MessageBroker
from common.config import Settings
from common.idempotency import IdempotencyStore, idempotent
from common.messages import Envelope, MessageType
from common.observability import get_logger
from common.work import StepError, simulate_work

log = get_logger("worker.process")
GROUP = "process"


async def handle_process(broker: MessageBroker, settings: Settings, env: Envelope) -> None:
    log.info("process_start", job=env.correlation_id, attempt=env.attempt)
    try:
        result = await simulate_work(
            label="process",
            duration_s=settings.process_duration,
            fail_rate=settings.process_fail_rate,
        )
    except StepError as exc:
        if env.attempt < settings.max_attempts:
            raise
        log.error("process_failed_permanently", job=env.correlation_id, error=str(exc))
        await broker.publish(
            env.caused(MessageType.STEP_FAILED, {"step": "process", "error": str(exc)})
        )
        return
    await broker.publish(env.caused(MessageType.STEP_PROCESSED, {"process": result}))
    log.info("process_done", job=env.correlation_id)


async def setup(broker: MessageBroker, settings: Settings, idem: IdempotencyStore) -> None:
    await broker.subscribe(
        MessageType.PROCESS_STEP,
        idempotent(idem, partial(handle_process, broker, settings)),
        group=GROUP,
    )
