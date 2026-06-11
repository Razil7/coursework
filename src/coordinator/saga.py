from __future__ import annotations

from common.broker.base import MessageBroker
from common.messages import Envelope, MessageType
from common.observability import get_logger
from coordinator.state import SagaState, SagaStatus

log = get_logger("coordinator")
GROUP = "coordinator"

COMPENSATIONS: dict[str, MessageType] = {"validate": MessageType.COMPENSATE_VALIDATE}


class Coordinator:
    def __init__(self, broker: MessageBroker) -> None:
        self._broker = broker
        self._sagas: dict[str, SagaState] = {}

    def saga(self, job_id: str) -> SagaState:
        return self._sagas.setdefault(job_id, SagaState(job_id=job_id))


    async def on_job_submitted(self, env: Envelope) -> None:
        saga = self.saga(env.correlation_id)
        saga.status = SagaStatus.VALIDATING
        log.info("saga_start", job=saga.job_id, status=str(saga.status))
        await self._broker.publish(env.caused(MessageType.VALIDATE_STEP, env.payload))

    async def on_step_validated(self, env: Envelope) -> None:
        saga = self.saga(env.correlation_id)
        saga.completed_steps.append("validate")
        saga.status = SagaStatus.PROCESSING
        await self._broker.publish(env.caused(MessageType.PROCESS_STEP, env.payload))

    async def on_step_processed(self, env: Envelope) -> None:
        saga = self.saga(env.correlation_id)
        saga.completed_steps.append("process")
        saga.status = SagaStatus.FINALIZING
        await self._broker.publish(env.caused(MessageType.FINALIZE_STEP, env.payload))

    async def on_job_completed(self, env: Envelope) -> None:
        saga = self.saga(env.correlation_id)
        saga.status = SagaStatus.COMPLETED
        log.info("saga_completed", job=saga.job_id)


    async def on_step_failed(self, env: Envelope) -> None:
        saga = self.saga(env.correlation_id)
        saga.status = SagaStatus.COMPENSATING
        saga.error = str(env.payload.get("error"))
        log.error("saga_compensating", job=saga.job_id, completed=saga.completed_steps)
        to_compensate = [s for s in reversed(saga.completed_steps) if s in COMPENSATIONS]
        if not to_compensate:
            await self._fail(env)
            return
        for step in to_compensate:
            await self._broker.publish(env.caused(COMPENSATIONS[step]))

    async def on_validate_compensated(self, env: Envelope) -> None:
        await self._fail(env)

    async def _fail(self, env: Envelope) -> None:
        saga = self.saga(env.correlation_id)
        saga.status = SagaStatus.FAILED
        log.error("saga_failed", job=saga.job_id, error=saga.error)
        await self._broker.publish(env.caused(MessageType.JOB_FAILED, {"error": saga.error}))


async def setup(broker: MessageBroker) -> Coordinator:
    c = Coordinator(broker)
    await broker.subscribe(MessageType.JOB_SUBMITTED, c.on_job_submitted, group=GROUP)
    await broker.subscribe(MessageType.STEP_VALIDATED, c.on_step_validated, group=GROUP)
    await broker.subscribe(MessageType.STEP_PROCESSED, c.on_step_processed, group=GROUP)
    await broker.subscribe(MessageType.JOB_COMPLETED, c.on_job_completed, group=GROUP)
    await broker.subscribe(MessageType.STEP_FAILED, c.on_step_failed, group=GROUP)
    await broker.subscribe(MessageType.VALIDATE_COMPENSATED, c.on_validate_compensated, group=GROUP)
    return c
