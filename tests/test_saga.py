from __future__ import annotations

from typing import Any

from common.broker.base import Handler, MessageBroker
from common.config import Settings
from common.messages import Envelope, MessageType, new_id
from coordinator.saga import Coordinator
from coordinator.state import SagaStatus


def _fast(**overrides: float | int) -> Settings:
    base = dict(
        validate_duration=0.0,
        process_duration=0.0,
        finalize_duration=0.0,
        process_fail_rate=0.0,
        max_attempts=3,
        retry_base_delay=0.01,
    )
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


async def test_saga_happy_path(make_stand, wait_for) -> None:
    broker, rm = await make_stand(_fast())
    job_id = new_id()
    await broker.publish(
        Envelope(correlation_id=job_id, type=MessageType.JOB_SUBMITTED, payload={"x": 1})
    )

    assert await wait_for(lambda: (rm.get(job_id) or {}).get("status") == "COMPLETED")
    job = rm.get(job_id)
    assert job is not None
    assert job["status"] == "COMPLETED"
    assert job["result"]["finalize"]["label"] == "finalize"


async def test_saga_compensation_on_permanent_failure(make_stand, wait_for) -> None:
    broker, rm = await make_stand(_fast(process_fail_rate=1.0, max_attempts=2))
    job_id = new_id()
    await broker.publish(Envelope(correlation_id=job_id, type=MessageType.JOB_SUBMITTED))

    assert await wait_for(lambda: (rm.get(job_id) or {}).get("status") == "FAILED")
    job = rm.get(job_id)
    assert job is not None
    assert job["status"] == "FAILED"


class RecordingBroker(MessageBroker):
    def __init__(self) -> None:
        self.published: list[Envelope] = []

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def publish(self, envelope: Envelope) -> None:
        self.published.append(envelope)

    async def subscribe(self, subject: MessageType, handler: Handler, *, group: str) -> None: ...


def _event(job_id: str, t: MessageType, payload: dict[str, Any] | None = None) -> Envelope:
    return Envelope(correlation_id=job_id, type=t, payload=payload or {})


async def test_coordinator_happy_path_command_sequence_and_states() -> None:
    rec = RecordingBroker()
    c = Coordinator(rec)
    job = "job-1"

    await c.on_job_submitted(_event(job, MessageType.JOB_SUBMITTED))
    assert c.saga(job).status is SagaStatus.VALIDATING

    await c.on_step_validated(_event(job, MessageType.STEP_VALIDATED))
    assert c.saga(job).status is SagaStatus.PROCESSING
    assert c.saga(job).completed_steps == ["validate"]

    await c.on_step_processed(_event(job, MessageType.STEP_PROCESSED))
    assert c.saga(job).status is SagaStatus.FINALIZING
    assert c.saga(job).completed_steps == ["validate", "process"]

    await c.on_job_completed(_event(job, MessageType.JOB_COMPLETED))
    assert c.saga(job).status is SagaStatus.COMPLETED

    assert [e.type for e in rec.published] == [
        MessageType.VALIDATE_STEP,
        MessageType.PROCESS_STEP,
        MessageType.FINALIZE_STEP,
    ]
    assert all(e.correlation_id == job for e in rec.published)


async def test_coordinator_compensation_publishes_compensate_then_jobfailed() -> None:
    rec = RecordingBroker()
    c = Coordinator(rec)
    job = "job-2"

    await c.on_job_submitted(_event(job, MessageType.JOB_SUBMITTED))
    await c.on_step_validated(_event(job, MessageType.STEP_VALIDATED))
    await c.on_step_failed(_event(job, MessageType.STEP_FAILED, {"error": "boom"}))
    assert c.saga(job).status is SagaStatus.COMPENSATING

    await c.on_validate_compensated(_event(job, MessageType.VALIDATE_COMPENSATED))
    assert c.saga(job).status is SagaStatus.FAILED

    assert [e.type for e in rec.published] == [
        MessageType.VALIDATE_STEP,
        MessageType.PROCESS_STEP,
        MessageType.COMPENSATE_VALIDATE,
        MessageType.JOB_FAILED,
    ]
    assert rec.published[-1].payload == {"error": "boom"}


async def test_coordinator_fail_without_completed_steps_skips_compensation() -> None:
    rec = RecordingBroker()
    c = Coordinator(rec)
    job = "job-3"

    await c.on_job_submitted(_event(job, MessageType.JOB_SUBMITTED))
    await c.on_step_failed(_event(job, MessageType.STEP_FAILED, {"error": "early"}))
    assert c.saga(job).status is SagaStatus.FAILED

    types = [e.type for e in rec.published]
    assert MessageType.COMPENSATE_VALIDATE not in types
    assert types == [MessageType.VALIDATE_STEP, MessageType.JOB_FAILED]
