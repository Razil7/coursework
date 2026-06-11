from __future__ import annotations

from typing import Any

from common.broker.base import MessageBroker
from common.messages import Envelope, MessageType
from common.observability import get_logger

log = get_logger("query")
GROUP = "query"


class ReadModel:
    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}

    def get(self, job_id: str) -> dict[str, Any] | None:
        return self._jobs.get(job_id)

    def all(self) -> list[dict[str, Any]]:
        return list(self._jobs.values())

    def _job(self, job_id: str) -> dict[str, Any]:
        return self._jobs.setdefault(
            job_id, {"job_id": job_id, "status": "NEW", "result": None}
        )

    async def on_submitted(self, env: Envelope) -> None:
        self._job(env.correlation_id)["status"] = "PENDING"

    async def on_validated(self, env: Envelope) -> None:
        self._job(env.correlation_id)["status"] = "PROCESSING"

    async def on_processed(self, env: Envelope) -> None:
        self._job(env.correlation_id)["status"] = "FINALIZING"

    async def on_completed(self, env: Envelope) -> None:
        job = self._job(env.correlation_id)
        job["status"] = "COMPLETED"
        job["result"] = env.payload

    async def on_failed(self, env: Envelope) -> None:
        job = self._job(env.correlation_id)
        job["status"] = "FAILED"
        job["result"] = env.payload


async def setup(broker: MessageBroker) -> ReadModel:
    rm = ReadModel()
    await broker.subscribe(MessageType.JOB_SUBMITTED, rm.on_submitted, group=GROUP)
    await broker.subscribe(MessageType.STEP_VALIDATED, rm.on_validated, group=GROUP)
    await broker.subscribe(MessageType.STEP_PROCESSED, rm.on_processed, group=GROUP)
    await broker.subscribe(MessageType.JOB_COMPLETED, rm.on_completed, group=GROUP)
    await broker.subscribe(MessageType.JOB_FAILED, rm.on_failed, group=GROUP)
    return rm
