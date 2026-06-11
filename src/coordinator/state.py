from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class SagaStatus(StrEnum):
    PENDING = "PENDING"
    VALIDATING = "VALIDATING"
    PROCESSING = "PROCESSING"
    FINALIZING = "FINALIZING"
    COMPLETED = "COMPLETED"
    COMPENSATING = "COMPENSATING"
    FAILED = "FAILED"


@dataclass
class SagaState:
    job_id: str
    status: SagaStatus = SagaStatus.PENDING
    completed_steps: list[str] = field(default_factory=list)
    error: str | None = None
