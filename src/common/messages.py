from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class MessageKind(StrEnum):
    COMMAND = "command"
    EVENT = "event"


class MessageType(StrEnum):
    JOB_SUBMITTED = "JobSubmitted"
    STEP_VALIDATED = "StepValidated"
    STEP_PROCESSED = "StepProcessed"
    JOB_COMPLETED = "JobCompleted"
    STEP_FAILED = "StepFailed"
    VALIDATE_COMPENSATED = "ValidateCompensated"
    JOB_FAILED = "JobFailed"
    VALIDATE_STEP = "ValidateStep"
    PROCESS_STEP = "ProcessStep"
    FINALIZE_STEP = "FinalizeStep"
    COMPENSATE_VALIDATE = "CompensateValidate"


COMMANDS: frozenset[MessageType] = frozenset(
    {
        MessageType.VALIDATE_STEP,
        MessageType.PROCESS_STEP,
        MessageType.FINALIZE_STEP,
        MessageType.COMPENSATE_VALIDATE,
    }
)


def kind_of(message_type: MessageType) -> MessageKind:
    return MessageKind.COMMAND if message_type in COMMANDS else MessageKind.EVENT


def new_id() -> str:
    return uuid.uuid4().hex


def utcnow() -> datetime:
    return datetime.now(UTC)


class Envelope(BaseModel):
    message_id: str = Field(default_factory=new_id)
    correlation_id: str
    causation_id: str | None = None
    type: MessageType
    occurred_at: datetime = Field(default_factory=utcnow)
    attempt: int = 1
    payload: dict[str, Any] = Field(default_factory=dict)

    @property
    def subject(self) -> str:
        return self.type.value

    @property
    def kind(self) -> MessageKind:
        return kind_of(self.type)

    def caused(self, message_type: MessageType, payload: dict[str, Any] | None = None) -> Envelope:
        return Envelope(
            correlation_id=self.correlation_id,
            causation_id=self.message_id,
            type=message_type,
            payload=payload if payload is not None else {},
        )

    def next_attempt(self) -> Envelope:
        return self.model_copy(update={"attempt": self.attempt + 1})
