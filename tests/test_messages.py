from __future__ import annotations

from common.messages import COMMANDS, Envelope, MessageKind, MessageType, kind_of


def _parent() -> Envelope:
    return Envelope(correlation_id="job-1", type=MessageType.JOB_SUBMITTED, payload={"n": 1})


def test_caused_preserves_correlation_and_sets_causation() -> None:
    parent = _parent()
    child = parent.caused(MessageType.VALIDATE_STEP, {"a": 1})
    assert child.correlation_id == parent.correlation_id
    assert child.causation_id == parent.message_id
    assert child.type is MessageType.VALIDATE_STEP
    assert child.attempt == 1
    assert child.payload == {"a": 1}
    assert child.message_id != parent.message_id


def test_caused_default_payload_is_empty() -> None:
    assert _parent().caused(MessageType.PROCESS_STEP).payload == {}


def test_next_attempt_increments_and_keeps_identity() -> None:
    env = _parent()
    retried = env.next_attempt()
    assert retried.attempt == env.attempt + 1
    assert retried.message_id == env.message_id
    assert retried.correlation_id == env.correlation_id
    assert retried.type is env.type


def test_kind_classification_commands_vs_events() -> None:
    for t in (
        MessageType.VALIDATE_STEP,
        MessageType.PROCESS_STEP,
        MessageType.FINALIZE_STEP,
        MessageType.COMPENSATE_VALIDATE,
    ):
        assert kind_of(t) is MessageKind.COMMAND
        assert Envelope(correlation_id="c", type=t).kind is MessageKind.COMMAND
    for t in (
        MessageType.JOB_SUBMITTED,
        MessageType.STEP_VALIDATED,
        MessageType.STEP_PROCESSED,
        MessageType.JOB_COMPLETED,
        MessageType.STEP_FAILED,
        MessageType.VALIDATE_COMPENSATED,
        MessageType.JOB_FAILED,
    ):
        assert kind_of(t) is MessageKind.EVENT
        assert Envelope(correlation_id="c", type=t).kind is MessageKind.EVENT


def test_commands_set_is_exactly_the_four_commands() -> None:
    assert COMMANDS == frozenset(
        {
            MessageType.VALIDATE_STEP,
            MessageType.PROCESS_STEP,
            MessageType.FINALIZE_STEP,
            MessageType.COMPENSATE_VALIDATE,
        }
    )


def test_subject_equals_type_value() -> None:
    env = Envelope(correlation_id="c", type=MessageType.JOB_SUBMITTED)
    assert env.subject == "JobSubmitted"


def test_envelope_json_roundtrip() -> None:
    env = _parent().caused(MessageType.STEP_VALIDATED, {"k": "v"})
    restored = Envelope.model_validate_json(env.model_dump_json())
    assert restored == env
