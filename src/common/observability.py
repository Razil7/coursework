from __future__ import annotations

import logging
import sys
from collections.abc import Iterator, MutableMapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

import structlog
from prometheus_client import Counter, Histogram

_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)

MESSAGES_PUBLISHED = Counter("ams_messages_published_total", "Опубликовано сообщений", ["type"])
MESSAGES_CONSUMED = Counter(
    "ams_messages_consumed_total", "Успешно обработано сообщений", ["type", "group"]
)
MESSAGES_FAILED = Counter("ams_messages_failed_total", "Сбоев обработки", ["type", "group"])
MESSAGES_DEAD_LETTERED = Counter(
    "ams_messages_dead_lettered_total", "Сообщений в очереди недоставленных", ["type"]
)
HANDLER_DURATION = Histogram(
    "ams_handler_duration_seconds", "Длительность обработки сообщения", ["type", "group"]
)


def set_correlation_id(value: str | None) -> None:
    _correlation_id.set(value)


def get_correlation_id() -> str | None:
    return _correlation_id.get()


@contextmanager
def correlation(value: str | None) -> Iterator[None]:
    token = _correlation_id.set(value)
    try:
        yield
    finally:
        _correlation_id.reset(token)


def _inject_correlation(
    _logger: Any, _name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    cid = _correlation_id.get()
    if cid is not None:
        event_dict.setdefault("correlation_id", cid)
    return event_dict


_configured = False


def configure_logging(level: str = "INFO") -> None:
    global _configured
    if _configured:
        return
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
        except (AttributeError, ValueError):
            pass
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(format="%(message)s", level=log_level)
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            _inject_correlation,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _configured = True


def get_logger(name: str) -> Any:
    if not _configured:
        configure_logging()
    return structlog.get_logger(name)
