"""Эксперимент по надёжности: сообщение не теряется при сбоях.

Сценарий 1 — устойчивость к сбоям обработчика: при высокой доле сбоев шага каждая заявка
всё равно достигает терминального состояния (повторы → COMPLETED, либо компенсация → FAILED),
ни одна не теряется и не «зависает».

Сценарий 2 — гарантия Outbox: при недоступном публикаторе (брокер «недостижим») заявки
не теряются, а ждут в Outbox и доставляются после восстановления.

Запуск:  .venv/Scripts/python.exe experiments/reliability.py
"""
from __future__ import annotations

import asyncio
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import coordinator.saga as coordinator
import query_service.projection as query
import worker_finalize.worker as wf
import worker_process.worker as wp
import worker_validate.worker as wv
from common.broker.base import RetryPolicy
from common.broker.in_memory import InMemoryBroker
from common.config import Settings
from common.idempotency import InMemoryIdempotencyStore
from common.messages import Envelope, MessageType, new_id
from common.observability import MESSAGES_FAILED
from common.outbox import InMemoryOutbox, OutboxPublisher

try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

FAST = dict(validate_duration=0.004, process_duration=0.02, finalize_duration=0.004)


async def setup(fail_rate: float):
    settings = Settings(process_fail_rate=fail_rate, max_attempts=5, retry_base_delay=0.02, **FAST)
    broker = InMemoryBroker(
        RetryPolicy(max_attempts=settings.max_attempts, base_delay=settings.retry_base_delay)
    )
    idem = InMemoryIdempotencyStore()
    await coordinator.setup(broker)
    rm = await query.setup(broker)
    await wv.setup(broker, settings, idem)
    await wp.setup(broker, settings, idem)
    await wf.setup(broker, settings, idem)
    await broker.start()
    outbox = InMemoryOutbox()
    pub = OutboxPublisher(outbox, broker, interval=0.02)
    return settings, broker, rm, outbox, pub


async def submit(outbox: InMemoryOutbox, n: int) -> list[str]:
    ids = []
    for _ in range(n):
        jid = new_id()
        await outbox.add(Envelope(correlation_id=jid, type=MessageType.JOB_SUBMITTED,
                                  payload={"data": {"n": 1}}))
        ids.append(jid)
    return ids


def status_of(rm, jid: str) -> str | None:
    j = rm.get(jid)
    return j["status"] if j else None


async def wait_terminal(rm, ids: list[str], timeout: float = 60.0) -> list[str]:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        st = [status_of(rm, i) for i in ids]
        if all(s in ("COMPLETED", "FAILED") for s in st):
            return st
        await asyncio.sleep(0.05)
    return [status_of(rm, i) for i in ids]


def failed_metric() -> int:
    return int(MESSAGES_FAILED.labels(type="ProcessStep", group="process")._value.get())


async def scenario_failures(n: int, fail_rate: float) -> dict:
    random.seed(42)
    settings, broker, rm, outbox, pub = await setup(fail_rate)
    await pub.start()
    before = failed_metric()
    ids = await submit(outbox, n)
    st = await wait_terminal(rm, ids)
    retries = failed_metric() - before
    await pub.stop()
    await broker.stop()
    completed = st.count("COMPLETED")
    failed = st.count("FAILED")
    lost = sum(1 for s in st if s not in ("COMPLETED", "FAILED"))
    return {
        "n": n, "fail_rate": fail_rate, "completed": completed, "failed_compensated": failed,
        "lost": lost, "retries": retries, "dlq": len(broker.dead_letters),
    }


async def scenario_outbox(n: int) -> dict:
    settings, broker, rm, outbox, pub = await setup(fail_rate=0.0)
    # публикатор НЕ запущен — брокер «недостижим»
    ids = await submit(outbox, n)
    await asyncio.sleep(0.3)
    reached = sum(1 for i in ids if status_of(rm, i) not in (None, "NEW"))
    pending_in_outbox = len(await outbox.fetch_unsent())
    # восстановление
    await pub.start()
    st = await wait_terminal(rm, ids)
    await pub.stop()
    await broker.stop()
    return {
        "n": n, "reached_broker_during_outage": reached, "waited_in_outbox": pending_in_outbox,
        "completed_after_recovery": st.count("COMPLETED"),
        "lost": sum(1 for s in st if s not in ("COMPLETED", "FAILED")),
    }


async def main() -> None:
    print("Эксперимент по надёжности (стенд в одном процессе, малые длительности)\n")
    s1 = await scenario_failures(n=200, fail_rate=0.5)
    print("Сценарий 1 — устойчивость к сбоям обработчика (доля сбоев шага 50%, до 5 попыток):")
    print(f"  заявок:               {s1['n']}")
    print(f"  COMPLETED (повторы):  {s1['completed']}")
    print(f"  FAILED (компенсация): {s1['failed_compensated']}")
    print(f"  ПОТЕРЯНО / зависло:   {s1['lost']}")
    print(f"  всего повторов шага:  {s1['retries']}")
    print(f"  в DLQ:                {s1['dlq']}")
    print()
    s2 = await scenario_outbox(n=50)
    print("Сценарий 2 — гарантия Outbox (публикатор остановлен = брокер недостижим):")
    print(f"  заявок:                       {s2['n']}")
    print(f"  дошло до брокера во время сбоя:{s2['reached_broker_during_outage']}")
    print(f"  ждали в Outbox:               {s2['waited_in_outbox']}")
    print(f"  COMPLETED после восстановления:{s2['completed_after_recovery']}")
    print(f"  ПОТЕРЯНО:                     {s2['lost']}")


if __name__ == "__main__":
    asyncio.run(main())
