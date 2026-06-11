"""Нагрузочный эксперимент: сравнение синхронного и асинхронного приёма (см. п. 3.12).

Создаёт фиксированное число конкурентных «пользователей», которые в течение заданного
времени непрерывно шлют запросы на эндпоинт, и измеряет задержку ответа и пропускную
способность. Сравниваются:

* ``POST /jobs``      — асинхронный приём (ответ 202 сразу после записи в outbox);
* ``POST /jobs/sync`` — синхронная блокирующая оркестрация (ответ по завершении всех шагов).

Запуск (стенд должен быть поднят — режим A или B):
    python experiments/bench.py --base http://127.0.0.1:8000 --duration 10 --concurrency 50
"""
from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
import time
from typing import Any

import httpx

try:  # вывод в UTF-8 (на Windows консоль по умолчанию cp1251)
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(len(ordered) * p))
    return ordered[idx]


async def _user(client: httpx.AsyncClient, url: str, stop_at: float, latencies: list[float],
                errors: list[int]) -> None:
    payload = {"data": {"n": 1}}
    while time.perf_counter() < stop_at:
        t0 = time.perf_counter()
        try:
            resp = await client.post(url, json=payload)
            resp.read()
            if resp.status_code >= 400:
                errors.append(resp.status_code)
        except httpx.HTTPError:
            errors.append(-1)
        latencies.append(time.perf_counter() - t0)


async def measure(base: str, path: str, duration: float, concurrency: int) -> dict[str, Any]:
    url = base + path
    latencies: list[float] = []
    errors: list[int] = []
    async with httpx.AsyncClient(timeout=60.0) as client:
        stop_at = time.perf_counter() + duration
        started = time.perf_counter()
        await asyncio.gather(
            *[_user(client, url, stop_at, latencies, errors) for _ in range(concurrency)]
        )
        elapsed = time.perf_counter() - started
    n = len(latencies)
    return {
        "path": path,
        "requests": n,
        "errors": len(errors),
        "throughput_rps": round(n / elapsed, 1) if elapsed else 0.0,
        "p50_ms": round(percentile(latencies, 0.50) * 1000, 1),
        "p95_ms": round(percentile(latencies, 0.95) * 1000, 1),
        "p99_ms": round(percentile(latencies, 0.99) * 1000, 1),
        "mean_ms": round(statistics.fmean(latencies) * 1000, 1) if latencies else 0.0,
    }


def _print_table(rows: list[dict[str, Any]]) -> None:
    cols = ["path", "requests", "errors", "throughput_rps", "mean_ms", "p50_ms", "p95_ms", "p99_ms"]
    widths = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in cols}
    line = " | ".join(c.ljust(widths[c]) for c in cols)
    print(line)
    print("-" * len(line))
    for r in rows:
        print(" | ".join(str(r[c]).ljust(widths[c]) for c in cols))


async def main() -> None:
    parser = argparse.ArgumentParser(description="sync vs async benchmark")
    parser.add_argument("--base", default="http://127.0.0.1:8000")
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--concurrency", type=int, default=50)
    args = parser.parse_args()

    print(
        f"Базовый URL: {args.base}, длительность: {args.duration}s, "
        f"конкурентность: {args.concurrency}\n"
    )
    rows = []
    for path in ("/jobs", "/jobs/sync"):
        print(f"Измерение {path} ...")
        rows.append(await measure(args.base, path, args.duration, args.concurrency))
    print()
    _print_table(rows)


if __name__ == "__main__":
    asyncio.run(main())
