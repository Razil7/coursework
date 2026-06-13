from __future__ import annotations

import asyncio
import os
import statistics
import sys
import threading
import time
import webbrowser
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

import coordinator.saga as coordinator_mod
import query_service.projection as query_mod
import worker_finalize.worker as wf
import worker_process.worker as wp
import worker_validate.worker as wv
from common.broker.base import RetryPolicy
from common.broker.in_memory import InMemoryBroker
from common.config import Settings
from common.idempotency import InMemoryIdempotencyStore, idempotent
from common.messages import COMMANDS, Envelope, MessageType, new_id
from common.observability import configure_logging, correlation
from common.outbox import InMemoryOutbox, OutboxPublisher
from common.work import StepError, simulate_work

GUI_DIR = Path(__file__).resolve().parent
PORT = 8080
BASE_URL = f"http://127.0.0.1:{PORT}"
EVENT_CAP = 800
BENCH_MAX_REQUESTS = 600

STEP_ORDER = ["validate", "process", "finalize"]


def _small_payload(payload: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in list(payload.items())[:6]:
        text = repr(value)
        out[key] = text if len(text) <= 120 else text[:117] + "..."
    return out


class Stand:
    def __init__(self) -> None:
        self.settings = Settings()
        self.broker = InMemoryBroker(
            RetryPolicy(
                max_attempts=self.settings.max_attempts,
                base_delay=self.settings.retry_base_delay,
            )
        )
        self.idem = InMemoryIdempotencyStore()
        self.outbox = InMemoryOutbox()
        self.publisher = OutboxPublisher(self.outbox, self.broker)
        self.coordinator: coordinator_mod.Coordinator | None = None
        self.read_model: query_mod.ReadModel | None = None
        self.events: list[dict[str, Any]] = []
        self.seq = 0
        self.started_at = time.time()
        self.crash_remaining = {"validate": 0, "process": 0, "finalize": 0}
        self.counters = self._fresh_counters()

    @staticmethod
    def _fresh_counters() -> dict[str, Any]:
        return {"published": 0, "retries": 0, "dead_letters": 0, "by_type": {}}

    async def start(self) -> None:
        self.coordinator = await coordinator_mod.setup(self.broker)
        self.read_model = await query_mod.setup(self.broker)
        workers = [
            ("validate", MessageType.VALIDATE_STEP, "validate", wv.handle_validate),
            ("process", MessageType.PROCESS_STEP, "process", wp.handle_process),
            ("finalize", MessageType.FINALIZE_STEP, "finalize", wf.handle_finalize),
        ]
        for step, mtype, group, real in workers:
            await self._setup_worker_with_crash(step, mtype, group, real)
        await self.broker.subscribe(
            MessageType.COMPENSATE_VALIDATE,
            partial(wv.handle_compensate, self.broker, self.settings),
            group="validate",
        )
        self._install_hooks()
        await self.broker.start()
        await self.publisher.start()

    async def _setup_worker_with_crash(
        self, step: str, mtype: MessageType, group: str, real_handler: Any
    ) -> None:
        async def handler(env: Envelope) -> None:
            if self.crash_remaining.get(step, 0) > 0:
                self.crash_remaining[step] -= 1
                raise StepError(f"имитация падения сервиса worker-{step}")
            await real_handler(self.broker, self.settings, env)

        await self.broker.subscribe(mtype, idempotent(self.idem, handler), group=group)

    def set_crash(self, service: str, count: int) -> None:
        if service in self.crash_remaining:
            self.crash_remaining[service] = max(0, min(3, count))

    async def stop(self) -> None:
        await self.publisher.stop()
        await self.broker.stop()

    def _install_hooks(self) -> None:
        broker = self.broker
        original_publish = broker.publish
        original_failure = broker._handle_failure

        async def traced_publish(env: Envelope) -> None:
            self._record(env, "published")
            await original_publish(env)

        async def traced_failure(grp: Any, env: Envelope, exc: Exception) -> None:
            phase = "retry" if env.attempt < broker._retry.max_attempts else "dead_letter"
            self._record(env, phase, error=str(exc))
            await original_failure(grp, env, exc)

        broker.publish = traced_publish  # type: ignore[method-assign]
        broker._handle_failure = traced_failure  # type: ignore[method-assign]

    def _record(self, env: Envelope, phase: str, error: str | None = None) -> None:
        self.seq += 1
        kind = "command" if env.type in COMMANDS else "event"
        record = {
            "seq": self.seq,
            "ts": time.time(),
            "phase": phase,
            "type": env.type.value,
            "kind": kind,
            "correlation_id": env.correlation_id,
            "causation_id": env.causation_id,
            "message_id": env.message_id,
            "attempt": env.attempt,
            "error": error,
            "payload": _small_payload(env.payload),
        }
        self.events.append(record)
        if len(self.events) > EVENT_CAP:
            self.events = self.events[-EVENT_CAP:]
        if phase == "published":
            self.counters["published"] += 1
            by_type = self.counters["by_type"]
            by_type[env.type.value] = by_type.get(env.type.value, 0) + 1
        elif phase == "retry":
            self.counters["retries"] += 1
        elif phase == "dead_letter":
            self.counters["dead_letters"] += 1

    async def submit_async(self, data: dict[str, Any]) -> str:
        job_id = new_id()
        with correlation(job_id):
            env = Envelope(correlation_id=job_id, type=MessageType.JOB_SUBMITTED, payload=data)
            await self.outbox.add(env)
        return job_id

    async def submit_sync(self, data: dict[str, Any]) -> dict[str, Any]:
        job_id = new_id()
        started = time.perf_counter()
        with correlation(job_id):
            v = await simulate_work(label="validate", duration_s=self.settings.validate_duration)
            p = await simulate_work(label="process", duration_s=self.settings.process_duration)
            f = await simulate_work(label="finalize", duration_s=self.settings.finalize_duration)
        return {
            "job_id": job_id,
            "status": "COMPLETED",
            "elapsed_s": round(time.perf_counter() - started, 4),
            "result": {"validate": v, "process": p, "finalize": f},
        }

    def _saga(self, job_id: str) -> Any:
        assert self.coordinator is not None
        return self.coordinator._sagas.get(job_id)

    def jobs(self, limit: int = 60) -> list[dict[str, Any]]:
        assert self.read_model is not None
        rows = self.read_model.all()[-limit:]
        out: list[dict[str, Any]] = []
        for job in reversed(rows):
            job_id = job["job_id"]
            saga = self._saga(job_id)
            out.append(
                {
                    "job_id": job_id,
                    "rm_status": job["status"],
                    "saga_status": saga.status.value if saga else None,
                    "completed_steps": list(saga.completed_steps) if saga else [],
                    "error": saga.error if saga else None,
                    "result": job.get("result"),
                }
            )
        return out

    def job_detail(self, job_id: str) -> dict[str, Any]:
        assert self.read_model is not None
        saga = self._saga(job_id)
        return {
            "job_id": job_id,
            "rm": self.read_model.get(job_id),
            "saga_status": saga.status.value if saga else None,
            "completed_steps": list(saga.completed_steps) if saga else [],
            "error": saga.error if saga else None,
            "events": [e for e in self.events if e["correlation_id"] == job_id],
        }

    def snapshot(self) -> dict[str, Any]:
        assert self.read_model is not None
        jobs = self.read_model.all()
        completed = sum(1 for j in jobs if j["status"] == "COMPLETED")
        failed = sum(1 for j in jobs if j["status"] == "FAILED")
        return {
            "uptime_s": round(time.time() - self.started_at, 1),
            "settings": {
                "process_fail_rate": self.settings.process_fail_rate,
                "validate_duration": self.settings.validate_duration,
                "process_duration": self.settings.process_duration,
                "finalize_duration": self.settings.finalize_duration,
                "max_attempts": self.settings.max_attempts,
            },
            "counters": {
                "published": self.counters["published"],
                "retries": self.counters["retries"],
                "dead_letters": self.counters["dead_letters"],
                "by_type": self.counters["by_type"],
                "jobs_total": len(jobs),
                "jobs_completed": completed,
                "jobs_failed": failed,
                "jobs_in_flight": len(jobs) - completed - failed,
            },
            "dead_letters": [
                {"type": e.type.value, "correlation_id": e.correlation_id, "attempt": e.attempt}
                for e in self.broker.dead_letters[-30:]
            ],
            "jobs": self.jobs(),
            "last_seq": self.seq,
            "crash_armed": dict(self.crash_remaining),
        }

    def set_config(self, body: ConfigBody) -> dict[str, Any]:
        if body.process_fail_rate is not None:
            self.settings.process_fail_rate = max(0.0, min(1.0, body.process_fail_rate))
        if body.process_duration is not None:
            self.settings.process_duration = max(0.0, min(5.0, body.process_duration))
        if body.validate_duration is not None:
            self.settings.validate_duration = max(0.0, min(5.0, body.validate_duration))
        if body.finalize_duration is not None:
            self.settings.finalize_duration = max(0.0, min(5.0, body.finalize_duration))
        return self.snapshot()["settings"]

    def reset(self) -> None:
        assert self.read_model is not None and self.coordinator is not None
        for by_group in self.broker._groups.values():
            for grp in by_group.values():
                while not grp.queue.empty():
                    try:
                        grp.queue.get_nowait()
                        grp.queue.task_done()
                    except Exception:
                        break
        for task in list(self.broker._bg):
            task.cancel()
        self.broker._bg.clear()
        self.outbox._rows.clear()
        self.read_model._jobs.clear()
        self.coordinator._sagas.clear()
        self.broker.dead_letters.clear()
        self.idem._seen.clear()
        self.events.clear()
        self.seq = 0
        self.counters = self._fresh_counters()
        for service in self.crash_remaining:
            self.crash_remaining[service] = 0

    async def _bench_path(
        self, path: str, concurrency: int, duration: float
    ) -> dict[str, Any]:
        import httpx

        deadline = time.perf_counter() + duration
        latencies: list[float] = []
        count = 0

        async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:

            async def worker() -> None:
                nonlocal count
                while time.perf_counter() < deadline and count < BENCH_MAX_REQUESTS:
                    started = time.perf_counter()
                    try:
                        await client.post(path, json={"data": {"bench": True}})
                    except Exception:
                        return
                    latencies.append((time.perf_counter() - started) * 1000.0)
                    count += 1

            await asyncio.gather(*[worker() for _ in range(concurrency)])

        ordered = sorted(latencies)
        mean = statistics.fmean(ordered) if ordered else 0.0
        p95 = ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))] if ordered else 0.0
        return {
            "requests": count,
            "throughput": round(count / duration, 1) if duration > 0 else 0.0,
            "latency_mean_ms": round(mean, 2),
            "latency_p95_ms": round(p95, 2),
        }

    async def bench(self, concurrency: int, duration: float) -> dict[str, Any]:
        concurrency = max(1, min(200, concurrency))
        duration = max(0.5, min(10.0, duration))
        async_result = await self._bench_path("/api/submit", concurrency, duration)
        await asyncio.sleep(0.25)
        sync_result = await self._bench_path("/api/submit_sync", concurrency, duration)
        thr = sync_result["throughput"]
        lat = async_result["latency_mean_ms"]
        return {
            "concurrency": concurrency,
            "duration": duration,
            "async": async_result,
            "sync": sync_result,
            "speedup_throughput": round(async_result["throughput"] / thr, 2) if thr else None,
            "speedup_latency": round(sync_result["latency_mean_ms"] / lat, 2) if lat else None,
        }

    async def level1_demo(self) -> dict[str, Any]:
        each = 0.2
        durations = [each] * 5

        async def task(seconds: float) -> None:
            await asyncio.sleep(seconds)

        started = time.perf_counter()
        for seconds in durations:
            await task(seconds)
        sequential = time.perf_counter() - started

        started = time.perf_counter()
        await asyncio.gather(*[task(seconds) for seconds in durations])
        concurrent = time.perf_counter() - started

        return {
            "tasks": len(durations),
            "each_s": each,
            "sequential_s": round(sequential, 3),
            "concurrent_s": round(concurrent, 3),
            "speedup": round(sequential / concurrent, 2) if concurrent else None,
        }


class SubmitBody(BaseModel):
    data: dict[str, Any] = Field(default_factory=dict)


class ConfigBody(BaseModel):
    process_fail_rate: float | None = None
    process_duration: float | None = None
    validate_duration: float | None = None
    finalize_duration: float | None = None


class BenchBody(BaseModel):
    concurrency: int = 20
    duration: float = 1.5


class CrashBody(BaseModel):
    service: str = "process"
    count: int = 2


stand = Stand()


@asynccontextmanager
async def lifespan(_: FastAPI) -> Any:
    await stand.start()
    yield
    await stand.stop()


app = FastAPI(title="УИР — асинхронные процессы: демонстрационный стенд", lifespan=lifespan)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(GUI_DIR / "index.html")


@app.get("/api/state")
async def api_state() -> JSONResponse:
    return JSONResponse(stand.snapshot())


@app.get("/api/events")
async def api_events(since: int = 0) -> JSONResponse:
    events = [e for e in stand.events if e["seq"] > since]
    return JSONResponse({"events": events[-200:], "last_seq": stand.seq})


@app.get("/api/job/{job_id}")
async def api_job(job_id: str) -> JSONResponse:
    return JSONResponse(stand.job_detail(job_id))


@app.post("/api/submit")
async def api_submit(body: SubmitBody) -> JSONResponse:
    job_id = await stand.submit_async(body.data)
    return JSONResponse({"job_id": job_id, "mode": "async", "status": "accepted"}, status_code=202)


@app.post("/api/submit_sync")
async def api_submit_sync(body: SubmitBody) -> JSONResponse:
    return JSONResponse(await stand.submit_sync(body.data))


@app.post("/api/config")
async def api_config(body: ConfigBody) -> JSONResponse:
    return JSONResponse(stand.set_config(body))


@app.post("/api/reset")
async def api_reset() -> JSONResponse:
    stand.reset()
    return JSONResponse({"ok": True})


@app.post("/api/bench")
async def api_bench(body: BenchBody) -> JSONResponse:
    return JSONResponse(await stand.bench(body.concurrency, body.duration))


@app.post("/api/crash")
async def api_crash(body: CrashBody) -> JSONResponse:
    stand.set_crash(body.service, body.count)
    return JSONResponse({"crash_armed": stand.crash_remaining})


@app.post("/api/level1")
async def api_level1() -> JSONResponse:
    return JSONResponse(await stand.level1_demo())


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def _open_browser() -> None:
    webbrowser.open(BASE_URL + "/")


def main() -> None:
    configure_logging("WARNING")
    print("=" * 64)
    print("  Демонстрационный стенд — асинхронные процессы в микросервисах")
    print(f"  Интерфейс:  {BASE_URL}/")
    print("  Браузер откроется автоматически. Остановить: Ctrl+C")
    print("=" * 64)
    if not os.environ.get("AMS_GUI_NO_BROWSER"):
        threading.Timer(1.3, _open_browser).start()
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
