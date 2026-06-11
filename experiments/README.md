# Экспериментальное исследование (sync vs async)

Сравнение синхронного (`POST /jobs/sync`) и асинхронного (`POST /jobs`) приёма заявок по
задержке ответа и пропускной способности под конкурентной нагрузкой.

## Подготовка

Поднимите стенд (любой режим):

```powershell
# Режим A (без Docker)
python examples/run_local.py
# или Режим B (Docker): docker compose -f deploy/docker-compose.yml up --build
```

## Вариант 1 — встроенный бенчмарк (httpx)

```powershell
python experiments/bench.py --base http://127.0.0.1:8000 --duration 10 --concurrency 50
```

Скрипт выводит таблицу: число запросов, ошибки, пропускная способность (rps),
средняя задержка и перцентили p50/p95/p99 — отдельно для `/jobs` и `/jobs/sync`.

## Вариант 2 — Locust

```powershell
pip install -e ".[load]"
locust -f experiments/load/locustfile.py --host http://127.0.0.1:8000
```

## Что фиксировать в работе

- Таблицу метрик при нескольких уровнях конкурентности (например, 10, 50, 100).
- Графики задержки и пропускной способности.
- Поведение при инъекции отказов: задайте `AMS_PROCESS_FAIL_RATE` (например, 1.0) и наблюдайте
  повторы с backoff и компенсацию саги (статус заявки → FAILED). Примечание: исчерпание попыток
  на шаге `process` публикует `StepFailed` → компенсацию, а не DLQ; в DLQ попадают сообщения,
  обработчик которых не порождает `StepFailed` (это проверяется тестом брокера). В режиме B
  очередь недоставленных — `ams.dead-letter` в консоли RabbitMQ http://localhost:15672.

Результаты складывайте в `experiments/results/`.
