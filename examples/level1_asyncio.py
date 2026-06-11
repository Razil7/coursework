from __future__ import annotations

import asyncio
import sys
import time

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
except (AttributeError, ValueError):
    pass


async def io_task(name: str, seconds: float) -> str:
    print(f"  [{time.perf_counter() - T0:5.2f}s] {name}: начало")
    await asyncio.sleep(seconds)
    print(f"  [{time.perf_counter() - T0:5.2f}s] {name}: конец")
    return f"{name}-результат"


async def sequential() -> None:
    start = time.perf_counter()
    await io_task("A", 1.0)
    await io_task("B", 1.0)
    await io_task("C", 1.0)
    print(f"Последовательно: {time.perf_counter() - start:.2f}s (≈ сумма)\n")


async def concurrent() -> None:
    start = time.perf_counter()
    results = await asyncio.gather(
        io_task("A", 1.0), io_task("B", 1.0), io_task("C", 1.0)
    )
    print(f"Конкурентно:    {time.perf_counter() - start:.2f}s (≈ максимум), {results}\n")


async def future_demo() -> None:
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[str] = loop.create_future()

    async def resolver() -> None:
        await asyncio.sleep(0.5)
        fut.set_result("значение, доставленное позже")

    task = asyncio.create_task(resolver())
    print(f"Future: ожидаем результат... -> {await fut}\n")
    await task


async def main() -> None:
    global T0
    T0 = time.perf_counter()
    print("== 1. Последовательное выполнение ==")
    await sequential()
    print("== 2. Конкурентное выполнение (один поток, цикл событий) ==")
    await concurrent()
    print("== 3. Будущий результат (Future) ==")
    await future_demo()


if __name__ == "__main__":
    T0 = time.perf_counter()
    asyncio.run(main())
