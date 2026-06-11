from __future__ import annotations

import asyncio
import random
from typing import Any


class StepError(RuntimeError):
    ...


async def simulate_work(
    *,
    label: str,
    duration_s: float,
    fail_rate: float = 0.0,
    rng: random.Random | None = None,
) -> dict[str, Any]:
    await asyncio.sleep(duration_s)
    roll = (rng or random).random()
    if roll < fail_rate:
        raise StepError(f"шаг {label!r} завершился сбоем (roll={roll:.3f} < fail_rate={fail_rate})")
    return {"label": label, "duration_s": duration_s}
