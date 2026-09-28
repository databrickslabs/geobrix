"""Model-agnostic GPU pool scheduler: run a callback over units across N device slots."""

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, List, TypeVar

T = TypeVar("T")


def gpu_pool_map(
    units: List[Any],
    run_one: Callable[[Any, int], T],
    *,
    gpus: int,
    max_retries: int = 1,
) -> List[T]:
    if gpus <= 1:
        return [_attempt(run_one, u, 0, max_retries) for u in units]
    results: List[Any] = [None] * len(units)
    # round-robin device assignment; ThreadPoolExecutor bounded to `gpus`
    with ThreadPoolExecutor(max_workers=gpus) as ex:
        futs = {
            ex.submit(_attempt, run_one, u, i % gpus, max_retries): i
            for i, u in enumerate(units)
        }
        for fut in futs:
            results[futs[fut]] = fut.result()
    return results


def _attempt(run_one, unit, gpu_id, max_retries):
    last = None
    for _ in range(max_retries + 1):
        try:
            return run_one(unit, gpu_id)
        except Exception as exc:  # noqa: BLE001 - retry any transient GPU error
            last = exc
    raise last
