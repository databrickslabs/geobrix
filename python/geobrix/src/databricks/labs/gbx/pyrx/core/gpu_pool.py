"""Model-agnostic GPU pool scheduler: run a callback over units across N device slots."""

from concurrent.futures import ThreadPoolExecutor
from queue import Queue
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
    # Device-id queue seeded with 0..gpus-1: a unit only starts once a device is free,
    # and holds it exclusively (across all of its retry attempts) until it finishes —
    # this guarantees at most one unit ever runs on a given device at a time, even under
    # uneven per-unit durations. A static `i % gpus` assignment does NOT guarantee that:
    # a fast unit can free its worker before a slow same-device unit finishes, letting
    # the freed worker pick up a later unit pre-assigned to that still-busy device.
    devices: Queue = Queue()
    for dev in range(gpus):
        devices.put(dev)

    def _run(unit):
        dev = devices.get()
        try:
            return _attempt(run_one, unit, dev, max_retries)
        finally:
            devices.put(dev)

    with ThreadPoolExecutor(max_workers=gpus) as ex:
        futs = {ex.submit(_run, u): i for i, u in enumerate(units)}
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
