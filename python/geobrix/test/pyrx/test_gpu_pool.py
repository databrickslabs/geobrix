import threading
import time

from databricks.labs.gbx.pyrx.core.gpu_pool import gpu_pool_map


def test_pool_preserves_order_and_passes_device_ids():
    seen = []

    def run_one(unit, gpu_id):
        seen.append((unit, gpu_id))
        return unit * 10

    out = gpu_pool_map([1, 2, 3, 4], run_one, gpus=2)
    assert out == [10, 20, 30, 40]  # order preserved
    assert {g for _, g in seen} <= {0, 1}  # only device ids 0..gpus-1


def test_pool_retries_then_succeeds():
    calls = {"n": 0}

    def flaky(unit, gpu_id):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient")
        return unit

    assert gpu_pool_map([7], flaky, gpus=1, max_retries=1) == [7]


def test_single_gpu_runs_serial_on_device_0():
    devs = []
    gpu_pool_map([1, 2], lambda u, g: devs.append(g), gpus=1)
    assert devs == [0, 0]


def test_pool_enforces_device_exclusivity_under_uneven_durations():
    """Pins the exclusivity contract: a static `i % gpus` assignment lets a fast
    unit's freed worker pick up a later unit pre-assigned to a still-busy device,
    overlapping two units on the same device id. Even-indexed units sleep much
    longer than odd-indexed ones, which reliably reproduces that overlap under the
    old static-assignment scheduler (device 0 gets units 0, 2, 4 - all slow; the
    worker freed by a fast odd unit grabs the next slow unit while the first slow
    unit is still running on device 0). The device-queue scheduler must not overlap.
    """
    lock = threading.Lock()
    intervals = []  # (dev, enter_ts, exit_ts)

    def run_one(unit, dev):
        enter = time.monotonic()
        time.sleep(0.05 if unit % 2 == 0 else 0.01)
        exit_ = time.monotonic()
        with lock:
            intervals.append((dev, enter, exit_))
        return unit

    units = list(range(6))
    out = gpu_pool_map(units, run_one, gpus=2)

    assert out == units  # order preserved

    by_device = {}
    for dev, enter, exit_ in intervals:
        by_device.setdefault(dev, []).append((enter, exit_))
    for dev, spans in by_device.items():
        spans.sort()
        for (prev_enter, prev_exit), (next_enter, next_exit) in zip(spans, spans[1:]):
            assert next_enter >= prev_exit, (
                f"device {dev} ran overlapping units: "
                f"{(prev_enter, prev_exit)} vs {(next_enter, next_exit)}"
            )
