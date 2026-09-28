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
