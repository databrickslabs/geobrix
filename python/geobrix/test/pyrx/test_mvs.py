from databricks.labs.gbx.pyrx.mvs import _parse_gpu_infra, recommend_dense_allocation


def test_parse_gpu_infra_8xh100():
    q = "\n".join(["0, NVIDIA H100 80GB HBM3, 81559 MiB, 81079 MiB"] * 8)
    meminfo = "MemTotal:       2097063168 kB\nMemAvailable:   2018981720 kB\n"
    info = _parse_gpu_infra(gpu_query_csv=q, meminfo=meminfo, nproc="192")
    assert info["gpu_count"] == 8
    assert info["per_gpu_vram_mb"] == 81559
    assert info["host_ram_mb"] == 2097063168 // 1024
    assert info["cores"] == 192


def test_parse_gpu_infra_no_gpu():
    info = _parse_gpu_infra(gpu_query_csv="", meminfo="MemTotal: 1048576 kB\n", nproc="4")
    assert info["gpu_count"] == 0
    assert info["cores"] == 4


# --- recommend_dense_allocation tests ---

_8XH100 = {"gpu_count": 8, "per_gpu_vram_mb": 81559, "host_ram_mb": 2097063,
           "host_ram_available_mb": 2018981, "cores": 192}


def test_alloc_many_clusters_one_gpu_each():
    a = recommend_dense_allocation([50]*20, infra=_8XH100)
    assert a["concurrency"] == 8
    assert a["gpus_per_task"] == 1
    assert a["gpu_index_per_slot"] == ["0", "1", "2", "3", "4", "5", "6", "7"]


def test_alloc_few_clusters_multi_gpu_each():
    a = recommend_dense_allocation([50, 50], infra=_8XH100)
    assert a["concurrency"] == 2
    assert a["gpus_per_task"] == 4
    assert a["gpu_index_per_slot"] == ["0,1,2,3", "4,5,6,7"]


def test_alloc_single_gpu_trivial():
    a = recommend_dense_allocation([50]*5, infra={"gpu_count": 1, "per_gpu_vram_mb": 24000,
                                                  "host_ram_mb": 200000, "host_ram_available_mb": 190000, "cores": 48})
    assert a["concurrency"] == 1
    assert a["gpu_index_per_slot"] == ["0"]


def test_alloc_ram_bound():
    # 8 GPUs but only ~100GB RAM at 32GB/task -> 3 concurrent
    infra = {"gpu_count": 8, "per_gpu_vram_mb": 81559, "host_ram_mb": 100000,
             "host_ram_available_mb": 100000, "cores": 192}
    a = recommend_dense_allocation([50]*10, infra=infra)
    assert a["concurrency"] == 3


def test_alloc_no_gpu_raises():
    import pytest
    with pytest.raises(RuntimeError):
        recommend_dense_allocation([50], infra={"gpu_count": 0, "host_ram_mb": 1000,
                                                "host_ram_available_mb": 1000, "cores": 4, "per_gpu_vram_mb": 0})


# --- dense_mvs_pool tests ---

import threading
from databricks.labs.gbx.pyrx.mvs import dense_mvs_pool


def test_pool_runs_all_clusters_on_slots():
    specs = [{"cluster_id": i} for i in range(20)]
    alloc = {"concurrency": 8, "gpus_per_task": 1,
             "gpu_index_per_slot": [str(i) for i in range(8)], "cache_size_gb": 32.0}
    seen_gpu = {}
    lock = threading.Lock()
    def runner(spec, gpu_index):
        with lock:
            seen_gpu[spec["cluster_id"]] = gpu_index
        return f"/tmp/{spec['cluster_id']}.ply"
    out = dense_mvs_pool(specs, allocation=alloc, runner=runner)
    assert len(out) == 20
    assert all(v["status"] == "ok" for v in out.values())
    assert set(seen_gpu.values()) <= {str(i) for i in range(8)}  # only assigned slots used


def test_pool_isolates_failures_and_retries():
    calls = {"c1": 0}
    def runner(spec, gpu_index):
        if spec["cluster_id"] == "c1":
            calls["c1"] += 1
            raise RuntimeError("boom")
        return "ok.ply"
    specs = [{"cluster_id": "c0"}, {"cluster_id": "c1"}, {"cluster_id": "c2"}]
    alloc = {"concurrency": 2, "gpus_per_task": 1, "gpu_index_per_slot": ["0", "1"], "cache_size_gb": 32.0}
    out = dense_mvs_pool(specs, allocation=alloc, runner=runner, max_retries=1)
    assert out["c0"]["status"] == "ok" and out["c2"]["status"] == "ok"
    assert out["c1"]["status"] == "error"
    assert calls["c1"] == 2  # initial + 1 retry


# --- _resolve_model_dir tests ---

from pathlib import Path
from databricks.labs.gbx.pyrx.mvs import _resolve_model_dir


def test_resolve_model_dir_numbered(tmp_path):
    (tmp_path / "0").mkdir()
    (tmp_path / "0" / "cameras.bin").write_bytes(b"x")
    (tmp_path / "0" / "images.bin").write_bytes(b"y" * 10)
    assert _resolve_model_dir(tmp_path) == tmp_path / "0"


def test_resolve_model_dir_flat(tmp_path):
    (tmp_path / "cameras.bin").write_bytes(b"x")
    assert _resolve_model_dir(tmp_path) == tmp_path


def test_pyrx_exports_mvs():
    from databricks.labs.gbx import pyrx
    for n in ["gpu_infra", "recommend_dense_allocation", "dense_mvs_pool",
              "dense_undistort", "dense_patch_match", "dense_fuse"]:
        assert hasattr(pyrx, n), f"pyrx missing export: {n}"
