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
