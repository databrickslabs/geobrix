from databricks.labs.gbx.pyrx.mvs import _parse_gpu_infra


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
