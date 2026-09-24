"""Dense MVS + GPU infra/scheduler helpers (pyrx, light tier; lazy pycolmap)."""
from __future__ import annotations
import subprocess


def _run(args: list[str], timeout: int = 60) -> str:
    # list-form (no shell=True): args are hardcoded, no injection surface.
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return (r.stdout or r.stderr or "").strip()
    except Exception:  # noqa: BLE001
        return ""


def _parse_gpu_infra(*, gpu_query_csv: str, meminfo: str, nproc: str) -> dict:
    rows = [r for r in gpu_query_csv.splitlines() if r.strip()]
    vram = 0
    if rows:
        parts = [p.strip() for p in rows[0].split(",")]
        # "index, name, <total> MiB, <free> MiB"
        vram = int(parts[2].split()[0]) if len(parts) >= 3 else 0
    mem_total = mem_avail = 0
    for line in meminfo.splitlines():
        if line.startswith("MemTotal:"):
            mem_total = int(line.split()[1]) // 1024
        elif line.startswith("MemAvailable:"):
            mem_avail = int(line.split()[1]) // 1024
    try:
        cores = int(nproc.strip())
    except ValueError:
        cores = 0
    return {
        "gpu_count": len(rows),
        "per_gpu_vram_mb": vram,
        "host_ram_mb": mem_total,
        "host_ram_available_mb": mem_avail or mem_total,
        "cores": cores,
    }


def gpu_infra() -> dict:
    import re
    q = _run(["nvidia-smi", "--query-gpu=index,name,memory.total,memory.free", "--format=csv,noheader"])
    try:
        meminfo = open("/proc/meminfo").read()
    except OSError:
        meminfo = ""
    info = _parse_gpu_infra(gpu_query_csv=q, meminfo=meminfo, nproc=_run(["nproc"]))
    drv = _run(["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"])
    info["driver"] = drv.splitlines()[0] if drv else ""
    m = re.search(r"CUDA Version:\s*([0-9.]+)", _run(["nvidia-smi"]))
    info["cuda"] = m.group(1) if m else ""
    return info
