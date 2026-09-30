"""Dense MVS + GPU infra/scheduler helpers (pyrx, light tier; lazy pycolmap)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from databricks.labs.gbx.pyrx.checkpoint import checkpoint_skip, input_signature


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

    q = _run(
        [
            "nvidia-smi",
            "--query-gpu=index,name,memory.total,memory.free",
            "--format=csv,noheader",
        ]
    )
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


def recommend_dense_allocation(
    cluster_sizes,
    *,
    dense_max_image_size=None,
    infra=None,
    per_task_host_gb=32.0,
    reserve_host_gb=6.0,
):
    infra = infra or gpu_infra()
    n_gpu = int(infra.get("gpu_count", 0))
    if n_gpu < 1:
        raise RuntimeError(
            "recommend_dense_allocation: no GPU visible (dense MVS needs a GPU driver node)"
        )
    n_clusters = max(1, len(cluster_sizes))
    ram_avail_gb = (
        infra.get("host_ram_available_mb", infra.get("host_ram_mb", 0)) / 1024.0
    )
    ram_slots = (
        max(1, int(ram_avail_gb // per_task_host_gb)) if per_task_host_gb > 0 else n_gpu
    )
    concurrency = min(n_gpu, n_clusters, ram_slots)
    gpus_per_task = max(1, n_gpu // concurrency) if concurrency <= n_clusters else 1
    # only hand extra GPUs to tasks when there are fewer clusters than GPUs
    if n_clusters >= n_gpu:
        gpus_per_task = 1
        concurrency = min(n_gpu, ram_slots)
    slots = []
    gpu = 0
    for _ in range(concurrency):
        ids = [str(gpu + k) for k in range(gpus_per_task) if gpu + k < n_gpu]
        slots.append(",".join(ids))
        gpu += gpus_per_task
    # Host patch-match cache must fit node RAM ALONGSIDE the Spark/JVM heap, the Python
    # process, the loaded images, and GPU host-pinned buffers — the COLMAP default (32GB)
    # OOM-kills small GPU nodes with a SIGABRT (e.g. a 16GB g4dn/T4).
    # The usable-RAM reserve is computed by budget_for("dense_alloc") (a fixed 6 GiB
    # via budget.py _DENSE_RESERVE_MB); the result is split across concurrent tasks and
    # capped to [2, per_task_host_gb]. Note: the reserve_host_gb parameter now only
    # labels the reason string — it no longer governs cache_size_gb.
    from databricks.labs.gbx.pyrx.core import budget as _budget

    usable_gb = _budget.budget_for("dense_alloc", infra=infra).budget_bytes / (1024**3)
    cache_size_gb = round(
        max(2.0, min(per_task_host_gb, usable_gb / max(1, concurrency))), 1
    )
    reason = (
        f"{n_clusters} clusters, {n_gpu} GPUs, ~{ram_avail_gb:.0f}GB avail RAM "
        f"(reserve {reserve_host_gb:.0f}GB) -> {concurrency} concurrent x {gpus_per_task} "
        f"GPU(s), cache {cache_size_gb}GB/task"
    )
    return {
        "concurrency": concurrency,
        "gpus_per_task": gpus_per_task,
        "gpu_index_per_slot": slots,
        "cache_size_gb": cache_size_gb,
        "reason": reason,
    }


def dense_mvs_pool(cluster_specs, *, allocation=None, runner=None, max_retries=1):
    from databricks.labs.gbx.pyrx.core.gpu_pool import gpu_pool_map

    if runner is None:
        runner = lambda spec, gpu_index: dense_patch_match(  # noqa: E731
            spec["work_dir"],
            gpu_index=gpu_index,
            max_image_size=spec.get("max_image_size"),
            geom_consistency=spec.get("geom_consistency", True),
            num_iterations=spec.get("num_iterations"),
            window_step=spec.get("window_step"),
            cache_size_gb=spec.get("cache_size_gb"),
        )
    if allocation is None:
        allocation = recommend_dense_allocation(
            [s.get("n_images", 1) for s in cluster_specs]
        )
    slots = allocation["gpu_index_per_slot"]
    concurrency = max(1, allocation["concurrency"])

    def _run_one(spec, slot_id):
        # gpu_pool_map hands us a device-SLOT index (0..concurrency-1); resolve it to
        # this allocation's physical gpu index string (may be multi-GPU, e.g. "0,1,2,3").
        gi = slots[slot_id]
        # Retry + failure isolation happen HERE (not delegated to gpu_pool_map's own
        # max_retries) so a permanently-failing cluster never raises out of the pool and
        # cannot discard other clusters' results — matching dense_mvs_pool's pre-existing
        # per-cluster isolate-and-continue behavior exactly.
        attempt = 0
        while True:
            attempt += 1
            try:
                res = runner(spec, gi)
                return {"status": "ok", "result": res}
            except Exception as e:  # noqa: BLE001
                if attempt > max_retries:
                    return {"status": "error", "error": str(e)[:400]}

    outcomes = gpu_pool_map(cluster_specs, _run_one, gpus=concurrency, max_retries=0)
    return {
        spec["cluster_id"]: outcome for spec, outcome in zip(cluster_specs, outcomes)
    }


def _resolve_model_dir(sparse_dir):
    """Pick the COLMAP model dir: sparse_dir itself if it holds cameras.bin/.txt,
    else the largest reconstruction among numbered subdirs (sparse/0, sparse/1, ...),
    where 'largest' means greatest images.bin size — COLMAP may emit several models
    and we want the biggest reconstruction, not the highest-numbered dir."""
    sp = Path(sparse_dir)
    if (sp / "cameras.bin").exists() or (sp / "cameras.txt").exists():
        return sp
    cands = [
        d
        for d in sorted(sp.iterdir())
        if d.is_dir() and ((d / "cameras.bin").exists() or (d / "cameras.txt").exists())
    ]
    if not cands:
        raise RuntimeError(f"_resolve_model_dir: no COLMAP model under {sparse_dir}")
    return max(
        cands,
        key=lambda d: (
            (d / "images.bin").stat().st_size if (d / "images.bin").exists() else 0
        ),
    )


def dense_undistort(sparse_dir, image_dir, work_dir, *, num_src_images=None):
    """CPU: undistort a cluster's images against its sparse model into a dense workspace.

    num_src_images caps how many source images each reference is patch-matched against
    (COLMAP num_patch_match_src_images; default = all). A small cap (e.g. 2-4) makes
    patch-match dramatically faster with modest quality cost - the main dev-speed lever.

    Do NOT subset the images here to shrink the workload: undistorting only a subset
    leaves patch_match referencing the cluster's other images as covisibility sources
    with no undistorted map ("Missing image or map dependency" -> kernel death). The
    dev-size cap belongs upstream at the SPARSE stage (nb1a's DEV_MAX_IMAGES builds a
    small self-consistent model); this undistorts the whole loaded model.
    """
    import pycolmap

    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    model_dir = _resolve_model_dir(sparse_dir)
    _kw = {}
    if num_src_images:
        _kw["num_patch_match_src_images"] = int(num_src_images)
    pycolmap.undistort_images(
        output_path=str(work),
        input_path=str(model_dir),
        image_path=str(image_dir),
        **_kw,
    )
    return str(work)


def dense_patch_match(
    work_dir,
    *,
    gpu_index="-1",
    max_image_size=None,
    geom_consistency=True,
    num_iterations=None,
    window_step=None,
    cache_size_gb=None,
):
    """GPU: patch-match stereo on a prepared dense workspace. gpu_index pins the GPU(s).

    Speed knobs (quality tradeoff): geom_consistency=False skips the second (geometric)
    pass (~2x faster); lower num_iterations / higher window_step cut the sweep cost.
    With dense_undistort(num_src_images=...) + a small max_image_size these make a dev
    reconstruction fast.

    cache_size_gb caps COLMAP's host-side patch-match cache (PatchMatchOptions.cache_size,
    GB). It MUST stay below the node's host RAM: the default is 32 GB, which OOM-kills the
    Python process (SIGABRT) on small GPU nodes (e.g. a 16 GB g4dn). recommend_dense_allocation
    derives a RAM-aware value; pass it through so patch_match never over-allocates.
    """
    import pycolmap

    pm = pycolmap.PatchMatchOptions()
    pm.gpu_index = str(gpu_index)
    if max_image_size:
        pm.max_image_size = int(max_image_size)
    pm.geom_consistency = bool(geom_consistency)
    if num_iterations:
        pm.num_iterations = int(num_iterations)
    if window_step:
        pm.window_step = int(window_step)
    if cache_size_gb:
        pm.cache_size = float(cache_size_gb)
    pycolmap.patch_match_stereo(str(work_dir), options=pm)
    return str(work_dir)


def dense_fuse(work_dir, out_ply, *, geom_consistency=True):
    """CPU-side: fuse depth maps into a colored dense point cloud (binary PLY).

    ``stereo_fusion`` defaults to fusing GEOMETRIC depth maps, but patch_match only
    writes geometric maps when it ran with geom_consistency=True. If patch_match ran
    with geom_consistency=False (photometric maps only), fusing "geometric" finds no
    maps and silently emits an EMPTY cloud (a header-only PLY). So input_type MUST
    match the patch_match pass: pass the SAME geom_consistency used for
    ``dense_patch_match`` here.
    """
    import pycolmap

    pycolmap.stereo_fusion(
        output_path=str(out_ply),
        workspace_path=str(work_dir),
        input_type="geometric" if geom_consistency else "photometric",
        output_type="PLY",
    )
    if not Path(out_ply).exists():
        raise RuntimeError(f"dense_fuse: stereo_fusion produced no PLY at {out_ply}")
    return str(out_ply)


def _dense_sig(
    cid,
    sparse_dir,
    *,
    max_image_size,
    src_images,
    geom_consistency,
    num_iterations,
    window_step,
):
    """Checkpoint signature for a cluster's dense output: sparse-model identity +
    the dense reconstruction knobs. Mirrors nb1b's (now-removed) local ``_dense_sig``.
    """
    bin_path = Path(sparse_dir) / "images.bin"
    bin_size = bin_path.stat().st_size if bin_path.exists() else 0
    return input_signature(
        {"cid": str(cid), "sparse_bin_size": bin_size},
        {
            "max_image_size": max_image_size,
            "src_images": src_images,
            "geom_consistency": geom_consistency,
            "iters": num_iterations,
            "window_step": window_step,
        },
    )


def dense_reconstruct_clusters(
    cluster_models,
    image_dir,
    *,
    work_root,
    ply_root=None,
    checkpoint=None,
    force=False,
    max_image_size=1600,
    src_images=None,
    geom_consistency=False,
    num_iterations=None,
    window_step=None,
    allocation=None,
    on_event=None,
):
    """Reconstruct dense point clouds for a set of clusters (undistort -> allocate
    -> patch_match -> fuse), with optional Volume-backed checkpointing.

    Mirrors nb1b cell 127b9903's per-cluster dense loop, collapsed into one call.
    ``cluster_models`` is ``{cid: (sparse_dir, gps_json)}`` (``gps_json`` is unused
    here; kept for symmetry with ``ortho.*``). All GPU/pycolmap work stays behind
    the lazy imports inside ``dense_undistort``/``dense_mvs_pool``/``dense_fuse``,
    so this orchestration itself needs no GPU.

    ``checkpoint=None`` disables skip/resume (always recomputes). ``ply_root=None``
    keeps fused plys local under ``work_root`` (no Volume copy). A fused ply is
    copied to ``ply_root`` and marked done only when BOTH ``ply_root`` and
    ``checkpoint`` are given. ``force=True`` bypasses the checkpoint (recomputes
    even a checkpointed cluster). ``geom_consistency`` threads to BOTH the
    patch_match spec and ``dense_fuse`` (the ``193ded76`` fix — a miss here
    reintroduces the empty-PLY bug). Clusters whose patch_match fails (pool status
    != "ok") are dropped from the result; failures and skips are reported via
    ``on_event(str)`` when given (a no-op otherwise).

    Returns ``{cid: fused.ply path}``, including checkpointed clusters.
    """
    emit = on_event if on_event is not None else lambda *_a, **_k: None

    done_plys = {}
    todo = []
    for cid, model in cluster_models.items():
        sparse_dir = model[0]
        work_dir = f"{work_root}/dense_c{cid}"
        ply_vol = f"{ply_root}/cluster_{cid}/fused.ply" if ply_root else None
        sig = _dense_sig(
            cid,
            sparse_dir,
            max_image_size=max_image_size,
            src_images=src_images,
            geom_consistency=geom_consistency,
            num_iterations=num_iterations,
            window_step=window_step,
        )
        if checkpoint is not None and checkpoint_skip(
            checkpoint, "dense", cid, sig, force=force
        ):
            emit(f"[dense][skip] cluster {cid} — checkpointed ({ply_vol})")
            done_plys[cid] = ply_vol if ply_vol else f"{work_dir}/fused.ply"
            continue
        # Warm cluster: /tmp persists across runs; clear a stale dense workspace
        # or patch_match can hit a resolution/dependency mismatch.
        shutil.rmtree(work_dir, ignore_errors=True)
        dense_undistort(sparse_dir, image_dir, work_dir, num_src_images=src_images)
        todo.append(
            {
                "cluster_id": cid,
                "work_dir": work_dir,
                "ply_vol": ply_vol,
                "sig": sig,
                "max_image_size": max_image_size,
                "geom_consistency": geom_consistency,
                "num_iterations": num_iterations,
                "window_step": window_step,
            }
        )

    pool = {}
    if todo:
        alloc = allocation
        if alloc is None:
            alloc = recommend_dense_allocation(
                [1] * len(todo), dense_max_image_size=max_image_size
            )
        for spec in todo:
            spec["cache_size_gb"] = alloc.get("cache_size_gb")
        pool = dense_mvs_pool(todo, allocation=alloc)

    plys = dict(done_plys)
    for spec in todo:
        cid = spec["cluster_id"]
        status = pool.get(cid, {})
        if status.get("status") != "ok":
            emit(
                f"  [DROP] cluster {cid} patch_match failed: "
                f"{str(status.get('error', ''))[:160]}"
            )
            continue
        local_ply = dense_fuse(
            spec["work_dir"],
            f'{spec["work_dir"]}/fused.ply',
            geom_consistency=geom_consistency,
        )
        out_ply = local_ply
        if spec["ply_vol"] and checkpoint is not None:
            Path(spec["ply_vol"]).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(local_ply, spec["ply_vol"])
            checkpoint.mark_done("dense", cid, spec["sig"], spec["ply_vol"])
            out_ply = spec["ply_vol"]
        plys[cid] = out_ply

    return plys
