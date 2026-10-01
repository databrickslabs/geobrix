import threading
from pathlib import Path

import pytest

from databricks.labs.gbx.pyrx.mvs import (
    _parse_gpu_infra,
    _resolve_model_dir,
    dense_mvs_pool,
    recommend_dense_allocation,
)


def test_parse_gpu_infra_8xh100():
    q = "\n".join(["0, NVIDIA H100 80GB HBM3, 81559 MiB, 81079 MiB"] * 8)
    meminfo = "MemTotal:       2097063168 kB\nMemAvailable:   2018981720 kB\n"
    info = _parse_gpu_infra(gpu_query_csv=q, meminfo=meminfo, nproc="192")
    assert info["gpu_count"] == 8
    assert info["per_gpu_vram_mb"] == 81559
    assert info["host_ram_mb"] == 2097063168 // 1024
    assert info["cores"] == 192


def test_parse_gpu_infra_no_gpu():
    info = _parse_gpu_infra(
        gpu_query_csv="", meminfo="MemTotal: 1048576 kB\n", nproc="4"
    )
    assert info["gpu_count"] == 0
    assert info["cores"] == 4


# --- recommend_dense_allocation tests ---

_8XH100 = {
    "gpu_count": 8,
    "per_gpu_vram_mb": 81559,
    "host_ram_mb": 2097063,
    "host_ram_available_mb": 2018981,
    "cores": 192,
}


def test_alloc_many_clusters_one_gpu_each():
    a = recommend_dense_allocation([50] * 20, infra=_8XH100)
    assert a["concurrency"] == 8
    assert a["gpus_per_task"] == 1
    assert a["gpu_index_per_slot"] == ["0", "1", "2", "3", "4", "5", "6", "7"]


def test_alloc_few_clusters_multi_gpu_each():
    a = recommend_dense_allocation([50, 50], infra=_8XH100)
    assert a["concurrency"] == 2
    assert a["gpus_per_task"] == 4
    assert a["gpu_index_per_slot"] == ["0,1,2,3", "4,5,6,7"]


def test_alloc_single_gpu_trivial():
    a = recommend_dense_allocation(
        [50] * 5,
        infra={
            "gpu_count": 1,
            "per_gpu_vram_mb": 24000,
            "host_ram_mb": 200000,
            "host_ram_available_mb": 190000,
            "cores": 48,
        },
    )
    assert a["concurrency"] == 1
    assert a["gpu_index_per_slot"] == ["0"]


def test_alloc_ram_bound():
    # 8 GPUs but only ~100GB RAM at 32GB/task -> 3 concurrent
    infra = {
        "gpu_count": 8,
        "per_gpu_vram_mb": 81559,
        "host_ram_mb": 100000,
        "host_ram_available_mb": 100000,
        "cores": 192,
    }
    a = recommend_dense_allocation([50] * 10, infra=infra)
    assert a["concurrency"] == 3


def test_alloc_no_gpu_raises():
    with pytest.raises(RuntimeError):
        recommend_dense_allocation(
            [50],
            infra={
                "gpu_count": 0,
                "host_ram_mb": 1000,
                "host_ram_available_mb": 1000,
                "cores": 4,
                "per_gpu_vram_mb": 0,
            },
        )


def test_recommend_dense_allocation_routes_budget_for(monkeypatch):
    """Fail-on-revert + behavior-preserving wiring test.

    Fail-on-revert: asserts recommend_dense_allocation routes its RAM budget
    through budget_for('dense_alloc'). Fails if reverted to direct gpu_infra().

    Behavior-preserving: on the _8XH100 representative snapshot with 20 clusters,
    the post-migration output matches the pre-migration formula exactly.
    """
    import databricks.labs.gbx.pyrx.core.budget as _budget_mod
    from databricks.labs.gbx.pyrx.mvs import recommend_dense_allocation

    # Pre-migration formula (independent computation, pinning expected values):
    # ram_avail_gb = 2018981 / 1024.0 = 1971.665...
    # reserve_host_gb = 6.0 (default)
    # usable_gb = max(0.0, 1971.665 - 6.0) = 1965.665...
    # ram_slots = max(1, int(1971.665 // 32.0)) = 61
    # concurrency = min(8 GPUs, 20 clusters, 61 ram_slots) = 8
    # cache_size_gb = round(max(2.0, min(32.0, 1965.665 / 8)), 1) = round(32.0, 1) = 32.0
    _EXPECTED_CONCURRENCY = 8
    _EXPECTED_CACHE_GB = 32.0

    # Spy: records all budget_for intent calls without changing behavior.
    calls = []
    orig = _budget_mod.budget_for

    def _spy(intent, *args, **kwargs):
        calls.append(intent)
        return orig(intent, *args, **kwargs)

    monkeypatch.setattr(_budget_mod, "budget_for", _spy)

    result = recommend_dense_allocation([50] * 20, infra=_8XH100)

    assert "dense_alloc" in calls, (
        "recommend_dense_allocation must route through budget_for('dense_alloc'); "
        "revert to direct gpu_infra() detected"
    )
    assert result["concurrency"] == _EXPECTED_CONCURRENCY
    assert (
        result["cache_size_gb"] == _EXPECTED_CACHE_GB
    ), f"behavior not preserved: got {result['cache_size_gb']}, expected {_EXPECTED_CACHE_GB}"


# --- dense_mvs_pool tests ---


def test_pool_runs_all_clusters_on_slots():
    specs = [{"cluster_id": i} for i in range(20)]
    alloc = {
        "concurrency": 8,
        "gpus_per_task": 1,
        "gpu_index_per_slot": [str(i) for i in range(8)],
        "cache_size_gb": 32.0,
    }
    seen_gpu = {}
    lock = threading.Lock()

    def runner(spec, gpu_index):
        with lock:
            seen_gpu[spec["cluster_id"]] = gpu_index
        return f"/tmp/{spec['cluster_id']}.ply"

    out = dense_mvs_pool(specs, allocation=alloc, runner=runner)
    assert len(out) == 20
    assert all(v["status"] == "ok" for v in out.values())
    assert set(seen_gpu.values()) <= {
        str(i) for i in range(8)
    }  # only assigned slots used


def test_pool_isolates_failures_and_retries():
    calls = {"c1": 0}

    def runner(spec, gpu_index):
        if spec["cluster_id"] == "c1":
            calls["c1"] += 1
            raise RuntimeError("boom")
        return "ok.ply"

    specs = [{"cluster_id": "c0"}, {"cluster_id": "c1"}, {"cluster_id": "c2"}]
    alloc = {
        "concurrency": 2,
        "gpus_per_task": 1,
        "gpu_index_per_slot": ["0", "1"],
        "cache_size_gb": 32.0,
    }
    out = dense_mvs_pool(specs, allocation=alloc, runner=runner, max_retries=1)
    assert out["c0"]["status"] == "ok" and out["c2"]["status"] == "ok"
    assert out["c1"]["status"] == "error"
    assert calls["c1"] == 2  # initial + 1 retry


# --- dense_fuse tests ---


def _fake_pycolmap(recorder):
    """A stand-in pycolmap whose stereo_fusion records its input_type and writes a
    (non-empty) PLY so dense_fuse's existence check passes. pycolmap is GPU-only and
    absent from the light test venv, so dense_fuse's lazy import is satisfied here."""
    import sys
    import types

    m = types.ModuleType("pycolmap")

    def stereo_fusion(*, output_path, workspace_path, input_type, output_type):
        recorder["input_type"] = input_type
        with open(output_path, "wb") as f:
            f.write(b"ply\n")  # any bytes so Path(out_ply).exists() is True

    m.stereo_fusion = stereo_fusion
    sys.modules["pycolmap"] = m
    return m


@pytest.mark.parametrize(
    "geom_consistency,expected",
    [(True, "geometric"), (False, "photometric")],
)
def test_dense_fuse_input_type_matches_geom_consistency(
    tmp_path, monkeypatch, geom_consistency, expected
):
    """REGRESSION GUARD: stereo_fusion defaults input_type='geometric', but
    patch_match only writes geometric maps when geom_consistency=True. Fusing
    'geometric' after a photometric-only (geom_consistency=False) pass finds no maps
    and silently produces an EMPTY cloud. dense_fuse MUST select input_type to match
    the patch_match pass, or the fast (geom_consistency=False) path yields 0 points."""
    from databricks.labs.gbx.pyrx import mvs

    rec = {}
    _fake_pycolmap(rec)
    out = tmp_path / "fused.ply"
    mvs.dense_fuse(str(tmp_path), str(out), geom_consistency=geom_consistency)
    assert rec["input_type"] == expected
    assert out.exists()


def test_dense_fuse_defaults_geometric(tmp_path):
    """Default (no arg) fuses geometric — the caller wiring passes the same
    geom_consistency it gave dense_patch_match."""
    from databricks.labs.gbx.pyrx import mvs

    rec = {}
    _fake_pycolmap(rec)
    mvs.dense_fuse(str(tmp_path), str(tmp_path / "f.ply"))
    assert rec["input_type"] == "geometric"


# --- _resolve_model_dir tests ---


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

    for n in [
        "gpu_infra",
        "recommend_dense_allocation",
        "dense_mvs_pool",
        "dense_undistort",
        "dense_patch_match",
        "dense_fuse",
        "dense_reconstruct_clusters",
    ]:
        assert hasattr(pyrx, n), f"pyrx missing export: {n}"


# --- dense_reconstruct_clusters tests ---


def _dense_sig_args(**overrides):
    args = dict(
        max_image_size=1600,
        src_images=None,
        geom_consistency=False,
        num_iterations=None,
        window_step=None,
    )
    args.update(overrides)
    return args


def test_reconstruct_skips_checkpointed(tmp_path, monkeypatch):
    """A cluster already checkpointed (matching sig + a persisted, existing ply)
    is skipped WITHOUT calling dense_undistort/dense_mvs_pool for it."""
    from databricks.labs.gbx.pyrx import mvs
    from databricks.labs.gbx.pyrx.checkpoint import Manifest

    sparse_dir = tmp_path / "sparse"
    sparse_dir.mkdir()
    (sparse_dir / "images.bin").write_bytes(b"x" * 42)

    ply_root = tmp_path / "plys"
    persisted = ply_root / "cluster_0" / "fused.ply"
    persisted.parent.mkdir(parents=True)
    persisted.write_bytes(b"ply")

    mani = Manifest(str(tmp_path / "manifest.json"))
    sig = mvs._dense_sig(0, str(sparse_dir), **_dense_sig_args())
    mani.mark_done("dense", 0, sig, str(persisted))

    calls = {"undistort": 0, "pool": 0}
    monkeypatch.setattr(
        mvs,
        "dense_undistort",
        lambda *a, **k: calls.__setitem__("undistort", calls["undistort"] + 1),
    )
    monkeypatch.setattr(
        mvs,
        "dense_mvs_pool",
        lambda *a, **k: (calls.__setitem__("pool", calls["pool"] + 1), {})[1],
    )

    result = mvs.dense_reconstruct_clusters(
        {0: (str(sparse_dir), "gps.json")},
        str(tmp_path / "images"),
        work_root=str(tmp_path / "work"),
        ply_root=str(ply_root),
        checkpoint=mani,
    )

    assert result == {0: str(persisted)}
    assert calls["undistort"] == 0
    assert calls["pool"] == 0


def test_reconstruct_recomputes_and_persists(tmp_path, monkeypatch):
    """No checkpoint entry -> undistort/pool/fuse run; ply is copied under
    ply_root and the checkpoint records it as done."""
    from databricks.labs.gbx.pyrx import mvs
    from databricks.labs.gbx.pyrx.checkpoint import Manifest

    sparse_dir = tmp_path / "sparse"
    sparse_dir.mkdir()
    (sparse_dir / "images.bin").write_bytes(b"x" * 10)

    calls = {"undistort": [], "fuse": []}

    def fake_undistort(sparse, image_dir, work_dir, *, num_src_images=None):
        calls["undistort"].append((sparse, image_dir, work_dir, num_src_images))
        Path(work_dir).mkdir(parents=True, exist_ok=True)
        return work_dir

    def fake_pool(specs, *, allocation=None, runner=None, max_retries=1):
        return {s["cluster_id"]: {"status": "ok"} for s in specs}

    def fake_fuse(work_dir, out_ply, *, geom_consistency=True):
        calls["fuse"].append((work_dir, out_ply, geom_consistency))
        Path(out_ply).write_bytes(b"ply")
        return out_ply

    monkeypatch.setattr(mvs, "dense_undistort", fake_undistort)
    monkeypatch.setattr(mvs, "dense_mvs_pool", fake_pool)
    monkeypatch.setattr(mvs, "dense_fuse", fake_fuse)
    monkeypatch.setattr(
        mvs, "recommend_dense_allocation", lambda *a, **k: {"cache_size_gb": 8.0}
    )

    ply_root = tmp_path / "plys"
    mani = Manifest(str(tmp_path / "manifest.json"))

    result = mvs.dense_reconstruct_clusters(
        {0: (str(sparse_dir), "gps.json")},
        str(tmp_path / "images"),
        work_root=str(tmp_path / "work"),
        ply_root=str(ply_root),
        checkpoint=mani,
    )

    assert len(calls["undistort"]) == 1
    assert len(calls["fuse"]) == 1
    expected_ply = str(ply_root / "cluster_0" / "fused.ply")
    assert result == {0: expected_ply}
    assert Path(expected_ply).exists()

    sig = mvs._dense_sig(0, str(sparse_dir), **_dense_sig_args())
    assert mani.is_done("dense", 0, sig) is True


@pytest.mark.parametrize("geom_consistency", [True, False])
def test_reconstruct_threads_geom_consistency(tmp_path, monkeypatch, geom_consistency):
    """geom_consistency must reach BOTH the patch_match spec (pool) AND dense_fuse
    (a miss here reintroduces the empty-PLY bug fixed in 193ded76)."""
    from databricks.labs.gbx.pyrx import mvs

    sparse_dir = tmp_path / "sparse"
    sparse_dir.mkdir()

    captured = {}

    def fake_undistort(sparse, image_dir, work_dir, *, num_src_images=None):
        Path(work_dir).mkdir(parents=True, exist_ok=True)
        return work_dir

    def fake_pool(specs, *, allocation=None, runner=None, max_retries=1):
        captured["pool_geom"] = [s["geom_consistency"] for s in specs]
        return {s["cluster_id"]: {"status": "ok"} for s in specs}

    def fake_fuse(work_dir, out_ply, *, geom_consistency=True):
        captured["fuse_geom"] = geom_consistency
        Path(out_ply).write_bytes(b"ply")
        return out_ply

    monkeypatch.setattr(mvs, "dense_undistort", fake_undistort)
    monkeypatch.setattr(mvs, "dense_mvs_pool", fake_pool)
    monkeypatch.setattr(mvs, "dense_fuse", fake_fuse)
    monkeypatch.setattr(
        mvs, "recommend_dense_allocation", lambda *a, **k: {"cache_size_gb": 8.0}
    )

    mvs.dense_reconstruct_clusters(
        {0: (str(sparse_dir), None)},
        str(tmp_path / "images"),
        work_root=str(tmp_path / "work"),
        geom_consistency=geom_consistency,
    )

    assert captured["pool_geom"] == [geom_consistency]
    assert captured["fuse_geom"] == geom_consistency


def test_reconstruct_drops_patch_match_failure(tmp_path, monkeypatch):
    """A cluster whose patch_match pool status != 'ok' is dropped from the
    result (and reported via on_event); other clusters still succeed."""
    from databricks.labs.gbx.pyrx import mvs

    sparse0 = tmp_path / "sparse0"
    sparse0.mkdir()
    sparse1 = tmp_path / "sparse1"
    sparse1.mkdir()

    def fake_undistort(sparse, image_dir, work_dir, *, num_src_images=None):
        Path(work_dir).mkdir(parents=True, exist_ok=True)
        return work_dir

    def fake_pool(specs, *, allocation=None, runner=None, max_retries=1):
        out = {}
        for s in specs:
            if s["cluster_id"] == 0:
                out[s["cluster_id"]] = {"status": "error", "error": "boom"}
            else:
                out[s["cluster_id"]] = {"status": "ok"}
        return out

    def fake_fuse(work_dir, out_ply, *, geom_consistency=True):
        Path(out_ply).write_bytes(b"ply")
        return out_ply

    monkeypatch.setattr(mvs, "dense_undistort", fake_undistort)
    monkeypatch.setattr(mvs, "dense_mvs_pool", fake_pool)
    monkeypatch.setattr(mvs, "dense_fuse", fake_fuse)
    monkeypatch.setattr(
        mvs, "recommend_dense_allocation", lambda *a, **k: {"cache_size_gb": 8.0}
    )

    events = []
    result = mvs.dense_reconstruct_clusters(
        {0: (str(sparse0), None), 1: (str(sparse1), None)},
        str(tmp_path / "images"),
        work_root=str(tmp_path / "work"),
        on_event=events.append,
    )

    assert 0 not in result
    assert 1 in result
    assert any("[DROP]" in e for e in events)


def test_reconstruct_force_ignores_checkpoint(tmp_path, monkeypatch):
    """force=True bypasses an existing checkpoint entry and recomputes."""
    from databricks.labs.gbx.pyrx import mvs
    from databricks.labs.gbx.pyrx.checkpoint import Manifest

    sparse_dir = tmp_path / "sparse"
    sparse_dir.mkdir()
    (sparse_dir / "images.bin").write_bytes(b"x" * 7)

    ply_root = tmp_path / "plys"
    persisted = ply_root / "cluster_0" / "fused.ply"
    persisted.parent.mkdir(parents=True)
    persisted.write_bytes(b"old")

    mani = Manifest(str(tmp_path / "manifest.json"))
    sig = mvs._dense_sig(0, str(sparse_dir), **_dense_sig_args())
    mani.mark_done("dense", 0, sig, str(persisted))

    calls = {"undistort": 0, "fuse": 0}

    def fake_undistort(sparse, image_dir, work_dir, *, num_src_images=None):
        calls["undistort"] += 1
        Path(work_dir).mkdir(parents=True, exist_ok=True)
        return work_dir

    def fake_pool(specs, *, allocation=None, runner=None, max_retries=1):
        return {s["cluster_id"]: {"status": "ok"} for s in specs}

    def fake_fuse(work_dir, out_ply, *, geom_consistency=True):
        calls["fuse"] += 1
        Path(out_ply).write_bytes(b"new")
        return out_ply

    monkeypatch.setattr(mvs, "dense_undistort", fake_undistort)
    monkeypatch.setattr(mvs, "dense_mvs_pool", fake_pool)
    monkeypatch.setattr(mvs, "dense_fuse", fake_fuse)
    monkeypatch.setattr(
        mvs, "recommend_dense_allocation", lambda *a, **k: {"cache_size_gb": 8.0}
    )

    result = mvs.dense_reconstruct_clusters(
        {0: (str(sparse_dir), None)},
        str(tmp_path / "images"),
        work_root=str(tmp_path / "work"),
        ply_root=str(ply_root),
        checkpoint=mani,
        force=True,
    )

    assert calls["undistort"] == 1
    assert calls["fuse"] == 1
    assert result == {0: str(persisted)}
    assert Path(persisted).read_bytes() == b"new"


def test_reconstruct_recompute_no_ply_root_returns_local_path(tmp_path, monkeypatch):
    """Review Focus (b): ply_root=None keeps a recomputed cluster's fused ply
    LOCAL (under work_root), not a Volume path."""
    from databricks.labs.gbx.pyrx import mvs

    sparse_dir = tmp_path / "sparse"
    sparse_dir.mkdir()

    def fake_undistort(sparse, image_dir, work_dir, *, num_src_images=None):
        Path(work_dir).mkdir(parents=True, exist_ok=True)
        return work_dir

    def fake_pool(specs, *, allocation=None, runner=None, max_retries=1):
        return {s["cluster_id"]: {"status": "ok"} for s in specs}

    def fake_fuse(work_dir, out_ply, *, geom_consistency=True):
        Path(out_ply).write_bytes(b"ply")
        return out_ply

    monkeypatch.setattr(mvs, "dense_undistort", fake_undistort)
    monkeypatch.setattr(mvs, "dense_mvs_pool", fake_pool)
    monkeypatch.setattr(mvs, "dense_fuse", fake_fuse)
    monkeypatch.setattr(
        mvs, "recommend_dense_allocation", lambda *a, **k: {"cache_size_gb": 8.0}
    )

    work_root = str(tmp_path / "work")
    result = mvs.dense_reconstruct_clusters(
        {0: (str(sparse_dir), None)},
        str(tmp_path / "images"),
        work_root=work_root,
    )

    assert result == {0: f"{work_root}/dense_c0/fused.ply"}


def test_reconstruct_skip_uses_manifest_output_not_this_run_ply_root(
    tmp_path, monkeypatch
):
    """FIX A regression guard: the checkpoint-skip branch must return the REAL
    persisted path recorded in the manifest (via Manifest.get_output), not a
    work_dir path derived from THIS run's (possibly absent/different) ply_root.
    Run 1 persists under ply_root; run 2 reuses the same manifest with a
    DIFFERENT work_root and ply_root=None, and must still (a) skip without
    calling dense_undistort/dense_mvs_pool and (b) return run 1's persisted
    Volume path — not f'{work_dir}/fused.ply' for a work_dir that was never
    populated this run."""
    from databricks.labs.gbx.pyrx import mvs
    from databricks.labs.gbx.pyrx.checkpoint import Manifest

    sparse_dir = tmp_path / "sparse"
    sparse_dir.mkdir()
    (sparse_dir / "images.bin").write_bytes(b"x" * 10)

    def fake_undistort(sparse, image_dir, work_dir, *, num_src_images=None):
        Path(work_dir).mkdir(parents=True, exist_ok=True)
        return work_dir

    def fake_pool(specs, *, allocation=None, runner=None, max_retries=1):
        return {s["cluster_id"]: {"status": "ok"} for s in specs}

    def fake_fuse(work_dir, out_ply, *, geom_consistency=True):
        Path(out_ply).write_bytes(b"ply")
        return out_ply

    monkeypatch.setattr(mvs, "dense_undistort", fake_undistort)
    monkeypatch.setattr(mvs, "dense_mvs_pool", fake_pool)
    monkeypatch.setattr(mvs, "dense_fuse", fake_fuse)
    monkeypatch.setattr(
        mvs, "recommend_dense_allocation", lambda *a, **k: {"cache_size_gb": 8.0}
    )

    mani = Manifest(str(tmp_path / "manifest.json"))
    ply_root = tmp_path / "plys"

    # Run 1: ply_root given -> persists + marks done.
    result1 = mvs.dense_reconstruct_clusters(
        {0: (str(sparse_dir), None)},
        str(tmp_path / "images"),
        work_root=str(tmp_path / "work1"),
        ply_root=str(ply_root),
        checkpoint=mani,
    )
    persisted_path = str(ply_root / "cluster_0" / "fused.ply")
    assert result1 == {0: persisted_path}

    calls = {"undistort": 0, "pool": 0}
    monkeypatch.setattr(
        mvs,
        "dense_undistort",
        lambda *a, **k: calls.__setitem__("undistort", calls["undistort"] + 1),
    )
    monkeypatch.setattr(
        mvs,
        "dense_mvs_pool",
        lambda *a, **k: (calls.__setitem__("pool", calls["pool"] + 1), {})[1],
    )

    # Run 2: same cid/knobs (same sig), different work_root, ply_root=None.
    result2 = mvs.dense_reconstruct_clusters(
        {0: (str(sparse_dir), None)},
        str(tmp_path / "images"),
        work_root=str(tmp_path / "work2"),
        ply_root=None,
        checkpoint=mani,
    )

    assert result2 == {0: persisted_path}
    assert calls["undistort"] == 0
    assert calls["pool"] == 0
