"""Tests for the two read-only store consumers: scorecard-from-store and the
pre-push staleness warning (regressions only: a variant case counts as its function, a zero heavy median does not
divide by zero, and stale means a changed source or no record).

Both are read-only over the authoritative store (no benchmarking, no Docker). A
temp dir is passed as ``root=`` so nothing touches the real ``test-logs/``.
"""

from types import SimpleNamespace

from databricks.labs.gbx.bench import compare, store


def _spec(name, sources):
    return SimpleNamespace(name=name, sources=tuple(sources))


def _write_source(root, rel, content):
    fp = root / rel
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_text(content)
    return rel


def _cell(consistency="exact", max_rel_delta=0.0, speedup=2.0):
    """A minimal store cell dict (subset of CellCompare fields the scorecard reads)."""
    return {
        "consistency": consistency,
        "max_rel_delta": max_rel_delta,
        "speedup": speedup,
        "hw_median_ms": 10.0,
        "lw_median_ms": 10.0 / speedup if speedup else 0.0,
    }


def _write(root, fn, *, sources, cells):
    store.write_record(
        fn,
        sources=sources,
        cells=cells,
        heavy_rows=[],
        light_rows=[],
        commit="abcdef1234567890",
        validated_at="2026-06-07T00:00:00Z",
        corpus="seed=4",
        which="full",
        root=root,
    )


def test_scorecard_counts_a_variant_case_as_its_function(tmp_path):
    # rst_h3_tessellate_centroid is a second case of rst_h3_tessellate: one function.
    src = _write_source(tmp_path, "core/t.py", "v1\n")
    for fn in ("rst_h3_tessellate", "rst_h3_tessellate_centroid"):
        _write(tmp_path, fn, sources=(src,), cells=[_cell("exact", 0.0, 2.0)])
    specs_by_name = {
        fn: _spec(fn, (src,))
        for fn in ("rst_h3_tessellate", "rst_h3_tessellate_centroid")
    }
    out = compare.scorecard_from_store(root=tmp_path, specs_by_name=specs_by_name)
    total = len(compare.registered_raster())
    assert f"Benchmark coverage:** 1 / {total}" in out
    assert f"Not yet covered:** {total - 1} registered" in out


def test_stale_changed_functions(tmp_path, monkeypatch):
    # Three affected functions: one up-to-date record, one stale-hash record, one
    # with no record. stale_changed_functions returns the stale + missing, NOT the
    # up-to-date one.
    src_ok = _write_source(tmp_path, "core/ok.py", "v1\n")
    src_stale = _write_source(tmp_path, "core/stale.py", "v1\n")
    src_missing = _write_source(tmp_path, "core/missing.py", "v1\n")

    _write(tmp_path, "rst_ok", sources=(src_ok,), cells=[])
    _write(tmp_path, "rst_stale", sources=(src_stale,), cells=[])
    # rst_missing: deliberately no record written.

    # Mutate the stale fn's source so its stored hash no longer matches.
    _write_source(tmp_path, "core/stale.py", "MUTATED\n")

    specs = [
        _spec("rst_ok", (src_ok,)),
        _spec("rst_stale", (src_stale,)),
        _spec("rst_missing", (src_missing,)),
    ]
    specs_by_name = {s.name: s for s in specs}

    # resolve_changed returns (changed_paths, affected_fns, unmapped). Patch it to
    # report all three as affected without touching git.
    monkeypatch.setattr(
        store,
        "resolve_changed",
        lambda base=None, specs=None, root=None: (
            [src_ok, src_stale, src_missing],
            ["rst_missing", "rst_ok", "rst_stale"],
            [],
        ),
    )

    out = store.stale_changed_functions(root=tmp_path, specs_by_name=specs_by_name)
    assert out == ["rst_missing", "rst_stale"]
    assert "rst_ok" not in out


def test_scorecard_delta_pct_zero_hw_guard(tmp_path):
    # hw_median_ms == 0 must render the guard token, not raise ZeroDivisionError.
    src = _write_source(tmp_path, "core/z.py", "v1\n")
    cell = {
        "consistency": "na",
        "max_rel_delta": 0.0,
        "speedup": 0.0,
        "hw_median_ms": 0.0,
        "lw_median_ms": 2.0,
    }
    _write(tmp_path, "rst_z", sources=(src,), cells=[cell])
    specs_by_name = {"rst_z": _spec("rst_z", (src,))}
    out = compare.scorecard_from_store(root=tmp_path, specs_by_name=specs_by_name)
    row = [ln for ln in out.splitlines() if ln.startswith("| rst_z ")][0]
    assert "n/a" in row or "—" in row
