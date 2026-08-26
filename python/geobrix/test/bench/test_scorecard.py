"""Tests for the two read-only store consumers: scorecard-from-store and the
pre-push staleness warning.

Both are read-only over the authoritative store (no benchmarking, no Docker). A
temp dir is passed as ``root=`` so nothing touches the real ``test-logs/``.
"""

from types import SimpleNamespace

from databricks.labs.gbx.bench import compare, spec, store


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


def test_scorecard_empty_store(tmp_path):
    out = compare.scorecard_from_store(root=tmp_path)
    # Denominator derived from the SAME source the scorecard uses
    # (spec.registered_rst) so it matches whatever the code emits -- no literal.
    total = len(compare.registered_raster())
    assert "Benchmark coverage:** 0" in out
    assert f"/ {total}" in out
    # nothing covered -> every registered fn listed as not-yet-covered
    assert "Not yet covered" in out


def test_scorecard_aggregates_over_store(tmp_path):
    # Three records: one exact, one divergent, one timing-only (na). One of them
    # has a stale sources hash (its source mutated after the record was written).
    src_exact = _write_source(tmp_path, "core/exact.py", "v1\n")
    src_div = _write_source(tmp_path, "core/div.py", "v1\n")
    src_na = _write_source(tmp_path, "core/na.py", "v1\n")

    _write(tmp_path, "rst_avg", sources=(src_exact,), cells=[_cell("exact", 0.0, 3.0)])
    _write(
        tmp_path,
        "rst_slope",
        sources=(src_div,),
        cells=[_cell("divergent", 0.5, 0.4)],
    )
    _write(tmp_path, "rst_min", sources=(src_na,), cells=[_cell("na", 0.0, 1.0)])

    # Mutate one source so its record is stale.
    _write_source(tmp_path, "core/div.py", "MUTATED\n")

    # Patch spec lookup so STALE marking uses our fake specs (real registry would
    # not know about these source paths).
    specs_by_name = {
        "rst_avg": _spec("rst_avg", (src_exact,)),
        "rst_slope": _spec("rst_slope", (src_div,)),
        "rst_min": _spec("rst_min", (src_na,)),
    }
    out = compare.scorecard_from_store(root=tmp_path, specs_by_name=specs_by_name)

    total = len(compare.registered_raster())
    assert f"Benchmark coverage:** 3 / {total}" in out
    # parity: 1 exact, 1 divergent, 1 timing-only (na)
    assert "exact 1" in out
    assert "rst_slope" in out  # divergent fn surfaced
    # functional parity gap line present (computed registered - implemented)
    assert "Functional parity gap" in out
    # not-yet-covered: total - 3
    assert "Not yet covered" in out
    # the mutated-source record is flagged STALE
    assert "STALE" in out
    # commits surface as short hashes
    assert "abcdef1" in out


def test_scorecard_default_specs_from_registry(tmp_path):
    # With no specs_by_name override the scorecard resolves specs from the live
    # registry; a record for a real registered fn must not crash and must show up.
    reg = sorted(compare.registered_raster())
    fn = reg[0]
    specs_by_name = {s.name: s for s in spec.select(set="full")}
    _write(
        tmp_path,
        fn,
        sources=specs_by_name[fn].sources,
        cells=[_cell("exact", 0.0, 2.0)],
    )
    out = compare.scorecard_from_store(root=tmp_path)
    total = len(compare.registered_raster())
    assert f"Benchmark coverage:** 1 / {total}" in out
    assert fn in out


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


def test_scorecard_per_function_table_has_delta_columns(tmp_path):
    # Δms = hw - lw; Δ% = (hw - lw) / hw * 100. Sign convention: positive =>
    # heavy slower / lightweight faster. _cell uses hw=10.0, speedup=2.0 =>
    # lw=5.0 => Δms=+5.0, Δ%=+50.0.
    src = _write_source(tmp_path, "core/avg.py", "v1\n")
    _write(tmp_path, "rst_avg", sources=(src,), cells=[_cell("exact", 0.0, 2.0)])
    specs_by_name = {"rst_avg": _spec("rst_avg", (src,))}
    out = compare.scorecard_from_store(root=tmp_path, specs_by_name=specs_by_name)
    header = [ln for ln in out.splitlines() if "| fn |" in ln][0]
    assert "Δms" in header
    assert "Δ%" in header
    row = [ln for ln in out.splitlines() if ln.startswith("| rst_avg ")][0]
    assert "+5.000" in row
    assert "+50.0" in row
    assert "heavy − light" in out


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


def test_stale_changed_functions_none_affected(tmp_path, monkeypatch):
    monkeypatch.setattr(
        store,
        "resolve_changed",
        lambda base=None, specs=None, root=None: ([], [], []),
    )
    assert store.stale_changed_functions(root=tmp_path) == []
