import csv as _csv

from databricks.labs.gbx.bench import compare as c
from databricks.labs.gbx.bench import results as R


def _rr(api, fn, mode, median, fp="", **kw):
    base = dict(
        run_id="r",
        api=api,
        fn=fn,
        category="terrain",
        mode=mode,
        tile_px=256,
        bands=2,
        dtype="float32",
        srid=4326,
        rows=1,
        nodata_frac=0.0,
        warmup_iters=1,
        measured_iters=2,
        iter_median_s=median,
        iter_min_s=median,
        iter_p90_s=median,
        throughput_mpix_s=1.0,
        throughput_rows_s=1.0,
        peak_rss_mb=0.0,
        status="ok",
        note="",
        env_arch="x",
        env_cpu_model="x",
        env_cpu_count=1,
        env_os="x",
        env_gbx_version="0.4.0",
        env_gdal_version="3.12.1",
        env_runtime_version="x",
        env_where="x",
        output_fingerprint=fp,
    )
    base.update(kw)
    return R.ResultRow(**base)


def test_join_and_speedup_and_consistency():
    hw = [
        _rr(
            "heavyweight", "rst_slope", "pure-core", 20.0, '{"kind":"scalar","value":5}'
        ),
        _rr(
            "heavyweight",
            "rst_only_hw",
            "pure-core",
            5.0,
            '{"kind":"scalar","value":1}',
        ),
    ]
    lw = [
        _rr(
            "lightweight", "rst_slope", "pure-core", 4.0, '{"kind":"scalar","value":5}'
        ),
        _rr(
            "lightweight",
            "rst_only_lw",
            "pure-core",
            3.0,
            '{"kind":"scalar","value":1}',
        ),
    ]
    cells, unmatched = c.compare_cells(hw, lw)
    assert len(cells) == 1
    cell = cells[0]
    assert cell.fn == "rst_slope"
    assert abs(cell.speedup - 5.0) < 1e-9
    assert cell.consistency == "exact"
    assert {u[0] for u in unmatched} == {"rst_only_hw", "rst_only_lw"}


def test_spark_path_consistency_is_na():
    hw = [_rr("heavyweight", "rst_width", "spark-path", 100.0, "", srid=0)]
    lw = [_rr("lightweight", "rst_width", "spark-path", 50.0, "", srid=0)]
    cells, _ = c.compare_cells(hw, lw)
    assert cells[0].consistency == "na"
    assert abs(cells[0].speedup - 2.0) < 1e-9


def test_scalar_exact_and_tol_and_divergent():
    assert (
        c.compare_fingerprints(
            '{"kind":"scalar","value":256}', '{"kind":"scalar","value":256}'
        )[0]
        == "exact"
    )
    cls, delta, _, _ = c.compare_fingerprints(
        '{"kind":"scalar","value":100.0}', '{"kind":"scalar","value":100.00005}'
    )
    assert cls == "within_tol"
    assert (
        c.compare_fingerprints(
            '{"kind":"scalar","value":100.0}', '{"kind":"scalar","value":150.0}'
        )[0]
        == "divergent"
    )


def test_kind_mismatch_is_divergent():
    assert (
        c.compare_fingerprints(
            '{"kind":"scalar","value":1}', '{"kind":"raster","bands":[]}'
        )[0]
        == "divergent"
    )


def test_empty_fingerprint_is_na():
    assert c.compare_fingerprints("", "")[0] == "na"
    assert c.compare_fingerprints('{"kind":"scalar","value":1}', "")[0] == "na"


def test_timing_only_empty_both_sides_is_na_not_divergent():
    # Task 4: a timing-only fn (fingerprint=False) emits "" on BOTH engines.
    # Empty-vs-empty must compare as `na` (timed, not compared), never divergent.
    cls, delta, ndc, note = c.compare_fingerprints("", "")
    assert cls == "na"
    assert delta == 0.0
    assert ndc == 0


def test_raster_dtype_excluded_nodata_count_informational():
    hw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"Float32","nodata_count":12,"min":0.0,"max":1.0,"mean":0.5,"std":0.25}]}'
    lw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"float32","nodata_count":0,"min":0.0,"max":1.0,"mean":0.5,"std":0.25}]}'
    cls, delta, ndc_delta, _ = c.compare_fingerprints(hw, lw)
    assert cls == "exact"
    assert ndc_delta == 12
    assert delta == 0.0


def test_raster_stat_divergence():
    hw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"Float32","nodata_count":0,"min":0.0,"max":90.0,"mean":45.0,"std":10.0}]}'
    lw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"float32","nodata_count":0,"min":0.0,"max":1.0,"mean":0.5,"std":0.25}]}'
    assert c.compare_fingerprints(hw, lw)[0] == "divergent"


def test_scalar_list_tolerance():
    assert (
        c.compare_fingerprints(
            '{"kind":"scalar_list","values":[1.0,2.0]}',
            '{"kind":"scalar_list","values":[1.0,2.0]}',
        )[0]
        == "exact"
    )
    assert (
        c.compare_fingerprints(
            '{"kind":"scalar_list","values":[1.0,2.0]}',
            '{"kind":"scalar_list","values":[1.0,9.0]}',
        )[0]
        == "divergent"
    )


def test_near_zero_stats_within_abs_tol_not_divergent():
    # min ~0 on both sides, tiny absolute diff -> within_tol via abs_tol, NOT divergent
    hw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"Float32","nodata_count":0,"min":1e-9,"max":1.0,"mean":0.5,"std":0.25}]}'
    lw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"float32","nodata_count":0,"min":2e-9,"max":1.0,"mean":0.5,"std":0.25}]}'
    cls, _, _, _ = c.compare_fingerprints(hw, lw)
    assert cls == "within_tol"


def test_abs_tol_does_not_mask_real_divergence():
    # large diff still divergent
    hw = '{"kind":"scalar","value":1.0}'
    lw = '{"kind":"scalar","value":5.0}'
    assert c.compare_fingerprints(hw, lw)[0] == "divergent"


def test_aspect_min_near_zero_not_divergent():
    # rst_aspect: mean/max/std agree to 8-9 sig figs, but the `min` aspect bearing
    # is a single near-zero pixel (~0.00029 deg) whose ~2.2e-5 deg abs diff blows
    # past the 1e-3 RELATIVE tolerance when divided by the near-zero reference.
    # This is a metric artifact at near-zero values, not an algorithmic divergence;
    # the absolute-tolerance floor must absorb it -> within_tol, NOT divergent.
    hw = (
        '{"kind":"raster","bands":[{"shape":[256,256],"dtype":"Float32",'
        '"nodata_count":0,"min":0.00029,"max":359.9876,"mean":180.4231,'
        '"std":103.8842}]}'
    )
    lw = (
        '{"kind":"raster","bands":[{"shape":[256,256],"dtype":"float32",'
        '"nodata_count":0,"min":0.0000679,"max":359.9876,"mean":180.4231,'
        '"std":103.8842}]}'
    )
    cls, _, _, _ = c.compare_fingerprints(hw, lw)
    assert cls == "within_tol"


def test_abs_tol_does_not_mask_real_min_divergence():
    # A genuine divergence on `min` (abs diff 2.0 on values ~10) must stay divergent;
    # the near-zero abs floor must not swallow real, order-of-magnitude-1+ differences.
    hw = (
        '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"Float32",'
        '"nodata_count":0,"min":10.0,"max":90.0,"mean":45.0,"std":10.0}]}'
    )
    lw = (
        '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"float32",'
        '"nodata_count":0,"min":12.0,"max":90.0,"mean":45.0,"std":10.0}]}'
    )
    assert c.compare_fingerprints(hw, lw)[0] == "divergent"


def test_write_csv(tmp_path):
    cells = [
        _cmp(
            "rst_slope",
            "pure-core",
            20.0,
            4.0,
            5.0,
            "within_tol",
            "nodata_count differs",
            max_rel_delta=0.0004,
            nodata_count_delta=1020,
        )
    ]
    p = tmp_path / "comparison.csv"
    c.write_csv(cells, p)
    rows = list(_csv.DictReader(p.open()))
    assert rows[0]["fn"] == "rst_slope"
    assert float(rows[0]["speedup"]) == 5.0
    assert rows[0]["consistency"] == "within_tol"


def test_summarize_compare_has_insights(tmp_path):
    cells = [
        _cmp(
            "rst_slope",
            "pure-core",
            20.0,
            4.0,
            5.0,
            "within_tol",
            "nodata_count differs",
            max_rel_delta=0.0004,
            nodata_count_delta=1020,
        ),
        _cmp(
            "rst_ndvi",
            "pure-core",
            660.0,
            5.0,
            132.0,
            "divergent",
            "",
            max_rel_delta=0.9,
        ),
    ]
    unmatched = [("rst_viewshed", "lightweight", ("rst_viewshed",))]
    md = c.summarize_compare(cells, unmatched, [], [])
    assert "## Insights" in md
    assert "rst_ndvi" in md  # biggest lightweight win (132x) surfaced
    assert "divergent" in md.lower()
    assert "rst_viewshed" in md  # unmatched surfaced


def test_compare_main_writes_outputs(tmp_path):
    from databricks.labs.gbx.bench import results as RR

    hw = tmp_path / "heavyweight.jsonl"
    lw = tmp_path / "lightweight.jsonl"
    RR.write_jsonl(
        [
            _rr(
                "heavyweight",
                "rst_slope",
                "pure-core",
                20.0,
                '{"kind":"scalar","value":5}',
            )
        ],
        hw,
    )
    RR.write_jsonl(
        [
            _rr(
                "lightweight",
                "rst_slope",
                "pure-core",
                4.0,
                '{"kind":"scalar","value":5}',
            )
        ],
        lw,
    )
    outdir = tmp_path / "out"
    c.main(
        ["--heavyweight", str(hw), "--lightweight", str(lw), "--out-dir", str(outdir)]
    )
    assert (outdir / "comparison.csv").exists()
    assert (outdir / "summary.md").exists()
    assert "Insights" in (outdir / "summary.md").read_text()


def test_divergent_with_nodata_delta_gets_border_note():
    # value stats differ past tol AND nodata_count differs -> note explains likely border cause
    hw = '{"kind":"raster","bands":[{"shape":[256,256],"dtype":"Float32","nodata_count":1020,"min":0.0,"max":90.0,"mean":45.0,"std":10.0}]}'
    lw = '{"kind":"raster","bands":[{"shape":[256,256],"dtype":"float32","nodata_count":0,"min":0.0,"max":90.5,"mean":47.0,"std":10.4}]}'
    cls, _, ndc, note = c.compare_fingerprints(hw, lw)
    assert cls == "divergent"
    assert ndc == 1020
    assert "nodata" in note.lower() and "border" in note.lower()


def test_divergent_without_nodata_delta_has_no_border_note():
    hw = '{"kind":"scalar","value":1.0}'
    lw = '{"kind":"scalar","value":5.0}'
    cls, _, ndc, note = c.compare_fingerprints(hw, lw)
    assert cls == "divergent"
    assert ndc == 0
    assert note == ""  # pure value divergence, no nodata delta -> no border note


def _cmp(fn, mode, hw_ms, lw_ms, speedup, consistency, note, **kw):
    """Build a CellCompare with sensible defaults for the throughput fields."""
    base = dict(
        fn=fn,
        mode=mode,
        tile_px=256,
        bands=2,
        dtype="float32",
        srid=4326,
        nodata_frac=0.0,
        rows=1,
        hw_median_ms=hw_ms,
        lw_median_ms=lw_ms,
        speedup=speedup,
        consistency=consistency,
        max_rel_delta=0.0,
        nodata_count_delta=0,
        note=note,
        hw_mpix_s=0.0,
        lw_mpix_s=0.0,
        hw_rows_s=0.0,
        lw_rows_s=0.0,
        hw_minus_lw_ms=hw_ms - lw_ms,
        delta_pct=((hw_ms - lw_ms) / hw_ms * 100.0) if hw_ms else 0.0,
    )
    base.update(kw)
    return c.CellCompare(**base)


def test_cellcompare_has_throughput_fields_and_csv_includes_them():
    cell = _cmp("rst_slope", "pure-core", 20.0, 4.0, 5.0, "exact", "")
    assert cell.hw_mpix_s == 0.0
    assert cell.lw_mpix_s == 0.0
    assert cell.hw_rows_s == 0.0
    assert cell.lw_rows_s == 0.0
    for f in ("hw_mpix_s", "lw_mpix_s", "hw_rows_s", "lw_rows_s"):
        assert f in c._CSV_FIELDS


def test_compare_cells_populates_throughput():
    hw = [
        _rr(
            "heavyweight",
            "rst_slope",
            "pure-core",
            20.0,
            '{"kind":"scalar","value":5}',
            throughput_mpix_s=3.3,
            throughput_rows_s=7.0,
        )
    ]
    lw = [
        _rr(
            "lightweight",
            "rst_slope",
            "pure-core",
            4.0,
            '{"kind":"scalar","value":5}',
            throughput_mpix_s=16.4,
            throughput_rows_s=35.0,
        )
    ]
    cells, _ = c.compare_cells(hw, lw)
    assert cells[0].hw_mpix_s == 3.3
    assert cells[0].lw_mpix_s == 16.4
    assert cells[0].hw_rows_s == 7.0
    assert cells[0].lw_rows_s == 35.0


def test_summarize_compare_pure_core_header_has_throughput_columns():
    cells = [_cmp("rst_slope", "pure-core", 20.0, 4.0, 5.0, "exact", "")]
    md = c.summarize_compare(cells, [], [], [])
    header = [ln for ln in md.splitlines() if "| fn |" in ln and "hw_iter_s" in ln][0]
    assert "hw_mpix/s" in header
    assert "lw_mpix/s" in header


def test_summarize_compare_renders_throughput_values():
    cells = [
        _cmp(
            "rst_slope",
            "pure-core",
            20.0,
            4.0,
            5.0,
            "exact",
            "",
            hw_mpix_s=3.3,
            lw_mpix_s=16.4,
        )
    ]
    md = c.summarize_compare(cells, [], [], [])
    row = [ln for ln in md.splitlines() if ln.startswith("| rst_slope ")][0]
    assert "3.3" in row
    assert "16.4" in row


def test_summarize_compare_has_tolerance_legend():
    cells = [_cmp("rst_slope", "pure-core", 20.0, 4.0, 5.0, "exact", "")]
    md = c.summarize_compare(cells, [], [], [])
    assert f"rel ≤ {c.REL_TOL:g}" in md
    assert f"abs ≤ {c.ABS_TOL:g}" in md
    assert "within_tol" in md


def test_summarize_compare_hoists_constant_dims_and_rounds_ms():
    cells = [
        _cmp("rst_a", "pure-core", 20.0, 4.0, 5.0, "exact", "", tile_px=512, bands=4),
        _cmp("rst_b", "pure-core", 30.0, 6.0, 5.0, "exact", "", tile_px=512, bands=4),
    ]
    md = c.summarize_compare(cells, [], [], [], pool_size=1000)
    header = [ln for ln in md.splitlines() if "| fn |" in ln and "hw_iter_s" in ln][0]
    # constant dims hoisted out of the header
    assert "tile_px" not in header
    assert "bands" not in header
    # context line carries them once, plus the pool token
    assert "tile_px 512" in md
    assert "4 bands" in md
    assert "pool 1000 tiles" in md
    # ms rounded to 1 decimal
    row = [ln for ln in md.splitlines() if ln.startswith("| rst_a ")][0]
    assert "20.0" in row
    assert "20.000" not in row


def test_summarize_compare_keeps_varying_dim_as_column():
    cells = [
        _cmp("rst_a", "pure-core", 20.0, 4.0, 5.0, "exact", "", tile_px=256),
        _cmp("rst_b", "pure-core", 30.0, 6.0, 5.0, "exact", "", tile_px=4096),
    ]
    md = c.summarize_compare(cells, [], [], [])
    header = [ln for ln in md.splitlines() if "| fn |" in ln and "hw_iter_s" in ln][0]
    assert "tile_px" in header


def test_summarize_compare_keeps_varying_srid_as_column():
    # Mixed srid (or any non-px dim that varies) must surface as a column, not be
    # hoisted into the context line above the table.
    cells = [
        _cmp("rst_a", "pure-core", 20.0, 4.0, 5.0, "exact", "", srid=4326),
        _cmp("rst_b", "pure-core", 30.0, 6.0, 5.0, "exact", "", srid=3857),
    ]
    md = c.summarize_compare(cells, [], [], [])
    header = [ln for ln in md.splitlines() if "| fn |" in ln and "hw_iter_s" in ln][0]
    assert "srid" in header
    # constant srid stays hoisted (context line), not a column
    same = [
        _cmp("rst_a", "pure-core", 20.0, 4.0, 5.0, "exact", "", srid=4326),
        _cmp("rst_b", "pure-core", 30.0, 6.0, 5.0, "exact", "", srid=4326),
    ]
    md2 = c.summarize_compare(same, [], [], [])
    header2 = [ln for ln in md2.splitlines() if "| fn |" in ln and "hw_iter_s" in ln][0]
    assert "srid" not in header2
    assert "srid 4326" in md2


def test_summarize_compare_exact_label_is_bare():
    cells = [_cmp("rst_slope", "pure-core", 20.0, 4.0, 5.0, "exact", "")]
    md = c.summarize_compare(cells, [], [], [])
    row = [ln for ln in md.splitlines() if ln.startswith("| rst_slope ")][0]
    # the consistency cell is the bare label, no parenthetical / explanation
    assert "| exact |" in row
    assert "exact (" not in row


def test_pyrx_implemented_guards_against_binding_regression():
    # pyrx_implemented() parses `def rst_*` out of functions.py -- the guard is
    # "no registered function lost its pyrx binding", NOT a brittle exact count.
    # Assert the meaningful invariant: every registered rst_ function has a pyrx
    # binding (so pyrx_implemented ⊇ the registered set), plus membership spot-checks.
    impl = c.pyrx_implemented()
    registered = c.registered_raster()
    missing = registered - impl
    assert not missing, f"registered rst_ fns missing a pyrx binding: {sorted(missing)}"
    assert len(impl) >= len(registered)
    assert "rst_slope" in impl
    assert "rst_merge" in impl


def test_coverage_block_reports_coverage_parity_gap_and_uncovered():
    cells = [
        _cmp("rst_slope", "pure-core", 20.0, 4.0, 5.0, "exact", ""),
        _cmp("rst_ndvi", "pure-core", 20.0, 4.0, 5.0, "within_tol", ""),
        _cmp("rst_proximity", "pure-core", 20.0, 4.0, 5.0, "divergent", ""),
        _cmp("rst_aspect", "spark-path", 20.0, 30.0, 0.66, "exact", ""),
        # a timing-only cell (na) for a distinct fn
        _cmp("rst_clip", "pure-core", 20.0, 10.0, 2.0, "na", ""),
    ]
    md = c.coverage_block(cells)
    # Coverage: distinct fn count across all cells = 5, out of the registered rst_
    # total. Derive the denominator from the SAME source coverage_block uses
    # (compare.registered_raster -- rst_ only, excludes the pyvx st_ set) so the
    # "N / <total>" string matches whatever the code emits -- no magic literal.
    total = len(c.registered_raster())
    assert "Benchmark coverage:" in md
    assert f"/ {total}" in md
    assert f"5 / {total}" in md
    # Parity counts among non-na cells (4 compared: exact 2, within_tol 1, divergent 1)
    assert "exact" in md
    assert "within_tol" in md
    assert "divergent" in md
    assert "rst_proximity" in md  # divergent fn named
    # Functional parity gap computed = 0
    assert "Functional parity gap:** 0" in md
    # timing-only count line
    assert "timing-only" in md
    # pure-core-only / timing-only fns: count + at least one known-missing name
    assert "No comparison cell in this run:" in md
    assert "rst_merge" in md or "rst_rasterize" in md


def test_summarize_compare_includes_coverage_block():
    cells = [
        _cmp("rst_slope", "pure-core", 20.0, 4.0, 5.0, "exact", ""),
        _cmp("rst_proximity", "pure-core", 20.0, 4.0, 5.0, "divergent", ""),
        _cmp("rst_clip", "pure-core", 20.0, 10.0, 2.0, "na", ""),
    ]
    md = c.summarize_compare(cells, [], [], [])
    total = len(c.registered_raster())
    assert "Coverage & parity" in md
    assert f"/ {total}" in md
    assert "Functional parity gap:** 0" in md
    assert "No comparison cell in this run:" in md
    # ordering: Insights before Coverage before the per-mode tables
    assert md.index("## Insights") < md.index("Coverage & parity")
    assert md.index("Coverage & parity") < md.index("## pure-core")


def test_results_main_writes_summary(tmp_path):
    from databricks.labs.gbx.bench import results as RR

    shard = tmp_path / "heavyweight.jsonl"
    RR.write_jsonl(
        [
            _rr(
                "heavyweight",
                "rst_slope",
                "pure-core",
                20.0,
                '{"kind":"scalar","value":5}',
            )
        ],
        shard,
    )
    RR.main(["--in", str(shard)])
    out = tmp_path / "heavyweight.summary.md"
    assert out.exists()
    assert "GeoBrix benchmark summary" in out.read_text()


# --- bucket C, group C4: raster_collection fingerprint (tiling fns) ----------
# C4 outputs a LIST of tiles. The fingerprint records the tile COUNT plus the
# pooled (order-independent) agg stats over every output tile's pixels. The
# comparator requires the count to match EXACTLY, then compares the pooled agg
# with the same float tolerance as the raster kind.
def _coll(count, mn, mx, mean, std):
    import json

    return json.dumps(
        {
            "kind": "raster_collection",
            "count": count,
            "agg": {"min": mn, "max": mx, "mean": mean, "std": std},
        },
        sort_keys=True,
    )


def test_raster_collection_equal_count_close_agg_within_tol():
    hw = _coll(4, 0.0, 100.0, 50.0, 10.0)
    # Identical agg -> exact.
    assert c.compare_fingerprints(hw, hw)[0] == "exact"
    # Tiny agg diff within tolerance -> within_tol (not divergent).
    lw = _coll(4, 0.0, 100.00005, 50.00005, 10.0)
    cls, delta, _, _ = c.compare_fingerprints(hw, lw)
    assert cls == "within_tol", (cls, delta)


def test_raster_collection_count_mismatch_is_divergent():
    hw = _coll(4, 0.0, 100.0, 50.0, 10.0)
    lw = _coll(9, 0.0, 100.0, 50.0, 10.0)
    cls, _, _, note = c.compare_fingerprints(hw, lw)
    assert cls == "divergent"
    # The note must name both counts so the divergence is diagnosable.
    assert "4" in note and "9" in note


def test_raster_collection_agg_divergence():
    hw = _coll(4, 0.0, 90.0, 45.0, 10.0)
    lw = _coll(4, 0.0, 1.0, 0.5, 0.25)
    assert c.compare_fingerprints(hw, lw)[0] == "divergent"


# --- heavy - light timing deltas (Δms / Δ%) ---------------------------------
# Sign convention: Δ = heavy - light. Positive => heavy took more time =>
# heavy slower / lightweight faster. Negative => heavy faster.
def test_summarize_compare_header_has_delta_columns():
    cells = [_cmp("rst_slope", "pure-core", 10.0, 2.0, 5.0, "exact", "")]
    md = c.summarize_compare(cells, [], [], [])
    header = [ln for ln in md.splitlines() if "| fn |" in ln and "hw_iter_s" in ln][0]
    assert "Δs" in header
    assert "Δ%" in header


def test_summarize_compare_renders_positive_delta_heavy_slower():
    # hw=10, lw=2 => Δms = +8.0, Δ% = +80.0 (heavy slower / lightweight faster)
    cells = [_cmp("rst_slope", "pure-core", 10.0, 2.0, 5.0, "exact", "")]
    md = c.summarize_compare(cells, [], [], [])
    row = [ln for ln in md.splitlines() if ln.startswith("| rst_slope ")][0]
    assert "+8.0" in row
    assert "+80.0" in row


def test_summarize_compare_renders_negative_delta_heavy_faster():
    # hw=1, lw=4 => Δms = -3.0, Δ% = -300.0 (heavy faster)
    cells = [_cmp("rst_h", "pure-core", 1.0, 4.0, 0.25, "exact", "")]
    md = c.summarize_compare(cells, [], [], [])
    row = [ln for ln in md.splitlines() if ln.startswith("| rst_h ")][0]
    assert "-3.0" in row
    assert "-300.0" in row


def test_summarize_compare_zero_hw_delta_pct_guard():
    # hw=0 => Δ% undefined; must render the guard token, not raise.
    cells = [_cmp("rst_z", "pure-core", 0.0, 2.0, 0.0, "na", "")]
    md = c.summarize_compare(cells, [], [], [])
    row = [ln for ln in md.splitlines() if ln.startswith("| rst_z ")][0]
    assert "n/a" in row or "—" in row


def test_summarize_compare_spark_path_header_has_delta_columns():
    cells = [_cmp("rst_w", "spark-path", 10.0, 2.0, 5.0, "na", "")]
    md = c.summarize_compare(cells, [], [], [])
    header = [ln for ln in md.splitlines() if "| fn |" in ln and "lw_per_tile_s" in ln][
        0
    ]
    assert "Δs" in header
    assert "Δ%" in header
    # spark-path reports per-tile (iter_median_s / rows), not rows/s
    assert "hw_per_tile_s" in header
    assert "rows/s" not in header


def test_summarize_compare_has_delta_legend():
    cells = [_cmp("rst_slope", "pure-core", 10.0, 2.0, 5.0, "exact", "")]
    md = c.summarize_compare(cells, [], [], [])
    assert "heavy − light" in md
    assert "positive → heavy slower" in md


def test_cellcompare_csv_includes_delta_fields():
    assert "hw_minus_lw_ms" in c._CSV_FIELDS
    assert "delta_pct" in c._CSV_FIELDS


def test_compare_cells_populates_delta_fields():
    hw = [
        _rr(
            "heavyweight",
            "rst_slope",
            "pure-core",
            10.0,
            '{"kind":"scalar","value":5}',
        )
    ]
    lw = [
        _rr(
            "lightweight",
            "rst_slope",
            "pure-core",
            2.0,
            '{"kind":"scalar","value":5}',
        )
    ]
    cells, _ = c.compare_cells(hw, lw)
    assert abs(cells[0].hw_minus_lw_ms - 8.0) < 1e-9
    assert abs(cells[0].delta_pct - 80.0) < 1e-9


def test_write_csv_with_delta_fields(tmp_path):
    cells = [_cmp("rst_slope", "pure-core", 10.0, 2.0, 5.0, "exact", "")]
    p = tmp_path / "comparison.csv"
    c.write_csv(cells, p)
    rows = list(_csv.DictReader(p.open()))
    assert abs(float(rows[0]["hw_minus_lw_ms"]) - 8.0) < 1e-9
    assert abs(float(rows[0]["delta_pct"]) - 80.0) < 1e-9


# --- bucket B: dggs_grid fingerprint (grid fns) ------------------------------
# dggs_grid output is a set of cells (cell id + measure). The comparator requires
# the cell COUNT to match EXACTLY, compares the order-independent agg stats with
# the raster float tolerance, and — since H3/quadbin cell ids are PARITY-comparable
# across engines — treats an identical cells_hash as `exact`. When the hash differs
# but count + agg agree, it reports the cell-set Jaccard overlap and passes on
# count + agg.
import json as _json  # noqa: E402


def _dggs(count, cells_hash, mn, mx, mean, std):
    return _json.dumps(
        {
            "kind": "dggs_grid",
            "count": count,
            "cells_hash": cells_hash,
            "agg": {"min": mn, "max": mx, "mean": mean, "std": std},
        },
        sort_keys=True,
    )


def test_dggs_grid_identical_hash_is_exact():
    fp = _dggs(3, "abc123", 1.0, 3.0, 2.0, 0.8)
    assert c.compare_fingerprints(fp, fp)[0] == "exact"


def test_dggs_grid_equal_count_close_agg_within_tol():
    hw = _dggs(3, "hashA", 1.0, 3.0, 2.0, 0.8)
    # Different hash (different cell ids) but equal count + close agg -> within_tol.
    lw = _dggs(3, "hashB", 1.0, 3.00005, 2.00005, 0.8)
    cls, _, _, note = c.compare_fingerprints(hw, lw)
    assert cls == "within_tol", (cls, note)


def test_dggs_grid_count_mismatch_is_divergent():
    hw = _dggs(3, "hashA", 1.0, 3.0, 2.0, 0.8)
    lw = _dggs(7, "hashB", 1.0, 3.0, 2.0, 0.8)
    cls, _, _, note = c.compare_fingerprints(hw, lw)
    assert cls == "divergent"
    assert "cell count" in note and "3" in note and "7" in note


def test_dggs_grid_agg_divergence():
    hw = _dggs(3, "hashA", 0.0, 90.0, 45.0, 10.0)
    lw = _dggs(3, "hashB", 0.0, 1.0, 0.5, 0.25)
    assert c.compare_fingerprints(hw, lw)[0] == "divergent"


def test_dggs_grid_reports_jaccard_when_hash_differs():
    # Same count + close agg but different ids -> within_tol AND the note reports
    # the Jaccard cell-set overlap so a partial-overlap divergence is diagnosable.
    hw = _json.dumps(
        {
            "kind": "dggs_grid",
            "count": 4,
            "cells_hash": "h1",
            "cell_ids": [1, 2, 3, 4],
            "agg": {"min": 1.0, "max": 4.0, "mean": 2.5, "std": 1.1},
        },
        sort_keys=True,
    )
    lw = _json.dumps(
        {
            "kind": "dggs_grid",
            "count": 4,
            "cells_hash": "h2",
            "cell_ids": [3, 4, 5, 6],
            "agg": {"min": 1.0, "max": 4.0, "mean": 2.5, "std": 1.1},
        },
        sort_keys=True,
    )
    cls, _, _, note = c.compare_fingerprints(hw, lw)
    assert cls == "within_tol"
    # intersection {3,4} / union {1..6} = 2/6 = 0.333
    assert "jaccard" in note.lower()
    assert "0.33" in note


# --- bucket B: vector fingerprint (contour, polygonize) ----------------------
# vector output is a set of features. Two contouring engines (gdal.ContourGenerateEx
# vs skimage marching-squares) trace the SAME iso-surfaces at the same levels but
# split them into a different number of features (a segmentation artifact, ~8-10%
# count delta on identical geometry). So the comparator GATES on the total `measure`
# (line length / polygon area) and the order-independent `attr_agg` with the raster
# float tolerance; the feature COUNT is INFORMATIONAL only (reported in the note,
# never divergent on its own) — same philosophy as raster_collection's pooled agg.
def _vec(count, measure, mn, mx, mean, std):
    return _json.dumps(
        {
            "kind": "vector",
            "count": count,
            "measure": measure,
            "attr_agg": {"min": mn, "max": mx, "mean": mean, "std": std},
        },
        sort_keys=True,
    )


def test_vector_equal_count_close_measure_within_tol():
    hw = _vec(5, 100.0, 1.0, 9.0, 5.0, 2.0)
    assert c.compare_fingerprints(hw, hw)[0] == "exact"
    lw = _vec(5, 100.00005, 1.0, 9.00005, 5.0, 2.0)
    assert c.compare_fingerprints(hw, lw)[0] == "within_tol"


def test_vector_equal_count_equal_measure_is_exact():
    # polygonize-after-fix case: GDAL-integer grouping makes count + measure + attr
    # all match exactly -> exact must be preserved.
    hw = _vec(5, 100.0, 1.0, 9.0, 5.0, 2.0)
    assert c.compare_fingerprints(hw, hw)[0] == "exact"


def test_vector_count_differs_measure_matches_is_within_tol_informational():
    # contour case: two engines segment the SAME iso-lines into a different feature
    # count (~10% delta) while total length agrees within tolerance. measure + attr
    # within tol -> within_tol (NOT divergent); the note records the count delta.
    hw = _vec(3025, 1.76660, 0.0, 100.0, 50.0, 30.0)
    lw = _vec(3334, 1.76665, 0.0, 100.0, 50.0, 30.0)
    cls, _, _, note = c.compare_fingerprints(hw, lw)
    assert cls == "within_tol", (cls, note)
    # count delta surfaced as informational (both counts + the % delta named)
    assert "3025" in note and "3334" in note
    assert "informational" in note.lower()


def test_vector_count_mismatch_alone_is_not_divergent():
    # count differs but measure + attr agree exactly -> NOT divergent; count is
    # an arbitrary segmentation artifact, not a divergence signal.
    hw = _vec(5, 100.0, 1.0, 9.0, 5.0, 2.0)
    lw = _vec(8, 100.0, 1.0, 9.0, 5.0, 2.0)
    cls, _, _, note = c.compare_fingerprints(hw, lw)
    assert cls != "divergent"
    assert "5" in note and "8" in note and "informational" in note.lower()


def test_vector_measure_divergence():
    # measure is the real signal: a measure beyond tol stays divergent.
    hw = _vec(5, 100.0, 1.0, 9.0, 5.0, 2.0)
    lw = _vec(5, 250.0, 1.0, 9.0, 5.0, 2.0)
    assert c.compare_fingerprints(hw, lw)[0] == "divergent"


def test_vector_attr_agg_divergence():
    hw = _vec(5, 100.0, 1.0, 9.0, 5.0, 2.0)
    lw = _vec(5, 100.0, 1.0, 90.0, 45.0, 20.0)
    assert c.compare_fingerprints(hw, lw)[0] == "divergent"


# --- per-FnSpec rel_tol override (contour) -----------------------------------
# GDAL's contour generator and the lightweight marching-squares segmenter trace
# the same iso-lines but split them into segments differently, so per-segment
# measures/attrs spread ~1.5%. That inherent algorithm spread blows past the
# strict global REL_TOL (1e-3) and would flag `divergent`, so rst_contour
# declares rel_tol=0.02 in its FnSpec. compare_cells must honor that per-fn tol
# WITHOUT loosening the global tol for any other function.
def test_compare_fingerprints_rel_tol_arg_flips_divergent_to_within_tol():
    # ~1.5% spread on measure AND attr stats.
    hw = _vec(3025, 1.0000, 1.0, 100.0, 50.0, 30.0)
    lw = _vec(3334, 1.0150, 1.0, 101.5, 50.75, 30.45)
    # Default (global) tol: the 1.5% spread is a divergence.
    assert c.compare_fingerprints(hw, lw)[0] == "divergent"
    # With a 2% rel_tol the same spread is within tolerance.
    assert c.compare_fingerprints(hw, lw, rel_tol=0.02)[0] == "within_tol"


def test_compare_cells_applies_contour_per_fn_rel_tol():
    from databricks.labs.gbx.bench import spec as _spec

    # rst_contour declares the looser per-fn tol; the registry is the source.
    assert _spec.REGISTRY["rst_contour"].rel_tol == 0.02

    fp_hw = _vec(3025, 1.0000, 1.0, 100.0, 50.0, 30.0)
    fp_lw = _vec(3334, 1.0150, 1.0, 101.5, 50.75, 30.45)

    # A function with NO per-fn tol (default None -> global REL_TOL) is divergent
    # on this same ~1.5% spread.
    hw_default = [_rr("heavyweight", "rst_polygonize", "pure-core", 20.0, fp_hw)]
    lw_default = [_rr("lightweight", "rst_polygonize", "pure-core", 4.0, fp_lw)]
    cells, _ = c.compare_cells(hw_default, lw_default)
    assert cells[0].consistency == "divergent"

    # rst_contour, same spread, must be within_tol via its 0.02 per-fn tol.
    hw_contour = [_rr("heavyweight", "rst_contour", "pure-core", 20.0, fp_hw)]
    lw_contour = [_rr("lightweight", "rst_contour", "pure-core", 4.0, fp_lw)]
    cells, _ = c.compare_cells(hw_contour, lw_contour)
    assert cells[0].consistency == "within_tol"
