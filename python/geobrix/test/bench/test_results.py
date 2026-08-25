import dataclasses
import json

from databricks.labs.gbx.bench import results as r


def _row(**kw):
    base = dict(
        run_id="run1",
        api="lightweight",
        fn="rst_width",
        category="accessor",
        mode="pure-core",
        tile_px=256,
        bands=1,
        dtype="float32",
        srid=4326,
        rows=1,
        nodata_frac=0.02,
        warmup_iters=2,
        measured_iters=5,
        iter_median_s=1.5,
        iter_min_s=1.2,
        iter_p90_s=1.9,
        throughput_mpix_s=44.0,
        throughput_rows_s=666.0,
        peak_rss_mb=120.0,
        status="ok",
        note="",
        env_arch="arm64",
        env_cpu_model="M3",
        env_cpu_count=8,
        env_os="Darwin",
        env_gbx_version="0.4.0",
        env_gdal_version="3.8.0",
        env_runtime_version="py3.12",
        env_where="venv",
    )
    base.update(kw)
    return r.ResultRow(**base)


def test_jsonl_roundtrip(tmp_path):
    rows = [_row(), _row(fn="rst_slope", category="terrain", iter_median_s=20.0)]
    p = tmp_path / "shard.jsonl"
    r.write_jsonl(rows, p)
    loaded = r.read_jsonl(p)
    assert loaded == rows


def test_summary_lists_slowest(tmp_path):
    rows = [
        _row(fn="rst_fast", iter_median_s=1.0),
        _row(fn="rst_slow", iter_median_s=99.0),
    ]
    md = r.summarize(rows)
    assert "rst_slow" in md
    # slowest should appear before fast in the slowest-functions section
    assert md.index("rst_slow") < md.index("rst_fast")


def test_spark_path_table_reports_per_tile_s_and_ms():
    # spark-path table surfaces per_tile_avg = iter_median_s / rows, in BOTH s and ms.
    rows = [
        _row(fn="rst_resample", mode="spark-path", rows=1000, iter_median_s=100.0),
    ]
    md = r.summarize(rows)
    hdr = [
        ln
        for ln in md.splitlines()
        if ln.startswith("| fn |") and "per_tile_avg_ms" in ln
    ]
    assert hdr, "spark-path table should have a per_tile_avg_ms column"
    # per_tile_avg_s sits to the LEFT of per_tile_avg_ms.
    assert "per_tile_avg_s" in hdr[0]
    assert hdr[0].index("per_tile_avg_s") < hdr[0].index("per_tile_avg_ms")
    # iter_median_s (whole-iteration, seconds) is delineated from per_tile_avg.
    assert "iter_median_s" in hdr[0]
    row_line = [ln for ln in md.splitlines() if ln.startswith("| rst_resample ")][0]
    # 100 s / 1000 tiles = 0.1 s/tile = 100 ms/tile.
    assert "0.10000" in row_line  # per_tile_avg_s
    assert "100.000" in row_line  # per_tile_avg_ms


def test_summarize_has_insights_status_and_flags(tmp_path):
    rows = [
        _row(
            fn="rst_width",
            mode="pure-core",
            tile_px=256,
            iter_median_s=1.0,
            status="ok",
        ),
        _row(
            fn="rst_slope",
            mode="pure-core",
            tile_px=4096,
            iter_median_s=50.0,
            status="ok",
            category="terrain",
            nodata_frac=0.1,
            output_fingerprint='{"kind": "raster", "bands": [{"nodata_count": 0, "min": 0.0}]}',
        ),
        _row(
            fn="rst_ndvi",
            mode="pure-core",
            tile_px=256,
            bands=1,
            status="error",
            note="band index 2 out of range",
            iter_median_s=0.0,
        ),
    ]
    md = r.summarize(rows)
    assert "## Insights" in md
    assert "## Status" in md
    assert "rst_slope" in md  # slowest op named
    assert "rst_ndvi" in md  # error fn surfaced
    assert "error" in md.lower()
    assert (
        "nodata" in md.lower()
    )  # consistency flag for sentinel-as-data on nodata tile
    # env line present
    assert "GDAL" in md
    # tile_px VARIES (256 and 4096) across the slowest-pure-core rows -> it must
    # stay a COLUMN, not get hoisted above the table.
    slow_header = [
        ln
        for ln in md.splitlines()
        if ln.startswith("| fn |") and "median_s" in ln and "mpix/s" in ln
    ][0]
    assert "tile_px" in slow_header


def test_summarize_keeps_varying_srid_as_column():
    # Mixed srid (or any non-px dim that varies) stays a column; constant hoists.
    rows = [
        _row(fn="rst_a", mode="pure-core", srid=4326, iter_median_s=5.0),
        _row(fn="rst_b", mode="pure-core", srid=3857, iter_median_s=6.0),
    ]
    md = r.summarize(rows)
    hdr = [
        ln for ln in md.splitlines() if ln.startswith("| fn |") and "median_s" in ln
    ][0]
    assert "srid" in hdr
    # constant srid is hoisted out of the table into the context line
    same = [
        _row(fn="rst_a", mode="pure-core", srid=4326, iter_median_s=5.0),
        _row(fn="rst_b", mode="pure-core", srid=4326, iter_median_s=6.0),
    ]
    md2 = r.summarize(same)
    hdr2 = [
        ln for ln in md2.splitlines() if ln.startswith("| fn |") and "median_s" in ln
    ][0]
    assert "srid" not in hdr2
    assert "srid 4326" in md2


def _hoist_md(pool_size=None):
    """All pure-core rows share tile_px/bands/dtype/srid -> they hoist."""
    rows = [
        _row(fn="rst_a", iter_median_s=1.5, tile_px=512, bands=4, dtype="float32"),
        _row(fn="rst_b", iter_median_s=9.0, tile_px=512, bands=4, dtype="float32"),
    ]
    return r.summarize(rows, pool_size=pool_size)


def test_summarize_hoists_constant_dims_above_table():
    md = _hoist_md()
    slow_header = [
        ln
        for ln in md.splitlines()
        if ln.startswith("| fn |") and "median_s" in ln and "mpix/s" in ln
    ][0]
    # constant dims are NOT columns
    assert "tile_px" not in slow_header
    assert "bands" not in slow_header
    assert "dtype" not in slow_header
    # they appear once in a context line above the table
    assert "tile_px 512" in md
    assert "4 bands" in md
    assert "float32" in md
    assert "srid 4326" in md


def test_summarize_rounds_ms_to_one_decimal():
    md = _hoist_md()
    row = [ln for ln in md.splitlines() if ln.startswith("| rst_a ")][0]
    assert "1.5" in row
    assert "1.500" not in row


def test_summarize_shows_pool_size_token():
    md = _hoist_md(pool_size=1000)
    assert "pool 1000 tiles" in md


def test_summarize_pool_warns_when_smaller_than_rows():
    rows = [
        _row(
            fn="rst_sp",
            mode="spark-path",
            rows=1000,
            iter_median_s=5.0,
            srid=0,
        )
    ]
    md = r.summarize(rows, pool_size=500)
    assert "⚠" in md or "< rows" in md


# --- _normalize_row: heavyweight (Scala BenchRow) ms->s deserialization ---


def test_normalize_row_converts_all_ms_fields_to_seconds():
    out = r._normalize_row(
        {
            "median_ms": 1234.0,
            "min_ms": 1000.0,
            "p90_ms": 1900.0,
            "total_wall_clock_ms": 6170.0,
            "avg_wall_clock_ms": 1234.0,
        }
    )
    assert out["iter_median_s"] == 1.234
    assert out["iter_min_s"] == 1.0
    assert out["iter_p90_s"] == 1.9
    assert out["iter_total_wall_clock_s"] == 6.17
    assert out["avg_wall_clock_s"] == 1.234
    # the ms-suffixed keys are consumed, not carried through
    assert not any(k.endswith("_ms") for k in out)


def test_normalize_row_none_ms_becomes_zero():
    # a null timing (e.g. an errored heavyweight row) maps to 0.0, not None.
    assert r._normalize_row({"median_ms": None})["iter_median_s"] == 0.0


def test_normalize_row_does_not_clobber_existing_seconds():
    # setdefault: if the canonical seconds field is already present, the ms
    # value must not overwrite it (and the ms key is still dropped).
    out = r._normalize_row({"median_ms": 9999.0, "iter_median_s": 2.5})
    assert out["iter_median_s"] == 2.5
    assert "median_ms" not in out


def test_normalize_row_drops_unknown_keys():
    # forward-compat: columns ResultRow doesn't declare are dropped.
    out = r._normalize_row({"iter_median_s": 1.0, "some_future_col": "x"})
    assert out == {"iter_median_s": 1.0}


def test_read_jsonl_normalizes_heavyweight_ms_shard(tmp_path):
    # A heavyweight shard carries seconds as ms-suffixed fields plus columns
    # ResultRow doesn't declare; read_jsonl must yield a valid seconds ResultRow.
    base = dataclasses.asdict(_row(api="heavyweight"))
    ms_only = {
        k: v
        for k, v in base.items()
        if k
        not in (
            "iter_median_s",
            "iter_min_s",
            "iter_p90_s",
            "iter_total_wall_clock_s",
            "avg_wall_clock_s",
        )
    }
    ms_only.update(
        median_ms=2500.0,
        min_ms=2000.0,
        p90_ms=3000.0,
        total_wall_clock_ms=12500.0,
        avg_wall_clock_ms=2500.0,
        some_future_col="ignored",
    )
    p = tmp_path / "heavyweight.jsonl"
    p.write_text(json.dumps(ms_only) + "\n")
    (row,) = r.read_jsonl(p)
    assert isinstance(row, r.ResultRow)
    assert row.iter_median_s == 2.5
    assert row.iter_min_s == 2.0
    assert row.avg_wall_clock_s == 2.5


def _row_for_new_fields(**over):
    """A minimal valid ResultRow; override any field via kwargs."""
    base = dict(
        run_id="t",
        api="lightweight",
        fn="rst_slope",
        category="terrain",
        mode="spark-path",
        tile_px=64,
        bands=1,
        dtype="float32",
        srid=4326,
        rows=10,
        nodata_frac=0.0,
        warmup_iters=1,
        measured_iters=2,
        iter_median_s=0.1,
        iter_min_s=0.09,
        iter_p90_s=0.11,
        throughput_mpix_s=1.0,
        throughput_rows_s=1.0,
        peak_rss_mb=1.0,
        status="ok",
        note="",
        env_arch="x86_64",
        env_cpu_model="m",
        env_cpu_count=4,
        env_os="linux",
        env_gbx_version="0.5.0",
        env_gdal_version="3.11",
        env_runtime_version="18",
        env_where="venv",
    )
    base.update(over)
    return r.ResultRow(**base)


def test_resultrow_new_fields_default_and_roundtrip(tmp_path):
    row = _row_for_new_fields()
    assert row.input_tile == "materialized"
    assert row.output_disposition == "na"
    row2 = dataclasses.replace(row, input_tile="virtual", output_disposition="deferred")
    p = tmp_path / "x.jsonl"
    r.write_jsonl([row2], p)
    back = r.read_jsonl(p)[0]
    assert back.input_tile == "virtual"
    assert back.output_disposition == "deferred"


def test_resultrow_loads_legacy_row_without_new_fields(tmp_path):
    d = dataclasses.asdict(_row_for_new_fields())
    d.pop("input_tile")
    d.pop("output_disposition")
    p = tmp_path / "legacy.jsonl"
    p.write_text(json.dumps(d) + "\n")
    row = r.read_jsonl(p)[0]
    assert row.input_tile == "materialized"
    assert row.output_disposition == "na"


def test_summarize_surfaces_disposition_and_anomalies():
    from databricks.labs.gbx.bench.results import summarize

    rows = [
        _row_for_new_fields(
            fn="rst_setsrid", input_tile="virtual", output_disposition="deferred"
        ),
        _row_for_new_fields(
            fn="rst_slope", input_tile="virtual", output_disposition="materialized"
        ),
        _row_for_new_fields(
            fn="rst_clip",
            input_tile="virtual",
            output_disposition="na",
            status="error",
            note="boom",
        ),
    ]
    md = summarize(rows)
    assert "disposition" in md.lower()
    assert "deferred" in md and "materialized" in md
    assert "rst_clip" in md  # anomaly surfaced
