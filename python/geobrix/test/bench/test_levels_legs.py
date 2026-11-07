"""The levels leg's slow end-to-end tests: each times a family of forms on a local session.

Split from test_levels.py so CI can run them as a shard of their own.
"""

from databricks.labs.gbx import _rs
from databricks.labs.gbx.bench import levels
from databricks.labs.gbx.bench import levels_forms as lf

from .levels_helpers import RST, B, run_s


def test_levels_leg_times_the_towers_at_l3(spark_session_local, tmp_path, monkeypatch):
    """The towers input is a DSM of tiles under the corpus with the towers beside it. L3 runs it and
    fingerprints one row per tower.
    """
    spark = spark_session_local
    monkeypatch.setattr(lf, "_DSM_GRID", 2)
    monkeypatch.setattr(lf, "_DSM_TILE_PX", 64)
    monkeypatch.setattr(lf, "_TOWER_RADIUS", 20.0)
    monkeypatch.setitem(
        lf.FORMS, "H", [f for f in lf.FORMS["H"] if f.fn == "rst_viewshed_towers"]
    )
    rows = run_s(
        spark, tmp_path, scales=(1,), base_rows=6, mvp="H", only_levels=("L3",)
    )
    by = {r.level: r for r in rows}
    assert set(by) == {"L3"}
    assert (
        by["L3"].status == "ok" and by["L3"].rows == 6 and by["L3"].output_fingerprint
    )


def test_levels_leg_runs_mvp_s_light_forms(spark_session_local, tmp_path):
    spark = spark_session_local
    # 20k rows and up: Spark's sent-bytes counter stops growing past one 10k-row Arrow batch
    rows = run_s(spark, tmp_path, scales=(1, 4), base_rows=20000)
    assert rows and {r.level for r in rows} == {"L3"}
    assert all(r.status in ("ok", "na_by_design") for r in rows)
    ok = [r for r in rows if r.status == "ok"]
    assert all(r.iter_median_s > 0 and r.rows > 0 and r.output_fingerprint for r in ok)
    c0 = [r for r in ok if r.fn == B and r.form == "c0"]
    # at each scale c0, c1 and d compute the same cells (`floor` is a constant, not a variant);
    # `d` is a row that says "Rust kernels unavailable" when this host has no build
    no_rust = _rs.unavailable_reason() is not None
    cells = {}
    for r in ok:
        if r.fn == B and r.form != "floor":
            cells.setdefault(r.rows, {})[r.form] = r.output_fingerprint
    assert len(cells) == 2 and all(
        set(forms) == ({"c0", "c1"} if no_rust else {"c0", "c1", "d"})
        and len(set(forms.values())) == 1
        for forms in cells.values()
    )
    # No tile corpus here: the raster forms are skipped with a reason, not dropped.
    assert {r.fn for r in rows if r.status == "na_by_design"} == (
        {RST, B} if no_rust else {RST}
    )
    for r in ok:
        assert "attr=" in r.note and r.compute == "dedicated" and r.native_s is None
        if "attr=rest" in r.note:  # classic reads the Spark REST API
            assert r.py_eval_s > 0 and 0 < r.py_eval_frac <= 1.0
    # Spark counts no more bytes sent for 4x the DOUBLEs: NULL, and says so. WKT scales.
    for r in c0:
        assert r.ser_sent_bytes is None and "Spark under-counts numeric" in r.note
    assert all(
        r.ser_sent_bytes > 0
        for r in ok
        if r.fn == "gbx_st_makevalid" and "attr=rest" in r.note
    )
    # The floor form is the same Arrow shell with a constant body: its fingerprint is the
    # constant's, and its median sits beside each variant's in the table.
    h = spark.sql("SELECT pmod(xxhash64('TQ3080'), 2147483647)").first()[0]
    floor = {r.rows: r for r in ok if r.form == "floor"}
    assert len(floor) == 2 and all(
        r.output_fingerprint == str(n * h) for n, r in floor.items()
    )
    table = levels.summarize(rows)
    assert "input unavailable" in table  # a skipped form says why
    for r in c0:
        line = next(
            x for x in table.splitlines() if f"| c0 | dedicated | {r.rows} |" in x
        )
        assert f"| {floor[r.rows].iter_median_s:.3f} |" in line
    # a cached input is named by its recipe version
    assert spark.catalog.tableExists(f"mvp_in_bng_points_v{lf.RECIPE_VERSION}_20000")


def test_levels_leg_times_every_light_shape_of_the_generators(
    spark_session_local, tmp_path
):
    """The shapes of one generator are the same rows asked for in different ways: one fingerprint each."""
    from databricks.labs.gbx.bench import oracles

    (tmp_path / "rows").mkdir()
    for i in range(
        20
    ):  # 300 x 200 px: six 128-px windows each; as many as the scale asks for
        (tmp_path / "rows" / f"r{i}.tif").write_bytes(
            oracles._tile_bytes("EPSG:4326", 300, 200)
        )
    rows = run_s(spark_session_local, tmp_path, scales=(1,), base_rows=20, mvp="G")
    bad = [r for r in rows if r.status == "error"]
    assert bad == [], [(r.fn, r.form, r.failure_mode) for r in bad]
    ok = [r for r in rows if r.status == "ok"]
    retile = {r.form: r.output_fingerprint for r in ok if r.fn == "gbx_rst_retile"}
    assert set(retile) == {
        "udtf",
        "udtf-arrow",
        "array_inline",
        "count_part",
        "session-tvf",
    }
    # the tessellate generator and the array function give the same cells, so their shapes share one answer
    cells = {
        (r.fn, r.form): r.output_fingerprint
        for r in ok
        if r.fn in ("gbx_bng_tessellateexplode", "gbx_bng_tessellate")
    }
    assert len(cells) == 3 and len(set(cells.values())) == 1, cells
    assert len(set(retile.values())) == 1, retile
    # the big-tile shapes have no tiles here, and say so
    big = [r for r in rows if r.form.endswith("-big")]
    assert big and {r.status for r in big} == {"na_by_design"}


def test_levels_leg_times_every_light_shape_of_the_aggregates(
    spark_session_local, tmp_path, monkeypatch
):
    """The light shapes of one aggregate ask for the same groups in different ways: one fingerprint each (the
    tree step re-encodes its canvases, so it agrees on the groups only), and each is a form of the Python
    worker, which is how the runner comes to record its worker memory."""
    from databricks.labs.gbx.bench import oracles

    (tmp_path / "rows").mkdir()
    for i in range(
        400
    ):  # as many tiles as the scale asks for (a short corpus is refused)
        (tmp_path / "rows" / f"r{i}.tif").write_bytes(
            oracles._tile_bytes("EPSG:4326", 32, 32)
        )
    small = (
        530000.0,
        180000.0,
        532000.0,
        182000.0,
        2,
        2,
    )  # 2 x 2 pixels: 200 groups stay cheap
    monkeypatch.setitem(
        lf.FORMS,
        "A",
        lf._a_light(
            lf._MA,
            lf._ma_shapes(),
            (("tiles_grouped", ""), ("tiles_grouped_skew", "-skew")),
        )
        + lf._a_light(
            lf._BR,
            lf._br_shapes(small),
            (("bng_cells", ""), ("bng_cells_skew", "-skew")),
        ),
    )
    rows = run_s(spark_session_local, tmp_path, scales=(1,), base_rows=400, mvp="A")
    bad = [r for r in rows if r.status != "ok"]
    assert bad == [], [(r.fn, r.form, r.status, r.failure_mode, r.note) for r in bad]
    assert len(rows) == 14 and {r.level for r in rows} == {"L3"}
    # provenance: the merge aggregate records the path a capability chose (pandas by default), the BNG one chooses none,
    # and every row says which Spark it ran on
    assert {r.path for r in rows if r.fn == lf._MA} == {"merge_agg:pandas"}
    assert {r.path for r in rows if r.fn == lf._BR} == {""}
    assert {r.env_spark_version for r in rows} == {spark_session_local.version}
    assert all(  # off by default (the source is the client on a pyspark that has no Arrow aggregate)
        r.env_caps.startswith("{") and '"arrow_grouped_agg":"off:' in r.env_caps
        for r in rows
    )
    for fn in (lf._MA, lf._BR):
        for sfx in ("", "-skew"):
            mine = {
                r.form: r
                for r in rows
                if r.fn == fn and r.form.endswith("-skew") == bool(sfx)
            }
            prints = {
                f: r.output_fingerprint
                for f, r in mine.items()
                if not f.startswith("tree")
            }
            assert len(set(prints.values())) == 1, (fn, sfx, prints)
            assert len({r.rows for r in mine.values()}) == 1
