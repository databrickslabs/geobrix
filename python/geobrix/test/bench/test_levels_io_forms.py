"""The IO forms of the levels leg (read paths and writers): their shape, the scratch guard, and a local probe."""

import os

import numpy as np
import pytest

from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import preflight


def _corpus(tmp_path, n=4):
    import rasterio
    from rasterio.transform import from_origin

    rows = tmp_path / "corpus" / "rows"
    rows.mkdir(parents=True)
    for i in range(n):
        with rasterio.open(
            rows / f"t{i}.tif",
            "w",
            driver="GTiff",
            width=16,
            height=16,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(i * 16, 16, 1, 1),
        ) as dst:
            dst.write(np.full((1, 16, 16), i, dtype="float32"))
    return str(tmp_path / "corpus")


def test_the_io_set_holds_the_reads_the_udf_the_floor_and_the_three_writers():
    forms = {f.variant: f for f in lf.FORMS["IO"]}
    assert set(forms) == {
        "a",
        "b",
        "c",
        "udf",
        "d-py",
        "d-rs",
        "floor",
        "W0",
        "W1",
        "W2",
    }
    # the floor sits beside the d forms: `summarize` keys floor_median_s on (fn, level, rows)
    assert (forms["floor"].fn, forms["floor"].level) == (
        forms["d-py"].fn,
        forms["d-py"].level,
    )
    assert {f.input for f in lf.FORMS["IO"]} == {"io_rows"}
    assert [lf.BASE_ROWS["io_rows"] * k for k in lf.SCALES["IO"]] == [1000, 4000, 10000]
    for f in lf.FORMS["IO"]:  # every form resolves to a function of this module
        assert callable(getattr(lf, f.run.rpartition(":")[2])), f.variant
        assert not f.setup or callable(
            getattr(lf, f.setup.rpartition(":")[2])
        ), f.variant


def test_a_writer_form_deletes_only_a_strict_child_of_the_runs_scratch(tmp_path):
    scratch = lf._io_scratch(
        str(tmp_path / "vol" / "data" / "corpus")
    )  # a corpus outside geobrix/0.6.0
    out = os.path.join(scratch, "io_1")
    os.makedirs(out)
    open(os.path.join(out, "f.tif"), "wb").write(b"x")
    lf.remove_scratch(out, scratch)
    assert not os.path.exists(out)
    sibling = tmp_path / "corpus"
    sibling.mkdir()
    for bad in (
        scratch,
        str(sibling),
        os.path.join(scratch, "..", "corpus"),
        os.path.join(scratch, ".."),
    ):
        with pytest.raises(ValueError, match="refusing to delete"):
            lf.remove_scratch(bad, scratch)
    assert sibling.exists()


def test_the_scratch_directory_is_beside_the_corpus_on_a_volume_and_in_temp_elsewhere():
    assert (
        lf._io_scratch("/Volumes/c/s/v/data/corpus/") == "/Volumes/c/s/v/data/scratch"
    )  # wherever the corpus is
    assert lf._io_scratch("/tmp/x/corpus").endswith(lf.SCRATCH_PREFIX.rstrip("/"))


def test_each_io_form_installs_the_level_it_measures(monkeypatch):
    """c (L1), udf (L3), W2 (L1) in the order of the set: W2 must not time the L3 UDF the `udf` form left installed."""
    import databricks.labs.gbx as gbx
    from databricks.labs.gbx.bench import levels

    installed = []
    monkeypatch.setattr(
        gbx,
        "register",
        lambda spark, only=None, _levels=None: installed.append(_levels[lf.IO_FN][0]),
    )
    ctx = levels._Ctx(None, 0, 1, "", None, None, {})
    by_variant = {f.variant: f for f in lf.FORMS["IO"]}
    seen = []
    for v in ("c", "udf", "W2", "W2"):
        levels._ensure_setup(ctx, by_variant[v])
        seen.append(ctx.registered[lf.IO_FN][0])
    assert seen == ["L1", "L3", "L1", "L1"] and installed == [
        "L1",
        "L3",
        "L1",
    ]  # the repeat installs nothing


def test_an_explicit_default_looking_ladder_is_kept_for_a_set_with_its_own():
    from databricks.labs.gbx.bench import levels

    assert levels.ladder("IO") == lf.IO_SCALES and levels.ladder("H") == (1, 4, 16)
    assert levels.ladder("IO", (1, 4, 16)) == (1, 4, 16)  # asked for, not the default


def test_the_light_io_forms_run_on_a_local_corpus(spark_session_local, tmp_path):
    """b, d-py, the row-wise UDF and the light writer W1 on four tiles; the JVM forms are skipped without the JAR."""
    corpus = _corpus(tmp_path)
    failures, skipped = preflight.probe_failures(
        spark_session_local, "IO", corpus, rows=4, forms=("b", "d-py", "udf", "W1")
    )
    assert failures == [], failures
    assert not os.path.exists(lf._io_scratch(corpus)) or not os.listdir(
        lf._io_scratch(corpus)
    )


def test_the_io_floor_is_the_d_py_shell_with_the_decode_skipped(
    spark_session_local, tmp_path
):
    """The floor reads the same bytes-free virtual tiles through the same `mapInArrow` shell as d-py, and keeps them
    virtual: no raster is decoded, so its median is the cost of the shell alone."""
    corpus = _corpus(tmp_path)
    rows = (
        lf._io_read(spark_session_local, corpus + "/rows", 4, "floor")
        .select("tile")
        .collect()
    )
    assert len(rows) == 4
    assert all(r["tile"]["raster"] is None and r["tile"]["path"] for r in rows)
    decoded = (
        lf._io_read(spark_session_local, corpus + "/rows", 4, "d-py")
        .select("tile")
        .collect()
    )
    assert all(
        r["tile"]["raster"] is not None for r in decoded
    )  # d-py does decode the same tiles


def test_the_io_floor_is_recorded_as_the_floor_and_reported_beside_d_py(
    spark_session_local, tmp_path
):
    """The floor row is `ok` (its rasters are NULL by design, not an error) and d-py's line carries its median."""
    from databricks.labs.gbx.bench import levels

    corpus = _corpus(tmp_path)
    got = levels.run_mvp(
        spark_session_local, "IO", "io-floor", 0, 1, corpus=corpus, schema=None, scales=(1,), base_rows=4,
        where="test", only_levels=("DF",),
    )  # fmt: skip
    by_form = {r.form: r for r in got}
    assert by_form["floor"].status == "ok", (
        by_form["floor"].failure_mode,
        by_form["floor"].note,
    )
    assert "NULL" not in (by_form["floor"].note or "")
    assert by_form["d-py"].status == "ok"
    lines = levels.summarize(got).splitlines()
    cells = {
        ln.split("|")[3].strip(): [c.strip() for c in ln.split("|")]
        for ln in lines
        if "| read | DF |" in ln
    }
    assert cells["d-py"][7] != "-"  # floor_median_s
    assert cells["floor"][7] == "-"
