"""The family sets of the levels leg: one FORMS and one SETS entry per manifest family with more than one level."""

import numpy as np
import pytest

from databricks.labs.gbx.bench import levels
from databricks.labs.gbx.bench import levels_family as fam
from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import local_timing as lt
from databricks.labs.gbx.manifest import load

# the families whose functions have both a JVM level and a light level (what "more than one level" means for a set)
MULTI = sorted(
    {
        f.family
        for f in load().functions.values()
        if "L1" in f.levels and "L3" in f.levels
    }
    - {fam.WEB_TILES_PENDING}  # no set yet (see the module doc of levels_family)
)


def _chosen(spec):
    return sorted(
        (
            f
            for f in lf.FORMS[spec.mvp]
            if f.variant in spec.forms and (spec.fn is None or f.fn == spec.fn)
        ),
        key=lambda f: spec.forms.index(f.variant),
    )


def _corpus(tmp_path, n):
    import rasterio
    from rasterio.transform import from_origin

    rows = tmp_path / "corpus" / "rows"
    rows.mkdir(parents=True)
    for i in range(n):
        with rasterio.open(
            rows / f"t{i}.tif", "w", driver="GTiff", width=16, height=16, count=1,
            dtype="float32", crs="EPSG:4326", transform=from_origin(i * 16, 16, 1, 1),
        ) as dst:  # fmt: skip
            dst.write(np.full((1, 16, 16), i, dtype="float32"))
    return str(tmp_path / "corpus")


def test_every_multi_level_family_has_its_forms_and_its_local_set():
    assert "raster-to-grid" not in MULTI  # light only: no level to compare
    for family in MULTI:
        assert lf.FORMS[family], family
        assert family in lt.SETS, family


@pytest.mark.parametrize("family", MULTI)
def test_a_set_names_its_forms_by_full_variant_and_ends_on_one_floor(family):
    spec = lt.SETS[family]
    got = _chosen(spec)
    assert [f.variant for f in got] == list(
        spec.forms
    ), "every named form exists once (a variant shared by two forms of the function overwrites)"
    assert spec.forms[-1].split("@")[0] == "floor" or spec.forms[-1] == "floor"
    assert sum(f.variant.split("@")[0] == "floor" for f in got) == 1
    # comparable by construction: one input for every form of the set
    assert len({f.input for f in got}) == 1, [(f.variant, f.input) for f in got]
    assert spec.reference in spec.forms and list(spec.ns) == sorted(spec.ns)


def test_the_tile_input_is_n_rows_in_a_cap_of_tiles_per_partition(
    spark_session_local, tmp_path
):
    """Every tile family reads the first n tiles over `io_partitions` partitions, never more than the cap in one."""
    corpus = _corpus(tmp_path, 40)
    n = 20
    src = lf.INPUTS["tiles_n"](spark_session_local, n, corpus, None)
    df = spark_session_local.table(src)
    from pyspark.sql import functions as F

    sizes = [
        r[0]
        for r in df.groupBy(F.spark_partition_id()).count().select("count").collect()
    ]
    assert sum(sizes) == n and max(sizes) <= fam.FAMILY_ROWS_PER_PARTITION, sizes
    assert df.columns == ["tile"]


def test_the_crs_of_a_long_corpus_listing_is_read_by_tasks_in_order(
    spark_session_local, tmp_path
):
    """Past a handful of files the header reads run on the cluster; the answer is the same list, in the listing's order."""
    corpus = _corpus(tmp_path, lf.SMALL_INPUT_ROWS + 6)
    paths = lf._corpus_rows(corpus)
    assert len(paths) > lf.SMALL_INPUT_ROWS
    assert lf._epsgs(spark_session_local, paths) == [lf._epsg_of(p) for p in paths]
    assert set(lf._epsgs(spark_session_local, paths)) == {4326}


def test_the_grouped_tile_input_is_spread_not_read_by_a_task_or_two(
    spark_session_local, tmp_path
):
    """The path rows of a grouped (or BNG) tile input are a few KB: left alone they sit in one or two partitions, and
    one task reads, holds and writes every tile (a merge or tessellate leg then waits on that task for the whole cap).
    """
    from pyspark.sql import functions as F

    corpus = _corpus(tmp_path, 40)
    n = 20
    src = lf.INPUTS["tiles_grouped"](spark_session_local, n, corpus, None)
    df = spark_session_local.table(src)
    sizes = [
        r[0]
        for r in df.groupBy(F.spark_partition_id()).count().select("count").collect()
    ]
    assert sum(sizes) == n and max(sizes) <= fam.FAMILY_ROWS_PER_PARTITION, sizes


def test_the_combine_input_members_are_written_by_tasks_not_the_driver(
    spark_session_local, tmp_path, monkeypatch
):
    """The by-key helpers' groups (a source tile and the members the recipe writes beside it) were written one after
    another on the driver (540-825 s at 1x). The driver now plans only (key, source, directory) rows: it never calls the
    synthesiser, and the members land in the table spread over many tasks, every group intact.
    """
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import synth

    def driver_must_not_synthesize(*a, **k):
        raise AssertionError("the driver wrote group members")

    monkeypatch.setattr(synth, "synthesize", driver_must_not_synthesize)
    corpus = _corpus(tmp_path, 40)
    n = 20
    src = lf.INPUTS["tiles_grouped_mat"](spark_session_local, n, corpus, None)
    df = spark_session_local.table(src)
    assert df.columns == ["key", "tile"]
    per_group = [r[0] for r in df.groupBy("key").count().select("count").collect()]
    assert len(per_group) == n and set(per_group) == {2}, per_group
    sizes = [
        r[0]
        for r in df.groupBy(F.spark_partition_id()).count().select("count").collect()
    ]
    assert max(sizes) < 2 * n and len(sizes) > 1, sizes


@pytest.mark.parametrize(
    "inp", ["tiles_n", "tiles_mat", "tiles_virt", "tiles_grouped", "file_bytes"]
)
def test_a_scale_over_the_corpus_is_an_error_naming_both_numbers_and_the_generator(
    spark_session_local, tmp_path, inp
):
    corpus = _corpus(tmp_path, 4)
    with pytest.raises(ValueError) as e:
        lf.INPUTS[inp](spark_session_local, 10, corpus, None)
    why = str(e.value)
    assert "10" in why and "4" in why, why
    assert "gbx:bench:gen-data --on-databricks --rows-from 4 --rows-to 9" in why, why
    assert "upload" not in why.replace("not uploaded", "")


def test_the_family_forms_run_at_the_light_level_and_report_a_fingerprint(
    spark_session_local, tmp_path
):
    """Every generated form of a family set runs at L3 on local Spark and reports a fingerprint (the floor too)."""
    corpus = _corpus(tmp_path, 12)
    spark = spark_session_local
    reps = {r.family: r for r in fam.REPS}
    ran = []
    for family in sorted(reps):
        forms = [
            f
            for f in lf.FORMS[family]
            if f.input in {r.inp for r in fam.REPS} and f.level == "L3"
        ]
        keep = [f for f in forms if "@" in f.variant]
        if not keep:
            continue
        from unittest import mock

        with mock.patch.dict(lf.FORMS, {family: keep}):
            rows = levels.run_mvp(
                spark, family, "t", 0, 1, corpus=corpus, schema=None, scales=(1,),
                where="local", base_rows=6,
            )  # fmt: skip
        bad = [
            (r.fn, r.form, r.status, r.failure_mode) for r in rows if r.status != "ok"
        ]
        assert not bad, (family, bad)
        assert all(r.output_fingerprint for r in rows), family
        ran.append(family)
    assert len(ran) >= 10, ran


# ---- the one-worker readdir probe twins of the JVM forms

from databricks.labs.gbx.bench import levels_probe as probe  # noqa: E402
from databricks.labs.gbx.bench import preflight  # noqa: E402


def _jvm_tile_forms():
    return [
        (family, f)
        for family, forms in lf.FORMS.items()
        for f in forms
        if f.level in ("L1", "L2") and f.input == "tiles_n" and f.sql and not f.opt_in
    ]


def test_every_jvm_tile_form_has_both_probe_twins_and_only_by_name():
    seen = set()
    jvm = []
    # one pair per (fn, variant): a variant shared by an L1 and an L2 form takes the L1
    for family, f in _jvm_tile_forms():
        if (family, f.fn, f.variant) not in seen:
            seen.add((family, f.fn, f.variant))
            jvm.append((family, f))
    assert jvm, "the family sets have JVM forms that read tiles"
    for family, f in jvm:
        twins = {t.variant: t for t in lf.FORMS[family] if t.opt_in and t.fn == f.fn}
        for suffix in ("probe-readdir", "probe-readdir-empty"):
            t = twins[f"{f.variant}@{suffix}"]
            assert t.input == "probe_paths" and t.level in ("L1", "L2") and t.run
            assert t.computes == ("dedicated",)
        # the default run skips them; naming them selects exactly them
        assert not [t for t in lf.select_forms(family) if t.opt_in]
        ids = [f"{f.variant}@probe-readdir", f"{f.variant}@probe-readdir-empty"]
        assert [t.variant for t in lf.select_forms(family, ids)] == ids or {
            t.variant for t in lf.select_forms(family, ids)
        } == set(ids)
        assert preflight.unknown_form_names(family, ids) == []
        assert not [t for t in preflight.forms_of(family) if t.opt_in]
    assert lf.BASE_ROWS["probe_paths"] == probe.PROBE_CALLS == 20


@pytest.mark.parametrize("empty", [False, True])
def test_a_probe_reads_twenty_corpus_tiles_on_one_task(
    spark_session_local, tmp_path, empty
):
    """The probe query reads `PROBE_CALLS` tiles on one partition; the -empty twin sets EMPTY_DIR on the worker."""
    corpus = _corpus(tmp_path, 24)
    spark = spark_session_local
    src = lf.INPUTS["probe_paths"](spark, probe.PROBE_CALLS, corpus, None)
    assert spark.table(src).rdd.getNumPartitions() == 1
    sql = "SELECT count(*) AS n, sum(length(tile.raster)) AS fp, count(tile) AS n_out FROM {src}"
    n, fp, n_out, _df = probe._probe_run(sql, empty, f"t_{empty}")(spark, src)
    assert (n, n_out) == (20, 20) and fp > 0


def test_a_probe_over_a_corpus_with_too_few_tiles_is_an_error_naming_both_numbers(
    spark_session_local, tmp_path
):
    corpus = _corpus(tmp_path, 5)
    with pytest.raises(Exception, match="5.*20|20.*5"):
        lf.INPUTS["probe_paths"](spark_session_local, 20, corpus, None)


def test_the_empty_dir_twin_sets_the_mode_in_the_worker_that_opens_the_tiles(
    spark_session_local, tmp_path
):
    """Spark runs two Python UDFs in separate workers unless it chains them into one evaluation node: the `-empty` twin
    sets `GDAL_DISABLE_READDIR_ON_OPEN` in a UDF and opens in `rst_fromfile`, so one evaluation node must hold both
    calls (a pandas UDF beside it runs in another worker), else the twin would time the same open as the plain one.
    """
    corpus = _corpus(tmp_path, 24)
    spark = spark_session_local
    src = lf.INPUTS["probe_paths"](spark, probe.PROBE_CALLS, corpus, None)
    sql = "SELECT count(*) AS n, sum(length(tile.raster)) AS fp, count(tile) AS n_out FROM {src}"
    *_, df = probe._probe_run(sql, True, "t_plan")(spark, src)
    plan = df._jdf.queryExecution().executedPlan().toString()
    nodes = [ln for ln in plan.splitlines() if "EvalPython" in ln]
    assert nodes and all(
        "open_tile" in ln for ln in nodes
    ), plan  # the open is in the UDF that sets the mode
    assert len({ln.strip(" +-") for ln in nodes}) == 1, plan  # one node, so one worker


def test_the_aggregate_set_keeps_its_2000_tile_input_beside_the_family_input():
    """The family sets read 250 grouped tiles through their own input; the A set's `tiles_grouped` (and the baselines
    stored against it) stay at 2000, and the two cached tables have different names.
    """
    assert lf.BASE_ROWS["tiles_grouped"] == 2000
    assert lf.BASE_ROWS["tiles_grouped_family"] == 250
    inputs = {
        f.input for f in lf.FORMS["raster-aggregate"] if f.fn == "gbx_rst_merge_agg"
    }
    assert "tiles_grouped_family" in inputs and "tiles_grouped" not in inputs
    a_inputs = {f.input for f in lf.FORMS["A"] if f.fn == "gbx_rst_merge_agg"}
    assert "tiles_grouped" in a_inputs and "tiles_grouped_family" not in a_inputs


def test_a_representative_the_manifest_lost_is_skipped_with_a_warning_not_a_crash(
    monkeypatch,
):
    """`install` runs when levels_forms is imported: a KeyError there broke every set, the preflight and the worker
    UDF imports. The manifest-pin test is what names the drift.
    """
    gone = fam.Rep(
        "raster-accessor", "gbx_rst_nope", "tiles_n", fam._SCALAR, "{fn}(tile)", "int"
    )
    monkeypatch.setattr(fam, "REPS", [gone] + fam.REPS)
    with pytest.warns(UserWarning, match="gbx_rst_nope"):
        got = fam.family_forms()
    assert "raster-accessor" in got
    assert all(f.fn != "gbx_rst_nope" for f in got["raster-accessor"])


def test_a_short_bng_corpus_names_the_bng_count_and_the_real_corpus_size(
    spark_session_local, tmp_path
):
    """The BNG tile input found 3 EPSG:27700 tiles in a corpus of 8: the error says that, and the generator range
    starts after the 8 tiles there are (not after the 3 BNG ones, which overlaps rows that exist).
    """
    from databricks.labs.gbx.bench import oracles

    rows = tmp_path / "rows"
    rows.mkdir()
    for i in range(8):
        crs = "EPSG:27700" if i % 3 == 0 else "EPSG:4326"
        (rows / f"r{i}.tif").write_bytes(oracles._tile_bytes(crs, 16, 16))
    with pytest.raises(ValueError) as e:
        lf.INPUTS["tiles_gb_mat"](spark_session_local, 10, str(tmp_path), None)
    why = str(e.value)
    assert "10" in why and "3" in why and "8 tiles" in why, why
    assert "--rows-from 8 " in why and "--rows-from 3" not in why, why


def test_the_bng_tile_input_reads_its_headers_in_tasks_and_keeps_exactly_n(
    spark_session_local, tmp_path, monkeypatch
):
    """Looking for the EPSG:27700 tiles opened up to 4n + 64 headers one after another on the driver (a Volume read
    each): setup of 261-474 s at 1x. The headers are read by tasks (the driver opens none), and the batch that
    finds the n-th tile may hold more than n: the input keeps n.
    """
    from databricks.labs.gbx.bench import oracles

    rows = tmp_path / "rows"
    rows.mkdir()
    systems = ["EPSG:27700", "EPSG:4326", "EPSG:3857", "EPSG:32618"]
    for i in range(128):
        (rows / f"r{i:03d}.tif").write_bytes(
            oracles._tile_bytes(systems[i % 4], 16, 16)
        )
    opened = []
    real = lf.open_data

    def spy(p):
        opened.append(p)
        return real(p)

    monkeypatch.setattr(lf, "open_data", spy)
    lf._GB_FOUND.clear()
    name = lf.INPUTS["tiles_gb_mat"](spark_session_local, 16, str(tmp_path), None)
    assert opened == [], "the driver opened a header"
    assert spark_session_local.table(name).count() == 16


def test_the_combineavg_group_is_the_source_twice_and_writes_nothing(
    spark_session_local, tmp_path
):
    """The recipe's two copies are the source's pixels on the source's grid, so the group is the source twice: the same
    combine work with no copy written to the Volume (they were tens of GB through FUSE, rebuilt per scale).
    """
    import numpy as np

    from databricks.labs.gbx._gdal_policy import open_data
    from databricks.labs.gbx.bench import synth

    corpus = _corpus(tmp_path, 12)
    src = lf._corpus_rows(corpus)[0]
    out = tmp_path / "synth_out"
    copies = synth.synthesize(src, "combineavg", str(out))
    with open_data(src) as a:
        for (
            c
        ) in copies:  # what the recipe wrote IS the source: pixels, grid, CRS, NoData
            with open_data(c) as b:
                assert np.array_equal(a.read(), b.read())
                assert (a.transform, a.crs, a.nodatavals) == (
                    b.transform,
                    b.crs,
                    b.nodatavals,
                )
    n = 6
    table = lf.INPUTS["tiles_aligned_mat"](spark_session_local, n, corpus, None)
    got = spark_session_local.table(table)
    assert got.count() == 2 * n
    assert {r[0] for r in got.groupBy("key").count().select("count").collect()} == {2}
    import os

    assert not os.path.isdir(os.path.join(corpus, "_synth", "combineavg"))
    assert not getattr(lf.INPUTS["tiles_aligned_mat"], "writes_under_corpus")


def test_a_synthesised_group_that_is_there_is_not_decoded_again(tmp_path, monkeypatch):
    """The check that the copies exist comes before the source is read (a whole tile decoded for nothing)."""
    from databricks.labs.gbx.bench import synth

    corpus = _corpus(tmp_path, 3)
    src = lf._corpus_rows(corpus)[0]
    for recipe in ("combineavg", "merge"):
        out = str(tmp_path / f"o_{recipe}")
        first = synth.synthesize(src, recipe, out)

        calls = []
        real = synth.open_data

        def spy(path, *a, **k):
            calls.append(str(path))
            return real(path, *a, **k)

        monkeypatch.setattr(synth, "open_data", spy)
        assert synth.synthesize(src, recipe, out) == first
        assert src not in calls, calls
        monkeypatch.undo()
