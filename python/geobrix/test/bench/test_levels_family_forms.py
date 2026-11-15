"""The family sets of the levels leg: one FORMS and one SETS entry per manifest family with more than one level."""

import os
from test._volume_safe import open_raster

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
    from rasterio.transform import from_origin

    rows = tmp_path / "corpus" / "rows"
    rows.mkdir(parents=True)
    for i in range(n):
        with open_raster(
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
        # a representative with a decoder reports the fingerprint of its decoded values (a failed pass is only a note)
        decoded = {r.fn for r in fam.REPS if r.vfp}
        not_value = [
            (r.fn, r.form, r.failure_mode)
            for r in rows
            if r.fn in decoded
            and not r.form.startswith("floor")
            and not r.output_fingerprint.startswith("v1:")
        ]
        assert not not_value, (family, not_value)
        ran.append(family)
    assert len(ran) >= 10, ran


# ---- the one-worker readdir probe twins of the JVM forms

from databricks.labs.gbx.bench import levels_probe as probe  # noqa: E402
from databricks.labs.gbx.bench import preflight  # noqa: E402


def _jvm_tile_forms():
    # the JV set (`bench.levels_jvm`) is installed after the twins: its accessors are timed against light, not probed
    return [
        (family, f)
        for family, forms in lf.FORMS.items()
        if family != "JV"
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


@pytest.mark.parametrize("held", [None, "NO"], ids=["unset", "user-set"])
def test_the_empty_dir_twin_puts_the_readdir_mode_back_after_its_open(
    spark_session_local, monkeypatch, held
):
    """The `-empty` twin's EMPTY_DIR is for its own open only. Left in the worker's environment, every later open in
    that reused Python worker skipped its sidecars: the `.aux.xml`, `.hdr` and `.prj` of the next rasters read (CRS and
    NoData gone, an EHdr not recognized)."""
    from databricks.labs.gbx.pyrx import functions as prx

    if held is None:
        monkeypatch.delenv(probe.READDIR_ENV, raising=False)
    else:
        monkeypatch.setenv(probe.READDIR_ENV, held)
    seen = []
    monkeypatch.setattr(
        prx,
        "_fromfile_impl",
        lambda *a, **k: seen.append(os.environ.get(probe.READDIR_ENV)),
    )
    probe._open_tile(True, 1 << 20).func("/x.tif")
    assert seen == ["EMPTY_DIR"]
    assert os.environ.get(probe.READDIR_ENV) == held
    probe._open_tile(False, 1 << 20).func("/x.tif")
    assert seen[-1] == held  # the plain twin opens under the setting it finds


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


def _systems_corpus(tmp_path, n):
    """`n` tiles cycling the generator's four systems (4326, 3857, 32618 are one place, 27700 another)."""
    from databricks.labs.gbx.bench import oracles

    rows = tmp_path / "corpus" / "rows"
    rows.mkdir(parents=True)
    systems = ["EPSG:4326", "EPSG:3857", "EPSG:32618", "EPSG:27700"]
    for i in range(n):
        (rows / f"r{i:04d}.tif").write_bytes(
            oracles._tile_bytes(systems[i % 4], 16, 16)
        )
    return str(tmp_path / "corpus")


def _crs_of(path):
    from databricks.labs.gbx._gdal_policy import open_data

    with open_data(path) as ds:
        return ds.crs.to_epsg()


def test_the_virtual_merge_inputs_are_the_same_plans_as_references(
    spark_session_local, tmp_path
):
    """`tiles_grouped_virt` (and `-skew`) hold the groups of `tiles_grouped` as virtual tiles: a path each, no bytes."""
    corpus = _systems_corpus(tmp_path, 128)
    spark = spark_session_local
    for virt, mat_plan, skew in (
        ("tiles_grouped_virt", "even", False),
        ("tiles_grouped_virt_skew", "skew", True),
    ):
        got = spark.table(lf.INPUTS[virt](spark, 24, corpus, None))
        rows = got.select("key", "tile.path", "tile.raster").collect()
        assert len(rows) == 24 and all(r.path and r.raster is None for r in rows)
        planned = lf._plan_grouped_tiles(lf._corpus_rows(corpus), 24, skew, spark)
        assert sorted((r.key, r.path) for r in rows) == sorted(planned), mat_plan
    # the table is spread, not one file read by one task
    assert got.rdd.getNumPartitions() > 1


def test_the_stack_input_is_one_key_of_n_tiles_of_one_crs(
    spark_session_local, tmp_path
):
    corpus = _systems_corpus(tmp_path, 128)
    got = spark_session_local.table(
        lf.INPUTS["tiles_stack_virt"](spark_session_local, 20, corpus, None)
    )
    rows = got.select("key", "tile.path").collect()
    assert {r.key for r in rows} == {"stack"} and len({r.path for r in rows}) == 20
    assert len({_crs_of(r.path) for r in rows}) == 1
    # a corpus with fewer tiles of one CRS than the depth asked for names the generator, as the other inputs do
    with pytest.raises(ValueError, match="one footprint.*gen-data"):
        lf.INPUTS["tiles_stack_virt"](spark_session_local, 40, corpus, None)


def test_the_mixed_crs_input_has_groups_of_several_systems_of_one_place(
    spark_session_local, tmp_path
):
    corpus = _systems_corpus(tmp_path, 128)
    got = spark_session_local.table(
        lf.INPUTS["tiles_mixed_crs_virt"](spark_session_local, 60, corpus, None)
    )
    by_key = {}
    for r in got.select("key", "tile.path").collect():
        by_key.setdefault(r.key, set()).add(_crs_of(r.path))
    assert len(by_key) == 2  # 60 tiles in groups of up to 50
    assert all(len(systems) >= 2 for systems in by_key.values()), by_key
    assert 27700 not in set().union(*by_key.values())  # the other place stays out


def test_the_planners_are_part_of_the_stamp_of_their_builders_only(monkeypatch):
    """A changed stack planner is another table for the stack input, and leaves the stamp of every other input (the
    materialized groups took minutes to build) alone."""
    before = {
        k: lf.input_stamp(lf._source_of(b))
        for k, b in lf.INPUTS.items()
        if k in ("tiles_stack_virt", "tiles_grouped", "tiles_grouped_virt")
    }

    def other(sources, n, spark=None):
        return []

    monkeypatch.setattr(lf, "_plan_stack_tiles", other)
    after = {k: lf.input_stamp(lf._source_of(lf.INPUTS[k])) for k in before}
    assert after["tiles_stack_virt"] != before["tiles_stack_virt"]
    assert after["tiles_grouped"] == before["tiles_grouped"]


def test_the_virtual_merge_forms_run_beside_the_byte_ones_which_stay_the_cliff():
    a = {
        f.variant: f
        for f in lf.FORMS["A"]
        if f.fn == "gbx_rst_merge_agg" and f.level == "L3"
    }
    assert a["c0"].input == "tiles_grouped"  # the bytes form: the documented cliff
    for shape in ("c0", "finalize", "tvf"):
        for sfx, inp in (
            ("-virt", "tiles_grouped_virt"),
            ("-virt-skew", "tiles_grouped_virt_skew"),
        ):
            assert a[shape + sfx].input == inp
            assert a[shape + sfx].computes == a[shape].computes  # tvf: no Serverless
    h = {f.variant: f for f in lf.FORMS["H"] if f.fn == "merge_by_key"}
    assert h["h_merge_fold"].input == "tiles_grouped_mat"  # bytes
    virtual = {
        "h_merge_vrt", "h_merge_fold_virt", "h_merge_fold_virt_np8", "h_merge_fold_virt_npkeys",
    }  # fmt: skip
    assert {h[v].input for v in virtual} == {"tiles_grouped_family_virt"}
    assert (h["h_merge_stack"].input, h["h_merge_mixed_crs"].input) == (
        "tiles_stack_virt",
        "tiles_mixed_crs_virt",
    )
    # the family sets read the 250-tile version of the group plan
    fam_in = {f.variant: f.input for f in lf.FORMS["raster-aggregate"]}
    assert fam_in["c0-virt"] == fam_in["finalize-virt"] == "tiles_grouped_family_virt"
    assert (
        lf.BASE_ROWS["tiles_grouped_family_virt"] == 250
        and lf.BASE_ROWS["tiles_grouped_virt"] == 2000
    )


def test_the_np_sweep_forms_pass_the_partition_counts_to_the_merge(monkeypatch):
    """default / 8 / one per key: the three forms differ only in `num_partitions`."""
    from databricks.labs.gbx.rasterx import helpers

    seen = {}

    class Frame:
        def select(self, *a):
            return self

        def distinct(self):
            return self

        def count(self):
            return 5

    spark = type("S", (), {"table": lambda self, src: Frame()})()
    monkeypatch.setattr(
        helpers,
        "merge_by_key",
        lambda df, keys, method="fold", num_partitions=None, **k: seen.setdefault(
            "calls", []
        ).append((method, num_partitions))
        or object(),
    )
    monkeypatch.setattr(lf, "_stats", lambda df, expr: (0, 0, 0, df))
    for fn in (
        lf.h_merge_fold_virt,
        lf.h_merge_fold_virt_np8,
        lf.h_merge_fold_virt_npkeys,
    ):
        fn(spark, "t")
    assert seen["calls"] == [("fold", None), ("fold", 8), ("fold", 5)]


def test_the_virtual_merge_forms_run_end_to_end_and_say_their_task_count(
    spark_session_local, tmp_path, monkeypatch
):
    """Every new merge form runs on its input at a small scale: ok, n = the input rows (not the groups), the shuffle's
    partition count in the note, and the worker's peak memory read (the harness's own hook).
    """
    corpus = _systems_corpus(tmp_path, 128)
    wanted = {
        "h_merge_vrt", "h_merge_fold_virt", "h_merge_fold_virt_np8", "h_merge_fold_virt_npkeys",
        "h_merge_stack", "h_merge_mixed_crs",
    }  # fmt: skip
    monkeypatch.setitem(
        lf.FORMS, "H", [f for f in lf.FORMS["H"] if f.variant in wanted]
    )
    rows = levels.run_mvp(
        spark_session_local, "H", "t1", 0, 1, corpus=corpus, schema=None,
        scales=(1,), where="venv", base_rows=24, only_levels=("DF",),
    )  # fmt: skip
    assert {r.form for r in rows} == wanted
    bad = [(r.form, r.status, r.failure_mode, r.note) for r in rows if r.status != "ok"]
    assert bad == [], bad
    assert {r.rows for r in rows} == {24}
    assert not any("short input" in r.note for r in rows), [r.note for r in rows]
    by = {r.form: r for r in rows}
    assert "shuffle_parts=8" in by["h_merge_fold_virt_np8"].note
    assert (
        "shuffle_parts=64" in by["h_merge_fold_virt"].note
    )  # the default: the session's slots, 64 at least
    keys = by["h_merge_fold_virt_npkeys"].note
    assert "shuffle_parts=" in keys and "shuffle_parts=64" not in keys, keys


def test_the_virtual_aggregate_shapes_agree_on_the_groups_and_count_their_input_rows(
    spark_session_local, tmp_path, monkeypatch
):
    """`c0-virt`, `finalize-virt` and `tvf-virt` merge the same references: one fingerprint, n = the input rows."""
    corpus = _systems_corpus(tmp_path, 128)
    monkeypatch.setitem(
        lf.FORMS, "A", lf._a_light(lf._MA, lf._ma_shapes(), (("tiles_grouped_virt", "-virt"),))
    )  # fmt: skip
    rows = levels.run_mvp(
        spark_session_local, "A", "t1", 0, 1, corpus=corpus, schema=None,
        scales=(1,), where="venv", base_rows=24, only_levels=("L3",),
    )  # fmt: skip
    assert sorted(r.form for r in rows) == ["c0-virt", "finalize-virt", "tvf-virt"]
    bad = [(r.form, r.status, r.failure_mode) for r in rows if r.status != "ok"]
    assert bad == [], bad
    assert {r.rows for r in rows} == {24}
    assert len({r.output_fingerprint for r in rows}) == 1, [
        (r.form, r.output_fingerprint) for r in rows
    ]


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


# ---- --levels-forms picks a form by variant or by <function>:<variant>


def test_a_bare_variant_of_two_functions_picks_both_and_a_qualified_name_picks_one():
    both = {f.fn for f in lf.select_forms("A", ("c0",))}
    assert {"gbx_rst_merge_agg", "gbx_rst_bng_rasterize_agg"} <= both
    one = lf.select_forms("A", ("gbx_rst_merge_agg:c0",))
    assert [(f.fn, f.variant) for f in one] == [("gbx_rst_merge_agg", "c0")]
    mixed = lf.select_forms(
        "A", ("gbx_rst_merge_agg:c0", "gbx_rst_bng_rasterize_agg:tvf")
    )
    assert sorted((f.fn, f.variant) for f in mixed) == [
        ("gbx_rst_bng_rasterize_agg", "tvf"),
        ("gbx_rst_merge_agg", "c0"),
    ]


def test_only_a_bare_variant_of_several_functions_is_ambiguous_and_lists_the_qualified_names():
    amb = lf.ambiguous_form_names("A", ("c0", "gbx_rst_merge_agg:c0", "no-such-form"))
    assert list(amb) == ["c0"]
    assert amb["c0"] == ["gbx_rst_bng_rasterize_agg:c0", "gbx_rst_merge_agg:c0"]
    unique = next(
        f
        for f in lf.FORMS["A"]
        if sum(g.variant == f.variant for g in lf.FORMS["A"]) == 1
    )
    assert lf.ambiguous_form_names("A", (unique.variant, lf.form_id(unique))) == {}


def test_the_preflight_knows_a_qualified_name_and_judges_only_the_forms_it_names():
    from databricks.labs.gbx.bench import preflight

    assert preflight.unknown_form_names("A", ("gbx_rst_merge_agg:c0", "c0")) == []
    assert preflight.unknown_form_names(
        "A", ("gbx_rst_nope:c0", "gbx_rst_merge_agg:nope")
    ) == [
        "gbx_rst_nope:c0",
        "gbx_rst_merge_agg:nope",
    ]
    got = preflight.forms_of("A", ("gbx_rst_merge_agg:c0",))
    assert [(f.fn, f.variant) for f in got] == [("gbx_rst_merge_agg", "c0")]


def test_a_stack_is_anchored_on_a_tile_that_has_a_crs(monkeypatch):
    """The first tile's header gave no EPSG: the stack was then every tile without one (or none at all) instead of the
    first system a tile has."""
    epsgs = [None, None, 4326, 3857, 4326, 4326, None, 4326]
    paths = [f"/c/t{i}.tif" for i in range(len(epsgs))]
    monkeypatch.setattr(lf, "corpus_guard", lambda *a, **k: None)
    monkeypatch.setattr(
        lf, "_epsgs", lambda spark, chunk: [epsgs[paths.index(p)] for p in chunk]
    )
    got = lf._plan_stack_tiles(paths, 3)
    assert got == [
        ("stack", "/c/t2.tif"),
        ("stack", "/c/t4.tif"),
        ("stack", "/c/t5.tif"),
    ]
    # no tile with a CRS at all: refused, naming why (not an empty or an all-None stack)
    epsgs[:] = [None] * len(epsgs)
    with pytest.raises(ValueError, match="needs tiles with a CRS"):
        lf._plan_stack_tiles(paths, 3)


def test_a_virtual_tile_table_is_written_in_files_of_two_rows(tmp_path, monkeypatch):
    """Optimized write packed 1000 virtual tiles (13 KB) into ONE file; a scan task then held 12 GB of outputs. The table of
    virtual tiles passes the family cap (2) as its rows per file, and the writer turns it into maxRecordsPerFile.
    """
    from unittest import mock

    assert fam.FAMILY_ROWS_PER_PARTITION == 2
    seen = {}
    monkeypatch.setattr(
        lf, "_table", lambda spark, schema, label, build, **kw: seen.update(kw) or label
    )
    rows = tmp_path / "rows"
    rows.mkdir()
    for i in range(4):
        (rows / f"t{i}.tif").write_bytes(b"")
    lf._tiles(True)(mock.MagicMock(), 4, str(tmp_path), "s")
    assert seen["per_file"] == 2
    seen.clear()
    lf._tiles(False)(mock.MagicMock(), 4, str(tmp_path), "s")
    assert seen["per_file"] is None  # a materialized tile is a file of its own already

    df = mock.MagicMock()
    spark = mock.MagicMock()
    lf._write_input(spark, df, "s.t", rows=1000, per_file=2)
    df.write.format.return_value.option.assert_called_once_with("maxRecordsPerFile", 2)


# ---- a floor reads the input of the forms it is the floor of


def _reused_with_floors():
    return [
        (family, fn)
        for family, floors in fam._REUSED_FLOORS.items()
        for fn, _, _ in floors
    ]


@pytest.mark.parametrize("family,fn", _reused_with_floors())
def test_every_virtual_input_of_a_function_has_a_floor_over_that_same_input(family, fn):
    """Plan shape: a floor never builds an input the forms it is the floor of do not read. A run of the virtual forms alone
    (`--levels-forms virtual,floor@virt`) needs the virtual input and no materialized tile.
    """
    forms = [f for f in lf.FORMS[family] if f.fn == fn]
    floors = {f.input for f in forms if f.variant.startswith("floor")}
    virtual = {
        f.input
        for f in forms
        if "virt" in f.input and "skew" not in f.input and not f.variant.startswith("floor")
    }  # fmt: skip
    assert virtual <= floors, (family, fn, virtual - floors)
    # a floor is a scan of the input it names, and one floor per input
    names = [f.variant for f in forms if f.variant.startswith("floor")]
    assert len(names) == len(set(names)) == len(floors), names


def test_the_virtual_tile_op_forms_and_their_floor_read_one_input_and_the_mat_floor_stays():
    forms = {f.variant: f for f in lf.FORMS["raster-tile-op"]}
    virt = [f for f in forms.values() if "virtual" in f.variant]
    assert virt and {f.input for f in virt} == {"tiles_virt"}
    assert forms["floor@virt"].input == "tiles_virt"
    assert forms["floor"].input == "tiles_n"  # the floor of the materialized forms
    picked = lf.select_forms("raster-tile-op", ["virtual", "floor@virt"])
    assert {f.input for f in picked} == {
        "tiles_virt"
    }  # nothing else is built for this part


def test_a_virtual_form_is_compared_with_the_floor_over_its_own_input():
    def row(form, med):
        return levels._row(
            dict(
                run_id="r", api="lightweight", fn="gbx_rst_transformcrs", category="levels", mode="spark-path",
                rows=4, level="L3", form=form, compute="dedicated", env_arch="", env_cpu_model="", env_cpu_count=0,
                env_os="", env_gbx_version="", env_gdal_version="", env_runtime_version="", env_where="t",
            ),
            status="ok",
            timing={"iter_median_ms": med * 1000.0},
        )  # fmt: skip

    table = levels.summarize(
        [row("-", 10.0), row("virtual", 3.0), row("floor", 1.0), row("floor@virt", 0.5)]
    )
    cells = {
        ln.split("|")[3].strip(): [c.strip() for c in ln.split("|")]
        for ln in table.splitlines()
        if "| L3 |" in ln
    }
    assert cells["-"][7] == "1.000" and cells["virtual"][7] == "0.500"
    assert cells["floor"][7] == cells["floor@virt"][7] == "-"


@pytest.mark.parametrize("family,fn", _reused_with_floors())
def test_a_floor_over_virtual_tiles_scans_their_paths_not_their_bytes(family, fn):
    """A virtual tile holds no bytes (`raster` is NULL, `path` is set): a scan that sums `length(tile.raster)` has a NULL
    fingerprint there and the form fails with "the form produced no output"."""
    for f in (f for f in lf.FORMS[family] if f.fn == fn and f.variant == "floor@virt"):
        assert "tile.path" in f.sql and "tile.raster" not in f.sql, f.sql
