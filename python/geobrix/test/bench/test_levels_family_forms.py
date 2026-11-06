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
