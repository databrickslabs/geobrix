"""The L1/L3 forms added for raster `bng_tessellate` and `contour` run at both levels.

L1 is the geobrix JAR, so the module skips when none is staged under ``python/geobrix/lib/``. Run in geobrix-dev
Docker, on its own (``spark.jars`` only applies to the first session of a process):

    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/bench/test_levels_family_l1_forms.py --with-integration
"""

import logging
from pathlib import Path
from unittest import mock

import pytest

from databricks.labs.gbx.bench import levels
from databricks.labs.gbx.bench import levels_forms as lf

pytestmark = pytest.mark.integration

_JARS = sorted(
    (Path(__file__).resolve().parents[2] / "lib").glob(
        "geobrix-*-jar-with-dependencies.jar"
    )
)

# (family, function, the forms to run: L1 first, then the L3 ones, then the floor)
_NEW = [
    (
        "raster-generator",
        "gbx_rst_bng_tessellate",
        ["h_tess_l1", "h_tess_udtf", "floor"],
    ),
    (
        "raster-vectorize",
        "gbx_rst_contour",
        ["l1@n", "c0@n", "floor@n"],
    ),
]


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    active = SparkSession.getActiveSession()
    if active is not None and str(_JARS[-1]) not in active.conf.get("spark.jars", ""):
        pytest.skip(
            "A JAR-free Spark session is already live in this process; run this file "
            "on its own with --with-integration"
        )
    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-bench-family-l1-forms")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session


def _corpus(tmp_path, n):
    """`n` tiles cycling four systems (every fourth is EPSG:27700, what the BNG tessellate input keeps)."""
    from databricks.labs.gbx.bench import oracles

    rows = tmp_path / "corpus" / "rows"
    rows.mkdir(parents=True)
    systems = ["EPSG:4326", "EPSG:3857", "EPSG:32618", "EPSG:27700"]
    for i in range(n):
        # the EPSG:27700 tiles sit in Britain (a tessellate of a tile outside it has no cells)
        gb = dict(origin=(400000.0 + 4000 * i, 300000.0), px=100.0)
        (rows / f"r{i:04d}.tif").write_bytes(
            oracles._tile_bytes(systems[i % 4], 32, 32, **(gb if i % 4 == 3 else {}))
        )
    return str(tmp_path / "corpus")


@pytest.mark.parametrize("family,fn,variants", _NEW, ids=[x[1] for x in _NEW])
def test_a_new_form_runs_at_its_level_and_reports_a_fingerprint(
    spark_with_jar, tmp_path, family, fn, variants
):
    """Each new form (L1, the L3 it is judged against, the floor) runs on local Spark: ok, a fingerprint, n_out == n."""
    corpus = _corpus(tmp_path, 40)
    forms = [f for f in lf.FORMS[family] if f.fn == fn and f.variant in variants]
    assert [f.variant for f in forms] == variants or sorted(
        f.variant for f in forms
    ) == sorted(variants)
    assert {f.level for f in forms} == {"L1", "L3"}
    with mock.patch.dict(lf.FORMS, {family: forms}):
        rows = levels.run_mvp(
            spark_with_jar, family, "t", 0, 1, corpus=corpus, schema=None,
            scales=(1,), where="local", base_rows=4,
        )  # fmt: skip
    got = {r.form: r for r in rows}
    assert set(got) == set(variants)
    bad = [(r.form, r.status, r.failure_mode, r.note) for r in rows if r.status != "ok"]
    assert not bad, bad
    assert all(r.output_fingerprint for r in rows)
    assert len({r.rows for r in rows}) == 1, [(r.form, r.rows) for r in rows]
