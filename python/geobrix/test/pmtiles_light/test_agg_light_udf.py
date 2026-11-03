"""Registered-UDF tests for the light gbx_pmtiles_agg (no JAR)."""

from pmtiles.reader import MmapSource, Reader
from pyspark.sql import functions as F

from databricks.labs.gbx.pmtiles import functions as pt
from databricks.labs.gbx.pmtiles._agg_light import register_pmtiles_agg

_MVT_A = b"mvt-a\x00\x01"
_MVT_B = b"mvt-b\x00\x02"


def _decode(blob, tmp_path):
    p = tmp_path / "r.pmtiles"
    p.write_bytes(blob)
    out = {}
    with open(p, "rb") as fh:
        r = Reader(MmapSource(fh))
        for z in range(0, 6):
            n = 2**z
            for x in range(n):
                for y in range(n):
                    t = r.get(z, x, y)
                    if t is not None:
                        out[(z, x, y)] = t
    return out


def _rows(spark):
    return spark.createDataFrame(
        [("grp", _MVT_A, 3, 2, 4), ("grp", _MVT_B, 3, 5, 6)],
        ["g", "tile", "z", "x", "y"],
    )


def test_wrapper_in_agg(spark, tmp_path):
    register_pmtiles_agg(spark)
    df = _rows(spark)
    out = df.groupBy("g").agg(pt.pmtiles_agg("tile", "z", "x", "y").alias("arc"))
    blob = out.collect()[0]["arc"]
    assert _decode(blob, tmp_path) == {(3, 2, 4): _MVT_A, (3, 5, 6): _MVT_B}


def test_sql_name_in_agg(spark, tmp_path):
    register_pmtiles_agg(spark)
    _rows(spark).createOrReplaceTempView("tiles_v")
    blob = spark.sql(
        "SELECT gbx_pmtiles_agg(tile, z, x, y) AS arc FROM tiles_v GROUP BY g"
    ).collect()[0]["arc"]
    assert _decode(blob, tmp_path) == {(3, 2, 4): _MVT_A, (3, 5, 6): _MVT_B}


def test_metadata_passthrough(spark, tmp_path):
    register_pmtiles_agg(spark)
    df = _rows(spark).withColumn("meta", F.lit('{"name": "demo"}'))
    out = df.groupBy("g").agg(
        # Bare str is a JSON literal in the wrapper contract; pass the column
        # explicitly to exercise per-group metadata passthrough.
        pt.pmtiles_agg("tile", "z", "x", "y", F.col("meta")).alias("arc")
    )
    blob = out.collect()[0]["arc"]
    p = tmp_path / "md.pmtiles"
    p.write_bytes(blob)
    with open(p, "rb") as fh:
        assert Reader(MmapSource(fh)).metadata().get("name") == "demo"


def _agg_registered(spark):
    names = {r.function for r in spark.sql("SHOW USER FUNCTIONS").collect()}
    return any(n.endswith("gbx_pmtiles_agg") for n in names)


def _drop_agg(spark):
    # The session-scoped spark fixture is shared; an earlier test may have
    # already registered gbx_pmtiles_agg. Drop it so this test actually
    # proves the tier's register() reinstalls it.
    spark.sql("DROP TEMPORARY FUNCTION IF EXISTS gbx_pmtiles_agg")


def test_pyrx_register_installs_pmtiles_agg(spark):
    from databricks.labs.gbx import manager as gbx_manager

    _drop_agg(spark)
    assert not _agg_registered(spark)
    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    assert _agg_registered(spark)


def test_registering_the_name_alone_installs_pmtiles_agg(spark):
    from databricks.labs.gbx import manager as gbx_manager

    _drop_agg(spark)
    assert not _agg_registered(spark)
    gbx_manager.register_family(
        spark, ("gbx_pmtiles_",), only=["gbx_pmtiles_agg"], level="L3"
    )
    assert _agg_registered(spark)
