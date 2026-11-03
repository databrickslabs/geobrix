from databricks.labs.gbx.vectorx import helpers as vx_helpers


def test_coverageisvalid_sql(spark):
    from shapely import box, to_wkb

    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(
        spark, ("gbx_st_",), only=["gbx_st_coverageisvalid"], level="L3"
    )
    rows = [("c", to_wkb(box(0, 0, 1, 1))), ("c", to_wkb(box(1, 0, 2, 1)))]
    spark.createDataFrame(rows, "cov_id string, geom binary").createOrReplaceTempView(
        "cov"
    )
    r = spark.sql(
        "SELECT gbx_st_coverageisvalid(geom, 0.0) AS ok FROM cov GROUP BY cov_id"
    ).collect()
    assert r[0]["ok"] is True


def test_coverage_simplify_helper_n_to_n(spark):
    from shapely import box, from_wkb, to_wkb

    rows = [("c", "p0", to_wkb(box(0, 0, 1, 1))), ("c", "p1", to_wkb(box(1, 0, 2, 1)))]
    df = spark.createDataFrame(rows, "cov_id string, name string, geom binary")
    out = vx_helpers.coverage_simplify(df, "cov_id", "geom", 0.0).collect()
    assert len(out) == 2
    assert {row["name"] for row in out} == {"p0", "p1"}
    # PySpark collect() returns bytearray for BinaryType; bytes() makes it readable
    # by shapely 2.1.2's from_wkb (which doesn't accept bytearray in this version).
    assert all(from_wkb(bytes(row["geom_simplified"])).is_valid for row in out)
