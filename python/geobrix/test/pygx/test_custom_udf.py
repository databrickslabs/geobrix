import pytest

shapely = pytest.importorskip("shapely")

from shapely import from_wkb, get_srid, to_wkb  # noqa: E402
from shapely.geometry import box  # noqa: E402

from databricks.labs.gbx.pygx import functions as gx  # noqa: E402

_GRID = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)"


# --- Validating grid-spec builder ---------------------------------


def test_custom_grid_struct_shape(spark):
    gx.register(spark)
    row = spark.sql(
        "SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS g"
    ).collect()[0]
    g = row["g"].asDict()
    assert g["bound_x_min"] == 0 and g["bound_x_max"] == 1000000
    assert g["cell_splits"] == 2 and g["root_cell_size_x"] == 1000
    assert g["srid"] == 27700


def test_custom_grid_7arg_defaults_srid_minus1(spark):
    gx.register(spark)
    row = spark.sql(
        "SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000) AS g"
    ).collect()[0]
    assert row["g"].asDict()["srid"] == -1


def test_custom_grid_validation_raises(spark):
    gx.register(spark)
    with pytest.raises(Exception):  # PythonException wrapping ValueError
        spark.sql(
            "SELECT gbx_custom_grid(1000000, 0, 0, 1000000, 2, 1000, 1000)"
        ).collect()  # xMax <= xMin
    with pytest.raises(Exception):
        spark.sql(
            "SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 1, 1000, 1000)"
        ).collect()  # cell_splits < 2


# --- Consuming functions ------------------------------------------


def test_pointascell_wkt_and_wkb(spark):
    gx.register(spark)
    # WKT input (all 4 geometry encodings are accepted).
    r1 = spark.sql(
        f"SELECT gbx_custom_pointascell('POINT(530000 180000)', {_GRID}, 0) AS c"
    ).collect()[0]
    assert r1["c"] is not None
    # WKB input must give the SAME cell id.
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(530000, 180000, 530001, 180001).centroid)),)],
        "g binary",
    )
    df.createOrReplaceTempView("pv")
    r2 = spark.sql(
        f"SELECT gbx_custom_pointascell(g, {_GRID}, 0) AS c FROM pv"
    ).collect()[0]
    assert r2["c"] == r1["c"]


def test_cellaswkb_no_srid_and_centroid(spark):
    gx.register(spark)
    cell = spark.sql(
        f"SELECT gbx_custom_pointascell('POINT(530000 180000)', {_GRID}, 0) AS c"
    ).collect()[0]["c"]
    out = spark.sql(f"SELECT gbx_custom_cellaswkb({cell}L, {_GRID}) AS w").collect()[0]
    g = from_wkb(bytes(out["w"]))
    assert g.geom_type == "Polygon" and get_srid(g) == 0  # no SRID stamped
    cen = spark.sql(f"SELECT gbx_custom_centroid({cell}L, {_GRID}) AS w").collect()[0]
    cg = from_wkb(bytes(cen["w"]))
    assert cg.geom_type == "Point" and get_srid(cg) == 0  # no SRID stamped
    wkt = spark.sql(f"SELECT gbx_custom_cellaswkt({cell}L, {_GRID}) AS w").collect()[0]
    assert wkt["w"].startswith("POLYGON")


def test_polyfill_and_kring_arrays(spark):
    gx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(530000.0, 180000.0, 533000.0, 183000.0))),)],
        "g binary",
    )
    df.createOrReplaceTempView("rv")
    pf = spark.sql(
        f"SELECT size(gbx_custom_polyfill(g, {_GRID}, 0)) AS n FROM rv"
    ).collect()[0]
    assert pf["n"] == 9
    cell = spark.sql(
        f"SELECT gbx_custom_pointascell('POINT(530000 180000)', {_GRID}, 0) AS c"
    ).collect()[0]["c"]
    kr = spark.sql(f"SELECT gbx_custom_kring({cell}L, {_GRID}, 1) AS r").collect()[0]
    assert cell in kr["r"] and len(set(kr["r"])) == 9


def test_null_propagation(spark):
    gx.register(spark)
    df = spark.createDataFrame([(None,)], "g binary")
    df.createOrReplaceTempView("nv")
    r = spark.sql(
        f"SELECT gbx_custom_pointascell(g, {_GRID}, 0) AS c FROM nv"
    ).collect()[0]
    assert r["c"] is None


# --- Column wrappers -------------------------------------------------------


def test_column_wrappers(spark):
    gx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(530000.0, 180000.0, 533000.0, 183000.0))),)],
        "g binary",
    )
    grid = gx.custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)
    cell_row = df.select(
        gx.custom_pointascell(gx.f.lit("POINT(530000 180000)"), grid, 0).alias("c")
    ).collect()[0]
    assert cell_row["c"] is not None
    out = df.select(
        gx.custom_cellaswkb(gx.f.lit(cell_row["c"]), grid).alias("w"),
        gx.custom_cellaswkt(gx.f.lit(cell_row["c"]), grid).alias("t"),
        gx.custom_centroid(gx.f.lit(cell_row["c"]), grid).alias("ce"),
        gx.f.size(gx.custom_polyfill(df["g"], grid, 0)).alias("pf"),
        gx.f.size(gx.custom_kring(gx.f.lit(cell_row["c"]), grid, 1)).alias("kr"),
    ).collect()[0]
    assert from_wkb(bytes(out["w"])).geom_type == "Polygon"
    assert out["t"].startswith("POLYGON")
    assert from_wkb(bytes(out["ce"])).geom_type == "Point"
    assert out["pf"] == 9 and out["kr"] == 9
