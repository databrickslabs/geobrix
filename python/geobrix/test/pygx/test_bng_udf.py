import pytest

shapely = pytest.importorskip("shapely")
from shapely import from_wkb, get_srid, to_wkb  # noqa: E402
from shapely.geometry import box  # noqa: E402

from databricks.labs.gbx.pygx import functions as gx  # noqa: E402

# quadbin cells at zoom 3 of (-122.4194, 37.7749) and (0, 0); custom-grid cells at resolution 3
QUADBIN_A, QUADBIN_B = 5202501994543054847, 5205105638077628415
CUSTOM_A, CUSTOM_B = 216172782125308048, 216172782156858408


def test_eastnorthasbng_and_cellarea(spark):
    gx.register(spark)
    row = spark.sql(
        "SELECT gbx_bng_eastnorthasbng(530000.0, 180000.0, '1km') AS c"
    ).collect()[0]
    assert row["c"] == "TQ3080"
    a = spark.sql("SELECT gbx_bng_cellarea('TQ3080') AS a").collect()[0]
    assert a["a"] == pytest.approx(1.0)


def test_pointascell_wkt_uses_bng_coords(spark):
    gx.register(spark)
    # EPSG:27700 eastings/northings (NOT WGS84). London at 1km.
    row = spark.sql(
        "SELECT gbx_bng_pointascell('POINT(530000 180000)', '1km') AS c"
    ).collect()[0]
    assert row["c"] == "TQ3080"


def test_a_null_in_an_int_resolution_column_leaves_the_other_rows_alone(spark):
    """A NULL in an INT (or DOUBLE) column makes pandas hand the whole batch over as floats: the
    resolution is 3.0 and the NULL is NaN. One row of each pointascell signature shape that takes
    a resolution (BNG geometry + res, BNG easting + northing + res, quadbin lon + lat + res,
    custom point + grid + res) beside the NULL rows: the NULL (a NULL resolution, or row 4's NULL
    easting / longitude) gives NULL, the rest are unchanged, and a resolution that is not a whole
    number is refused, not cut to one."""
    gx.register(spark)
    grid = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)"
    rows = [
        (1, "POINT(530000 180000)", 530000.0, 180000.0, -122.4194, 37.7749, 3),
        (2, "POINT(530000 180000)", 530000.0, 180000.0, -122.4194, 37.7749, None),
        (3, "POINT(325000 673000)", 325000.0, 673000.0, 0.0, 0.0, 3),
        (4, "POINT(530000 180000)", None, 180000.0, None, 37.7749, 3),
    ]
    spark.createDataFrame(
        rows, "id int, wkt string, e double, n double, lon double, lat double, res int"
    ).coalesce(1).createOrReplaceTempView("grid_null_res")
    got = spark.sql(
        "SELECT id, gbx_bng_pointascell(wkt, res) AS p, "
        "gbx_bng_eastnorthasbng(e, n, res) AS en, "
        "gbx_quadbin_pointascell(lon, lat, res) AS q, "
        f"gbx_custom_pointascell(wkt, {grid}, res) AS c FROM grid_null_res ORDER BY id"
    ).collect()
    assert [r["p"] for r in got] == ["TQ3080", None, "NT2573", "TQ3080"]
    assert [r["en"] for r in got] == ["TQ3080", None, "NT2573", None]
    assert [r["q"] for r in got] == [QUADBIN_A, None, QUADBIN_B, None]
    assert [r["c"] for r in got] == [CUSTOM_A, None, CUSTOM_B, CUSTOM_A]
    for call in (
        "gbx_quadbin_pointascell(-122.4194, 37.7749, 3.5)",
        f"gbx_custom_pointascell('POINT(530000 180000)', {grid}, 3.5)",
    ):
        with pytest.raises(Exception, match="3.5"):
            spark.sql(f"SELECT {call} AS c").collect()


def test_aswkb_no_srid_polygon(spark):
    gx.register(spark)
    out = spark.sql("SELECT gbx_bng_aswkb('TQ3080') AS w").collect()[0]
    g = from_wkb(bytes(out["w"]))
    assert g.geom_type == "Polygon" and get_srid(g) == 0


def test_aswkt_and_centroid(spark):
    gx.register(spark)
    wkt = spark.sql("SELECT gbx_bng_aswkt('TQ3080') AS w").collect()[0]["w"]
    assert wkt.upper().startswith("POLYGON")
    c = spark.sql("SELECT gbx_bng_centroid('TQ3080') AS w").collect()[0]
    g = from_wkb(bytes(c["w"]))
    assert g.geom_type == "Point" and get_srid(g) == 0
    assert (g.x, g.y) == (530500.0, 180500.0)


def test_distance_and_euclideandistance(spark):
    gx.register(spark)
    a = spark.sql(
        "SELECT gbx_bng_eastnorthasbng(530000.0, 180000.0, '1km') AS c"
    ).collect()[0]["c"]
    b = spark.sql(
        "SELECT gbx_bng_eastnorthasbng(531000.0, 181000.0, '1km') AS c"
    ).collect()[0]["c"]
    d = spark.sql(f"SELECT gbx_bng_distance('{a}', '{b}') AS d").collect()[0]["d"]
    e = spark.sql(f"SELECT gbx_bng_euclideandistance('{a}', '{b}') AS e").collect()[0][
        "e"
    ]
    assert d == 2  # Manhattan diagonal
    assert e == 1  # Chebyshev diagonal


def test_kring_array_of_strings(spark):
    gx.register(spark)
    rows = spark.sql("SELECT gbx_bng_kring('TQ3080', 1) AS r").collect()[0]
    assert "TQ3080" in rows["r"] and len(set(rows["r"])) == 9


def test_kloop_array_excludes_center(spark):
    gx.register(spark)
    rows = spark.sql("SELECT gbx_bng_kloop('TQ3080', 1) AS r").collect()[0]
    assert "TQ3080" not in rows["r"] and len(set(rows["r"])) == 8


def test_polyfill_and_tessellate(spark):
    gx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(530000.0, 180000.0, 533000.0, 183000.0))),)], "g binary"
    )
    df.createOrReplaceTempView("bv")
    pf = spark.sql("SELECT size(gbx_bng_polyfill(g, 3)) AS n FROM bv").collect()[0]
    assert pf["n"] > 0
    chips = spark.sql(
        "SELECT t.cellid, t.core, t.chip "
        "FROM bv LATERAL VIEW explode(gbx_bng_tessellate(g, 3)) AS t"
    ).collect()
    assert len(chips) > 0 and isinstance(chips[0]["cellid"], str)


def test_tessellate_keep_core_geom_defaults_to_false(spark):
    """The 2-arg SQL form and the Python wrapper leave core chips NULL, as heavy does."""
    gx.register(spark)
    poly = "POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))"
    df = spark.createDataFrame([(poly,)], "g string")
    df.createOrReplaceTempView("bv_tess_default")

    def by_cell(chips):
        return {c["cellid"]: c for c in chips}

    sql_two = by_cell(
        spark.sql(
            "SELECT gbx_bng_tessellate(g, 3) AS t FROM bv_tess_default"
        ).collect()[0]["t"]
    )
    wrapper = by_cell(df.select(gx.bng_tessellate("g", 3).alias("t")).collect()[0]["t"])
    kept = by_cell(
        df.select(gx.bng_tessellate("g", 3, keep_core_geom=True).alias("t")).collect()[
            0
        ]["t"]
    )
    assert sql_two["TQ3080"]["core"] and sql_two["TQ3080"]["chip"] is None
    assert wrapper == sql_two
    assert kept["TQ3080"]["chip"] is not None


def test_geomkring_and_geomkloop(spark):
    gx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(530000.0, 180000.0, 533000.0, 183000.0))),)], "g binary"
    )
    df.createOrReplaceTempView("bvg")
    gkr = spark.sql("SELECT gbx_bng_geomkring(g, 3, 1) AS r FROM bvg").collect()[0]["r"]
    gkl = spark.sql("SELECT gbx_bng_geomkloop(g, 3, 2) AS r FROM bvg").collect()[0]["r"]
    assert len(gkr) > 0 and all(isinstance(c, str) for c in gkr)
    assert all(isinstance(c, str) for c in gkl)


def test_kringexplode_udtf(spark):
    gx.register(spark)
    rows = spark.sql("SELECT cellid FROM gbx_bng_kringexplode('TQ3080', 1)").collect()
    assert len(rows) == 9 and all(isinstance(r["cellid"], str) for r in rows)


def test_kloopexplode_udtf(spark):
    gx.register(spark)
    rows = spark.sql("SELECT cellid FROM gbx_bng_kloopexplode('TQ3080', 1)").collect()
    assert len(rows) == 8


def test_tessellateexplode_udtf(spark):
    gx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(530000.0, 180000.0, 533000.0, 183000.0))),)], "g binary"
    )
    df.createOrReplaceTempView("bvte")
    rows = spark.sql(
        "SELECT t.cellid, t.core, t.chip FROM bvte, "
        "LATERAL gbx_bng_tessellateexplode(g, 3) AS t"
    ).collect()
    assert len(rows) > 0 and all(isinstance(r["cellid"], str) for r in rows)


def test_geomkringexplode_udtf(spark):
    gx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(530000.0, 180000.0, 533000.0, 183000.0))),)], "g binary"
    )
    df.createOrReplaceTempView("bvge")
    rows = spark.sql(
        "SELECT t.cellid FROM bvge, LATERAL gbx_bng_geomkringexplode(g, 3, 1) AS t"
    ).collect()
    assert len(rows) > 0 and all(isinstance(r["cellid"], str) for r in rows)


def test_cellunion_and_cellintersection_scalar(spark):
    gx.register(spark)
    cid = "TQ3080"
    left_chip = to_wkb(box(530000.0, 180000.0, 530500.0, 181000.0))
    right_chip = to_wkb(box(530500.0, 180000.0, 531000.0, 181000.0))
    df = spark.createDataFrame(
        [(cid, False, bytearray(left_chip), cid, False, bytearray(right_chip))],
        "lc string, lcore boolean, lg binary, rc string, rcore boolean, rg binary",
    )
    df.createOrReplaceTempView("cu")
    out = spark.sql(
        "SELECT gbx_bng_cellunion(struct(lc,lcore,lg), struct(rc,rcore,rg)) AS u FROM cu"
    ).collect()[0]["u"]
    assert out["cellid"] == cid
    g = from_wkb(bytes(out["chip"]))
    # two adjacent 500m-wide x 1km-tall halves union to the full 1km x 1km cell.
    assert g.area == pytest.approx(1000000.0, rel=1e-6)
    inter = spark.sql(
        "SELECT gbx_bng_cellintersection(struct(lc,lcore,lg), struct(rc,rcore,rg)) AS i FROM cu"
    ).collect()[0]["i"]
    gi = from_wkb(bytes(inter["chip"]))
    assert gi.is_empty or gi.area == pytest.approx(0.0)


def test_cellunion_agg(spark):
    gx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(530000.0, 180000.0, 535000.0, 185000.0))),)], "g binary"
    )
    df.createOrReplaceTempView("bv2")
    # explode tessellate chips, then dissolve per cell via the agg (BINARY chip).
    out = spark.sql(
        "WITH chips AS (SELECT t.* FROM bv2 LATERAL VIEW explode(gbx_bng_tessellate(g,3)) AS t) "
        "SELECT cellid, gbx_bng_cellunion_agg(struct(cellid, core, chip)) AS u "
        "FROM chips GROUP BY cellid"
    ).collect()
    assert len(out) > 0
    blobs = [r["u"] for r in out if r["u"] is not None]
    assert blobs
    g = from_wkb(bytes(blobs[0]))
    assert g.geom_type in ("Polygon", "MultiPolygon")


def test_cellintersection_agg(spark):
    gx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(530000.0, 180000.0, 535000.0, 185000.0))),)], "g binary"
    )
    df.createOrReplaceTempView("bv3")
    out = spark.sql(
        "WITH chips AS (SELECT t.* FROM bv3 LATERAL VIEW explode(gbx_bng_tessellate(g,3)) AS t) "
        "SELECT cellid, gbx_bng_cellintersection_agg(struct(cellid, core, chip)) AS u "
        "FROM chips GROUP BY cellid"
    ).collect()
    assert len(out) > 0


def test_explode_column_wrappers_raise_not_implemented():
    # The 5 *explode UDTFs are SQL-LATERAL-only; their Column wrappers must exist
    # (for binding parity) but raise NotImplementedError pointing to SQL LATERAL.
    for name in (
        "bng_kringexplode",
        "bng_kloopexplode",
        "bng_geomkringexplode",
        "bng_geomkloopexplode",
        "bng_tessellateexplode",
    ):
        fn = getattr(gx, name)
        with pytest.raises(NotImplementedError):
            fn("TQ3080", 1)


def test_scalar_column_wrappers_exist():
    # All non-explode wrappers exist as importable callables for parity.
    for name in (
        "bng_pointascell",
        "bng_eastnorthasbng",
        "bng_cellarea",
        "bng_distance",
        "bng_euclideandistance",
        "bng_aswkb",
        "bng_aswkt",
        "bng_centroid",
        "bng_cellintersection",
        "bng_cellunion",
        "bng_kring",
        "bng_kloop",
        "bng_polyfill",
        "bng_geomkring",
        "bng_geomkloop",
        "bng_tessellate",
        "bng_cellunion_agg",
        "bng_cellintersection_agg",
    ):
        assert callable(getattr(gx, name)), name
