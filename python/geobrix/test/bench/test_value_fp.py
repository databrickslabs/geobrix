"""The value-level fingerprints of the levels leg: the same values in another encoding hash alike, other values do not."""

import types

import numpy as np
import pytest

from databricks.labs.gbx.bench import levels_family as fam
from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import value_fp as vfp

rasterio = pytest.importorskip("rasterio")


def _tif(arr, **profile):
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    prof = dict(
        driver="GTiff", width=arr.shape[-1], height=arr.shape[-2], count=arr.shape[0],
        dtype=str(arr.dtype), crs="EPSG:4326", transform=from_origin(0, 64, 1, 1), nodata=-1.0,
    )  # fmt: skip
    prof.update(profile)
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(arr)
        return mf.read()


def _field():
    rng = np.random.default_rng(3)
    a = rng.random((1, 64, 64)).astype("float32")
    a[0, :4, :4] = -1.0  # nodata
    return a


def test_a_raster_hashes_by_its_pixels_not_its_codec_tiling_or_overviews():
    a = _field()
    plain = _tif(a)
    zstd_tiled = _tif(a, compress="zstd", tiled=True, blockxsize=16, blockysize=16)
    deflate = _tif(a, compress="deflate", predictor=3)
    assert (
        len(plain) != len(zstd_tiled) != len(deflate)
    )  # the length fingerprint tells them apart
    assert vfp.raster_fp(plain) == vfp.raster_fp(zstd_tiled) == vfp.raster_fp(deflate)


def test_a_raster_with_another_pixel_nodata_cell_or_grid_hashes_apart():
    a = _field()
    base = vfp.raster_fp(_tif(a))
    b = a.copy()
    b[0, 30, 30] += 0.01
    assert vfp.raster_fp(_tif(b)) != base  # a pixel
    c = a.copy()
    c[0, 10, 10] = -1.0
    assert vfp.raster_fp(_tif(c)) != base  # a nodata cell
    from rasterio.transform import from_origin

    assert (
        vfp.raster_fp(_tif(a, transform=from_origin(1, 64, 1, 1))) != base
    )  # the grid


def test_a_raster_with_another_dtype_nodata_value_or_crs_hashes_apart():
    a = np.zeros((1, 8, 8), dtype="float32")
    a[0, 2:4, 2:4] = (
        5.0  # exactly representable in both float32 and float64: only the dtype differs
    )
    base = vfp.raster_fp(_tif(a))
    assert vfp.raster_fp(_tif(a.astype("float64"))) != base  # dtype
    assert (
        vfp.raster_fp(_tif(a, nodata=-9999.0)) != base
    )  # nodata value, no pixel is nodata
    assert vfp.raster_fp(_tif(a, crs="EPSG:3857")) != base  # CRS
    assert vfp.raster_fp(_tif(a, nodata=None)) != base  # nodata present vs absent
    assert vfp.raster_fp(_tif(a, crs="EPSG:4326")) == base  # the same CRS hashes alike


def test_a_raster_with_sub_precision_noise_hashes_alike_and_a_real_gap_does_not():
    a = _field().astype("float64")
    noisy = a + 1e-13
    assert vfp.raster_fp(_tif(a)) == vfp.raster_fp(
        _tif(noisy)
    )  # kernels differ by rounding noise
    approx = a * 1.05  # GDAL's approximate proximity against the exact one: up to ~6%
    assert vfp.raster_fp(_tif(a)) != vfp.raster_fp(_tif(approx))


def test_a_polygon_hashes_alike_from_any_vertex_and_orientation():
    from shapely import wkb
    from shapely.geometry import Polygon

    ring = [(0, 0), (0, 4), (6, 4), (6, 0), (0, 0)]
    a = Polygon(ring)
    b = Polygon(ring[2:-1] + ring[:2] + [ring[2]])  # the same ring from another vertex
    c = Polygon(list(reversed(ring)))  # and the other way round
    assert wkb.dumps(a) != wkb.dumps(b)
    assert (
        vfp.wkb_fp(wkb.dumps(a)) == vfp.wkb_fp(wkb.dumps(b)) == vfp.wkb_fp(wkb.dumps(c))
    )
    assert vfp.wkb_fp(wkb.dumps(a)) != vfp.wkb_fp(
        wkb.dumps(Polygon([(0, 0), (0, 4), (6, 5), (6, 0)]))
    )
    bow = Polygon(
        [(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)]
    )  # invalid: the bench's repair input
    assert not bow.is_valid and vfp.wkb_fp(wkb.dumps(bow)) == vfp.wkb_fp(wkb.dumps(bow))


def test_doubles_hash_alike_to_the_last_bits_and_apart_beyond_them():
    x = [0.4875086306277067, 0.4228135583360952]
    assert vfp.doubles_fp(x) == vfp.doubles_fp([x[0] - 1.5e-14, x[1] + 1.2e-14])
    assert vfp.doubles_fp(x) != vfp.doubles_fp([x[0] + 1e-3, x[1]])


def test_a_vector_tile_hashes_by_its_extent_and_features():
    mvt = pytest.importorskip("mapbox_vector_tile")
    feats = [{"geometry": f"POINT({i} {i})", "properties": {"id": i}} for i in range(5)]
    full = mvt.encode(
        [{"name": "l", "features": feats}], default_options={"extents": 4096}
    )
    again = mvt.encode(
        [{"name": "l", "features": feats[::-1]}], default_options={"extents": 4096}
    )
    fewer = mvt.encode(
        [{"name": "l", "features": feats[:4]}], default_options={"extents": 4096}
    )
    coarse = mvt.encode(
        [{"name": "l", "features": feats}], default_options={"extents": 128}
    )
    assert vfp.mvt_fp(full) == vfp.mvt_fp(again)
    assert (
        len({vfp.mvt_fp(b) for b in (full, fewer, coarse)}) == 3
    )  # a dropped feature or a coarser extent shows


def test_every_decoded_representative_has_a_value_query_for_each_of_its_levels():
    """The families whose length fingerprint differed between levels (G8) carry a `vsql` on every L1/L3 form, never on a floor."""
    want = {r.fn for r in fam.REPS if r.vfp}
    assert want >= {
        "gbx_rst_boundingbox", "gbx_rst_avg", "gbx_rst_proximity", "gbx_rst_cog_convert", "gbx_rst_binpoints",
        "gbx_rst_align_to", "gbx_st_asmvt", "gbx_rst_bng_rasterize_agg",
    }  # fmt: skip
    for family, forms in lf.FORMS.items():
        for f in forms:
            if f.fn in want and f.level in ("L1", "L3") and "@" in f.variant:
                if f.variant.startswith("floor"):
                    assert not f.vsql, (family, f.variant)
                else:
                    assert f.vsql and "gbx_bench_vfp_" in f.vsql, (
                        family,
                        f.fn,
                        f.variant,
                    )
    t = [
        f
        for f in lf.FORMS["S"]
        if f.fn == "gbx_rst_transformcrs" and f.variant != "floor"
    ]
    assert t and all("gbx_bench_vfp_raster" in f.vsql for f in t)


# ---- the few tiles a value fingerprint decodes are chosen by a key of the input, not by the scan ------------------------


def _vfp_tile_rows(n, reverse):
    """`n` tiles of a table, in the order a scan may meet them: `path` and `raster` differ per tile."""
    rows = [(f"/v/t{i:03d}.tif", bytes([i % 251, (i * 7) % 251, 1])) for i in range(n)]
    return rows[::-1] if reverse else rows


def _tile_table(spark, name, reverse):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        _vfp_tile_rows(100, reverse), "path string, raster binary"
    ).select(F.struct("path", "raster").alias("tile"))
    df.repartition(1).createOrReplaceTempView(name)
    return name


def _stub_vfp_udfs(spark, fn):
    """The decoder and the function under test as stand-ins that return the tile's bytes: the fingerprint then depends only
    on WHICH tiles the query picked."""
    import zlib

    from pyspark.sql.types import BinaryType, LongType, StructField, StructType

    spark.udf.register(
        "gbx_bench_vfp_raster", lambda b: zlib.crc32(b) if b else 0, LongType()
    )
    out = StructType([StructField("raster", BinaryType())])
    spark.udf.register(fn, lambda t, *a: (t["raster"],), out)


def _fp(spark, sql, src):
    r = spark.sql(sql.format(src=src, uc="")).collect()[0]
    return r[0], r[1]


def test_the_tile_value_query_picks_the_same_tiles_whatever_order_the_scan_meets_them(
    spark_session_local,
):
    """An unordered LIMIT hashed whichever 64 tiles the scan met first: two levels (or two runs) differed in value while
    every output was equal. The 64 are the first by a key of the tile, so the sample is the same for any scan order.
    """
    rep = next(r for r in fam.REPS if r.fn == "gbx_rst_proximity")
    _stub_vfp_udfs(spark_session_local, rep.fn)
    sql = fam._vsql(rep, rep.fn)
    assert "ORDER BY" in sql and sql.index("ORDER BY") < sql.index("LIMIT")
    fwd = _fp(
        spark_session_local, sql, _tile_table(spark_session_local, "vfp_fwd", False)
    )
    rev = _fp(
        spark_session_local, sql, _tile_table(spark_session_local, "vfp_rev", True)
    )
    assert fwd[0] == rev[0] == fam._VFP_TILE_ROWS
    assert fwd == rev


def test_the_tile_value_query_spreads_its_tiles_over_tasks_and_keeps_the_value(
    spark_session_local,
):
    """The 64 tiles after the ORDER BY ... LIMIT sit in ONE partition: the function and the decoder then ran in one task
    (a light rst_fromcontent value pass took over 6 min on dedicated DBR 19). They are dealt over 64 tasks first; the
    fingerprint is a sum, so it does not change."""
    rep = next(r for r in fam.REPS if r.fn == "gbx_rst_proximity")
    _stub_vfp_udfs(spark_session_local, rep.fn)
    sql = fam._vsql(rep, rep.fn)
    src = _tile_table(spark_session_local, "vfp_spread", False)
    df = spark_session_local.sql(sql.format(src=src, uc=""))
    plan = df._jdf.queryExecution().executedPlan().toString()
    assert "RoundRobinPartitioning(" + str(fam._VFP_TILE_ROWS) in plan
    one_task = sql.replace("/*+ REPARTITION(" + str(fam._VFP_TILE_ROWS) + ") */ ", "")
    assert one_task != sql
    assert _fp(spark_session_local, sql, src) == _fp(spark_session_local, one_task, src)


def test_the_transformcrs_value_query_picks_the_same_tiles_whatever_order_the_scan_meets_them(
    spark_session_local,
):
    fn = "gbx_rst_transformcrs"
    _stub_vfp_udfs(spark_session_local, fn)
    sql = lf._TCRS_V.replace("{fn}", fn)
    assert "ORDER BY" in sql and sql.index("ORDER BY") < sql.index("LIMIT")
    fwd = _fp(
        spark_session_local, sql, _tile_table(spark_session_local, "vfp_fwd2", False)
    )
    rev = _fp(
        spark_session_local, sql, _tile_table(spark_session_local, "vfp_rev2", True)
    )
    assert fwd[0] == rev[0] == 64 and fwd == rev


def test_every_tile_producing_representative_with_a_value_query_has_an_ordering_key_for_its_input():
    for r in fam.REPS:
        if r.vfp and r.shape is fam._TILE:
            sql = fam._vsql(r, r.fn)
            assert "ORDER BY" in sql and sql.index("ORDER BY") < sql.index(
                "LIMIT"
            ), r.fn
            assert "{order}" not in sql, r.fn


# ---- the fingerprint falls back to the length one, with a note, when the value pass cannot answer ------------------------


class _Rows:
    def __init__(self, row=None, error=None):
        self.row, self.error = row, error

    def sql(self, text):
        if self.error:
            raise self.error
        return types.SimpleNamespace(collect=lambda: [self.row])


def _vfp_ctx(spark):
    from databricks.labs.gbx.bench import levels

    ctx = levels._Ctx(spark, 0, 1, "", None, None, {})
    ctx.setups.add("value_fp")  # the UDF registration is not under test
    return ctx


@pytest.mark.parametrize(
    "spark, allows_null, want_fp, want_note",
    [
        (_Rows(row=(10, 777, 10)), False, "v1:777", "len_fp=LEN"),
        (
            _Rows(error=RuntimeError("decoder blew up")),
            False,
            "LEN",
            "value_fp failed: RuntimeError: decoder blew up",
        ),
        (_Rows(row=(10, 777, 7)), False, "LEN", "value_fp: 3 of 10 outputs NULL"),
        (_Rows(row=(10, None, 10)), False, "LEN", "value_fp: no value"),
        (
            _Rows(row=(10, 777, 7)),
            True,
            "v1:777",
            "len_fp=LEN",
        ),  # a form that allows NULL outputs keeps its value
    ],
)
def test_the_value_fingerprint_keeps_the_length_one_when_the_value_pass_fails(
    spark, allows_null, want_fp, want_note
):
    from databricks.labs.gbx.bench import levels

    form = levels.Form(
        mvp="X", fn="f", level="L3", variant="v", input="i", sql="", vsql="SELECT 1 FROM {src}",
        allows_null=allows_null,
    )  # fmt: skip
    fp, note = levels._value_fingerprint(_vfp_ctx(spark), form, "t", 10, 10, "LEN")
    assert (fp, note) == (want_fp, [want_note])


def test_the_null_report_names_how_many_which_set_and_the_first_tiles():
    from databricks.labs.gbx.bench import levels

    form = levels.Form(
        mvp="X", fn="f", level="L3", variant="v", input="i", sql="", nullsql="SELECT 1 FROM {src}"
    )  # fmt: skip
    spark = _Rows(row=(2, "abc123", ["/a.tif", "/b.tif"]))
    assert levels._null_report(_vfp_ctx(spark), form, "t") == [
        "nulls=2",
        "null_set=abc123",
        "null_first=/a.tif,/b.tif",
    ]
    bad = _Rows(error=RuntimeError("no such column"))
    (note,) = levels._null_report(_vfp_ctx(bad), form, "t")
    assert note == "null_report failed: RuntimeError: no such column"


def test_a_step_after_the_timed_iterations_is_named_on_the_rung_and_the_heartbeat():
    from databricks.labs.gbx.bench import levels

    seen = []
    ctx = _vfp_ctx(_Rows(row=(1, 1, 1)))
    ctx.rung = types.SimpleNamespace(phase="")
    ctx.heartbeat = types.SimpleNamespace(set_phase=seen.append)
    levels._say_phase(ctx, "value fingerprint")
    assert ctx.rung.phase == "value fingerprint" and seen == ["value fingerprint"]


def test_the_steps_after_the_timed_iterations_compose_in_order(monkeypatch):
    """`_after_timing` runs the value fingerprint (which replaces the length fingerprint) and then the NULL report,
    names each step and the row write on the heartbeat, and runs a step only for a form that asks for it.
    """
    from databricks.labs.gbx.bench import levels

    monkeypatch.setattr(
        levels, "_value_fingerprint", lambda *a: ("v1:abc", ["len_fp=LEN"])
    )
    monkeypatch.setattr(levels, "_null_report", lambda *a: ["nulls=0"])
    seen = []
    ctx = _vfp_ctx(_Rows(row=(1, 1, 1)))
    ctx.heartbeat = types.SimpleNamespace(set_phase=seen.append)

    both = types.SimpleNamespace(vsql="SELECT 1", nullsql="SELECT 2")
    fp, notes = levels._after_timing(ctx, both, "t", 10, 10, "LEN")
    assert (fp, notes) == ("v1:abc", ["len_fp=LEN", "nulls=0"])
    assert [p.split(" (")[0] for p in seen] == [
        "value fingerprint",
        "NULL-output report",
        "row written",
    ]

    seen.clear()
    neither = types.SimpleNamespace(vsql=None, nullsql=None)
    assert levels._after_timing(ctx, neither, "t", 10, 10, "LEN") == ("LEN", [])
    assert seen == ["row written"]
