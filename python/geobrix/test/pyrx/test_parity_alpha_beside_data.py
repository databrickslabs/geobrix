"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for rasters with an alpha band beside data bands
and for rasters masked by their own mask band.

Every reader and operation below judges each band alone: a pixel is valid for a band when the band's own
mask says so (NoData, the raster's own mask band) and, for a raster with an alpha band, its alpha is above
0, at any band count. Both tiers must aggregate, count, filter, vectorise and stretch the same pixels:
the raster-to-grid reducers, the band statistics, the histogram, the focal filters and convolution,
polygonize and isoband, the derived band, the percentile stretch and the tessellation chips.

The helpers (Spark session with the JAR, the two tiers' row collectors) are those of
``test_parity_bng_quadbin_raster_grid.py``.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-parity.sh \\
        --path python/geobrix/test/pyrx/test_parity_alpha_beside_data.py
"""

import logging
from pathlib import Path

import pytest

pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (a light-tier extra (e.g. geobrix[light_env6]) required)",
)
pytest.importorskip(
    "shapely",
    reason="shapely not installed (a light-tier extra (e.g. geobrix[light_env6]) required)",
)
from test.pyrx import _alpha_layouts as al  # noqa: E402
from test.pyrx import _tessellate_parity as tp  # noqa: E402

import numpy as np  # noqa: E402

from .test_parity_bng_quadbin_raster_grid import (  # noqa: E402
    _TESS_CONFIG,
    _decoded,
    _heavy_grid_rows,
    _light_grid_rows,
    _parity_cmp,
    _single_tile,
    _tess_id,
)

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pyrx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


# ---------------------------------------------------------------------------
# Spark fixture — JAR loaded (module scope), matches the pyvx/h3 parity harness
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    # spark.jars is a JVM-startup-time setting: no effect if a JVM (and Spark
    # session) is already live. Skip rather than mislead.
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; run "
                "this test in isolation: gbx:test:python --path "
                "python/geobrix/test/pyrx/test_parity_alpha_beside_data.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-bng-quadbin-raster-grid-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    yield session


# ---------------------------------------------------------------------------
# An alpha band beside data bands, at every band count: reducers, statistics, chips
# ---------------------------------------------------------------------------
# GDAL derives a mask from the alpha band for 2- and 4-band rasters only, and the statistics
# it computes through the multi-dimensional API apply none, so both tiers read the alpha
# band as values: a transparent pixel is valid for no data band, whatever the band count.


def _pattern_raster(name):
    """The 4x4 ``al.pattern`` of layout ``name`` over 2 x 2 degrees at 10E, 50N (rows 1 and
    3 opaque for an alpha layout)."""
    from rasterio.transform import from_origin

    return al.layout_bytes(name, transform=from_origin(10, 50, 0.5, 0.5))


def _valid_counts(name):
    """Per band, the pixels of the pattern that are valid for it: a data band of an alpha
    layout counts the opaque pixels, the alpha band every pixel."""
    n = len(al.LAYOUTS[name][0])
    return [8] * (n - 1) + [16] if al.LAYOUTS[name][1] else [16] * n


@pytest.mark.parametrize("assignment", ["centroid", "covering"])
@pytest.mark.parametrize("grid,resolution", [("h3", 4), ("quadbin", 6)])
@pytest.mark.parametrize("name", ["gray_x_alpha", "rgb_x_alpha", "rgba", "rgb_nir"])
def test_rastertogrid_alpha_parity(spark_with_jar, name, grid, resolution, assignment):
    """The reducers aggregate the same cells and the same pixels in both tiers: per band, the
    counts add up to the pixels valid for it."""
    spark = spark_with_jar
    raster = _pattern_raster(name)
    label = f"{grid} {assignment} {name}"
    kw = dict(coverage="sparse", assignment=assignment)
    light = _light_grid_rows(spark, raster, resolution, grid, "count", **kw)
    heavy = _heavy_grid_rows(spark, raster, resolution, grid, "count", **kw)
    assert light and heavy, f"{label}: a tier emitted no cells"
    _parity_cmp(light, heavy, label=label, tol=1e-6)
    for tier, rows in (("light", light), ("heavy", heavy)):
        totals = [
            sum(m for (b, _), m in rows.items() if b == band + 1)
            for band in range(len(_valid_counts(name)))
        ]
        assert totals == pytest.approx(
            [float(c) for c in _valid_counts(name)]
        ), f"{label}: {tier} totals {totals}"


def _scalar(spark, raster, tier, fn):
    """``fn(module, tile)`` of a one-row tile column in one tier -> the value."""
    from pyspark.sql import functions as f

    if tier == "light":
        from databricks.labs.gbx.pyrx import functions as mod

        mod.register(spark)
        df = tp.light_tile_df(spark, raster)
    else:
        from databricks.labs.gbx.rasterx import functions as mod

        mod.register(spark)
        df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
            mod.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
        )
    return df.select(fn(mod, f.col("tile")).alias("v")).collect()[0]["v"]


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgb_x_alpha", "rgba", "rgb_nir"])
def test_alpha_statistics_parity(spark_with_jar, name):
    """pixel count, min, max and mean per band, and isempty, agree between the tiers on
    the pixels valid for each band (an RGBA raster included: heavy's multi-dimensional
    statistics used to count its transparent pixels)."""
    spark = spark_with_jar
    raster = _pattern_raster(name)
    for fn_name in ("rst_pixelcount", "rst_min", "rst_max", "rst_avg"):
        light = _scalar(spark, raster, "light", lambda m, t: getattr(m, fn_name)(t))
        heavy = _scalar(spark, raster, "heavy", lambda m, t: getattr(m, fn_name)(t))
        assert list(light) == pytest.approx(list(heavy)), f"{name} {fn_name}"
    counts = _scalar(spark, raster, "light", lambda m, t: m.rst_pixelcount(t))
    assert list(counts) == _valid_counts(name)
    assert _scalar(spark, raster, "light", lambda m, t: m.rst_isempty(t)) is False
    assert _scalar(spark, raster, "heavy", lambda m, t: m.rst_isempty(t)) is False


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgb_x_alpha", "rgba"])
def test_an_entirely_transparent_raster_is_empty_in_both_tiers(spark_with_jar, name):
    count = len(al.LAYOUTS[name][0])
    data = np.full((count, 4, 4), 9, dtype="uint8")
    data[-1] = 0
    raster = al.layout_bytes(name, data=data)
    for tier in ("light", "heavy"):
        got = _scalar(spark_with_jar, raster, tier, lambda m, t: m.rst_isempty(t))
        assert got is True, f"{name}: {tier} reads a fully transparent raster as data"


def _chip_valid_rows(chips):
    """The source rows (coded in the pixels of ``al.row_coded``) that some chip pixel is valid for."""
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core._nodata import valid_mask

    rows = set()
    for _, b, _ in chips:
        with MemoryFile(b) as mf, mf.open() as ds:
            coded = ds.read(1).astype("int64") - 100
            rows |= {int(r) for r in coded[valid_mask(ds)] if r >= 0}
    return rows


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
@pytest.mark.parametrize("assignment", ["covering", "centroid"])
@pytest.mark.parametrize("name", ["rgba", "gray_x_alpha", "rgb_nir"])
def test_tessellate_alpha_and_own_mask_parity(spark_with_jar, name, assignment, grid):
    """A source with an alpha band AND its own mask band: the chips of both tiers are valid
    on exactly the source rows that are neither masked (rows 3-4) nor transparent (row 8).
    """
    from rasterio.transform import from_origin

    spark = spark_with_jar
    sql_name, fn_name, bng = _TESS_CONFIG[grid]
    raster = al.row_coded(
        name,
        height=12,
        masked_rows=(3, 4),
        transparent_rows=(8,),
        transform=from_origin(-0.13, 51.55, 0.01, 0.01),
    )
    resolution = {"h3": 8, "quadbin": 14}[grid]
    label = f"{grid} {assignment} {name}"

    def _id(cid):
        return _tess_id(cid, bng=bng)

    light = tp.light_chips(
        spark, raster, sql_name, resolution, assignment, "sparse", _id
    )
    heavy = tp.heavy_chips(
        spark, raster, fn_name, resolution, assignment, "sparse", _id
    )
    assert light and heavy, f"{label}: a tier emitted no chips"
    assert not tp.error_rows(light) and not tp.error_rows(heavy), f"{label}: errors"
    want = set(al.valid_rows(name, 12, (3, 4), (8,)))
    assert _chip_valid_rows(light) == want, f"{label}: light"
    assert _chip_valid_rows(heavy) == want, f"{label}: heavy"


# ---------------------------------------------------------------------------
# Readers and operations that judge each band alone: histogram, statistics of a masked raster,
# focal filters, polygonize, isoband, derived band, percentile stretch
# ---------------------------------------------------------------------------
# A pixel that is transparent (an alpha band beside data bands, at any band count) or hidden by the
# raster's own mask band is valid for no band: both tiers leave it out.

_STRIPED = ["gray_x_alpha", "rgba", "rgb_nir"]


def _striped(name):
    """6x6 of layout ``name``: 5 over rows 0-1 and 4-5, 99 over rows 2-3, which are transparent (an
    alpha layout) or masked (``rgb_nir``). 1 degree pixels at 0E, 6N."""
    from test.pyrx.test_alpha_own_mask_paths import _striped_alpha_raster

    return _striped_alpha_raster(name)


def _masked_rgb_nir():
    """The 4x4 pattern of RGB+NIR (no alpha band) with an internal mask hiding column 0."""
    from rasterio.transform import from_origin

    mask = np.full((4, 4), 255, dtype="uint8")
    mask[:, 0] = 0
    return al.layout_bytes(
        "rgb_nir", mask=mask, transform=from_origin(10, 50, 0.5, 0.5)
    )


def _sql_rows(spark, raster, tier, sql):
    """The rows of ``sql`` over a one-row view ``_alpha_t(tile)`` made from ``raster`` in one tier."""
    from pyspark.sql import functions as f

    if tier == "light":
        from databricks.labs.gbx.pyrx import functions as mod

        mod.register(spark)
        df = tp.light_tile_df(spark, raster)
    else:
        from databricks.labs.gbx.rasterx import functions as mod

        mod.register(spark)
        df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
            mod.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
        )
    df.createOrReplaceTempView("_alpha_t")
    return spark.sql(sql).collect()


@pytest.mark.parametrize(
    "name", ["gray_x_alpha", "rgb_x_alpha", "rgba", "masked_rgb_nir"]
)
def test_histogram_parity_over_the_pixels_valid_for_each_band(spark_with_jar, name):
    """Per band, the histogram counts the pixels valid for it. The range [0, 8) holds the data values
    (0 and 7) and not the alpha band's 255: GDAL's buckets are half-open and numpy's close the top one, so
    a range ending on a value that occurs would count it in one tier only."""
    raster = _masked_rgb_nir() if name == "masked_rgb_nir" else _pattern_raster(name)
    sql = "SELECT gbx_rst_histogram(tile, 8, 0.0, 8.0, false) AS h FROM _alpha_t"
    light = dict(_sql_rows(spark_with_jar, raster, "light", sql)[0]["h"])
    heavy = dict(_sql_rows(spark_with_jar, raster, "heavy", sql)[0]["h"])
    assert {k: list(v) for k, v in light.items()} == {
        k: list(v) for k, v in heavy.items()
    }
    # band 1 is 0 over rows 1 and 2 and 7 over rows 0 and 3 of the pattern; an alpha layout counts rows 1 and 3
    # (4 zeros and 4 sevens), the masked raster all rows but column 0 (6 and 6)
    want = (6, 6) if name == "masked_rgb_nir" else (4, 4)
    assert (light["band_1"][0], light["band_1"][7]) == want, light["band_1"]


@pytest.mark.parametrize("name", ["masked_rgb_nir"])
def test_statistics_of_a_raster_with_its_own_mask_parity(spark_with_jar, name):
    """pixel count, min, max, mean per band and isempty: the masked column is left out in both tiers
    (heavy's multi-dimensional statistics ignored the mask band)."""
    raster = _masked_rgb_nir()
    for fn_name in ("rst_pixelcount", "rst_min", "rst_max", "rst_avg"):
        light = _scalar(
            spark_with_jar, raster, "light", lambda m, t: getattr(m, fn_name)(t)
        )
        heavy = _scalar(
            spark_with_jar, raster, "heavy", lambda m, t: getattr(m, fn_name)(t)
        )
        assert list(light) == pytest.approx(list(heavy)), fn_name
    counts = _scalar(spark_with_jar, raster, "light", lambda m, t: m.rst_pixelcount(t))
    assert list(counts) == [12, 12, 12, 12]


@pytest.mark.parametrize("op", ["max", "min"])
@pytest.mark.parametrize("name", _STRIPED)
def test_focal_filter_parity_skips_transparent_and_masked_neighbours(
    spark_with_jar, name, op
):
    from pyspark.sql import functions as f

    raster = _striped(name)
    outs = {
        tier: _decoded(
            _single_tile(
                spark_with_jar, raster, tier, lambda m, t: m.rst_filter(t, 3, f.lit(op))
            )
        )
        for tier in ("light", "heavy")
    }
    assert outs["light"]["dtype"] == outs["heavy"]["dtype"]
    assert (outs["light"]["data"][0] == outs["heavy"]["data"][0]).all(), op
    assert (outs["light"]["data"][0] == 5).all(), "a hidden 99 reached a window"


@pytest.mark.parametrize("name", _STRIPED)
def test_convolve_parity_skips_transparent_and_masked_neighbours(spark_with_jar, name):
    """A 3x3 sum of ones. The edges differ by design (heavy skips the pixels off the raster, light
    replicates the edge), so the interior is compared: every window there holds nine pixels or
    their hidden neighbours' zero contribution."""
    from pyspark.sql import functions as f

    raster = _striped(name)
    kernel = f.array(*[f.array(*[f.lit(1.0)] * 3)] * 3)
    outs = {
        tier: _decoded(
            _single_tile(
                spark_with_jar, raster, tier, lambda m, t: m.rst_convolve(t, kernel)
            )
        )
        for tier in ("light", "heavy")
    }
    light, heavy = outs["light"]["data"][0], outs["heavy"]["data"][0]
    assert (light[1:5, 1:5] == heavy[1:5, 1:5]).all(), (light.tolist(), heavy.tolist())
    assert heavy.max() <= 45, "a hidden 99 reached a window"


def _vector_rows(spark, raster, tier, kind):
    """``(area, value)`` per polygon of polygonize / isoband of band 1 in one tier."""
    import shapely.wkb

    if kind == "polygonize" and tier == "light":
        sql = "SELECT t.geom_wkb AS g, t.value AS v FROM _alpha_t, LATERAL gbx_rst_polygonize(tile, 1, 4) t"
    elif kind == "polygonize":
        sql = (
            "SELECT p.geom_wkb AS g, p.value AS v FROM "
            "(SELECT explode(gbx_rst_polygonize(tile, 1, 4)) AS p FROM _alpha_t)"
        )
    else:
        sql = (
            "SELECT p.geom_wkb AS g, p.band AS v FROM "
            "(SELECT explode(gbx_rst_isoband(tile, array(0.0D, 10.0D, 200.0D))) AS p FROM _alpha_t)"
        )
    rows = _sql_rows(spark, raster, tier, sql)
    return sorted(
        (round(shapely.wkb.loads(bytes(r["g"])).area, 6), float(r["v"])) for r in rows
    )


@pytest.mark.parametrize("kind", ["polygonize", "isoband"])
@pytest.mark.parametrize("name", _STRIPED)
def test_polygonize_and_isoband_parity_leave_out_transparent_and_masked_pixels(
    spark_with_jar, name, kind
):
    raster = _striped(name)
    light = _vector_rows(spark_with_jar, raster, "light", kind)
    heavy = _vector_rows(spark_with_jar, raster, "heavy", kind)
    assert light == heavy, (light, heavy)
    assert sum(a for a, _ in light) == 24.0, "the hidden rows must stay out"


_FIRST_FUNC = (
    "def first(in_ar, out_ar, xoff, yoff, xsize, ysize, rxsize, rysize, radius, gt, **kw):\n"
    "    out_ar[:] = in_ar[0]\n"
)


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgba", "masked_rgb_nir"])
def test_derivedband_parity_is_invalid_where_the_input_is(spark_with_jar, name):
    """The single band derived from a raster has no alpha band of its own: both tiers write the input's
    validity as its own mask band, and the same values."""
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core._nodata import valid_mask

    raster = _masked_rgb_nir() if name == "masked_rgb_nir" else _pattern_raster(name)

    def derive(m, t):
        from pyspark.sql import functions as f

        return m.rst_derivedband(t, f.lit(_FIRST_FUNC), f.lit("first"))

    outs = {
        tier: _single_tile(spark_with_jar, raster, tier, derive)
        for tier in ("light", "heavy")
    }
    valids, datas = {}, {}
    for tier, out in outs.items():
        with MemoryFile(out) as mf, mf.open() as ds:
            valids[tier], datas[tier] = valid_mask(ds), ds.read(1)
    assert (valids["light"] == valids["heavy"]).all()
    # the heavyweight derived band keeps the input's data type, the lightweight one is Float64; the values
    # of the pixels that are invalid are not compared (GDAL's VRT reads a pixel its own mask hides as 0)
    ok = valids["light"]
    assert (
        datas["light"].astype("float64")[ok] == datas["heavy"].astype("float64")[ok]
    ).all()
    assert valids["light"].any() and not valids["light"].all()


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgba", "masked_rgb_nir"])
def test_percentile_stretch_parity_over_the_pixels_valid_for_each_band(
    spark_with_jar, name
):
    """The percentiles are over the pixels valid for the band and the pixels invalid for it are written 0,
    in both tiers: the values agree everywhere, the hidden pixels included."""
    # the 4x4 pattern: its valid values are 0 and 7 only, so both tiers' percentile rules (interpolated in
    # light, the nearest rank in heavy) give the same 0 and 7
    raster = _masked_rgb_nir() if name == "masked_rgb_nir" else _pattern_raster(name)
    outs = {
        tier: _decoded(
            _single_tile(
                spark_with_jar,
                raster,
                tier,
                lambda m, t: m.rst_percentile_stretch(t, 2.0, 98.0),
            )
        )
        for tier in ("light", "heavy")
    }
    assert (outs["light"]["data"] == outs["heavy"]["data"]).all()
