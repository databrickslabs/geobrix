"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for rasters with an alpha band beside data bands
and for rasters masked by their own mask band.

Every reader and operation below judges each band alone: a pixel is valid for a band when the band's own
mask says so (NoData, the raster's own mask band) and, for a raster with an alpha band, its alpha is above
0, at any band count. Both tiers must aggregate, count, vectorise and convolve the same pixels:
the band statistics, the histogram, convolution,
isoband, the derived band and the tessellation chips.

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
    reason="rasterio not installed (the lightweight tier required: the env wheel, or pip install geobrix)",
)
pytest.importorskip(
    "shapely",
    reason="shapely not installed (the lightweight tier required: the env wheel, or pip install geobrix)",
)
from test._heavy import heavy_register  # noqa: E402
from test.pyrx import _alpha_layouts as al  # noqa: E402
from test.pyrx import _tessellate_parity as tp  # noqa: E402

import numpy as np  # noqa: E402
from pyspark.sql import functions as f  # noqa: E402

from databricks.labs.gbx.rasterx import functions as rx  # noqa: E402

from .test_parity_bng_quadbin_raster_grid import (  # noqa: E402
    _TESS_CONFIG,
    _decoded,
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
        .appName("gbx-alpha-beside-data-parity")
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


def _tile_df(spark, raster, tier):
    """A one-row DataFrame ``(tile)`` of ``raster`` made by one tier (its functions registered)."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx import manager as gbx_manager

    if tier == "light":
        from databricks.labs.gbx.pyrx import functions as mod

        gbx_manager.register_family(spark, ("gbx_rst_",), level="L3")
        return mod, tp.light_tile_df(spark, raster)
    from databricks.labs.gbx.rasterx import functions as mod

    heavy_register(spark, ("gbx_rst_",))
    return mod, spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )


def _scalar(spark, raster, tier, fn):
    """``fn(module, tile)`` of a one-row tile column in one tier -> the value."""
    from pyspark.sql import functions as f

    mod, df = _tile_df(spark, raster, tier)
    return df.select(fn(mod, f.col("tile")).alias("v")).collect()[0]["v"]


def _assert_band_statistics(spark, raster, want, label=""):
    """pixel count, min, max and mean per band, each stated (``want``: one ``(count, min, max, mean)`` per band).

    These four functions have no heavyweight level, so the light tier is checked against the values.
    """
    for index, fn_name in enumerate(
        ("rst_pixelcount", "rst_min", "rst_max", "rst_avg")
    ):
        got = _scalar(
            spark, raster, "light", lambda m, t: f.call_function(f"gbx_{fn_name}", t)
        )
        assert list(got) == pytest.approx(
            [band[index] for band in want]
        ), f"{label} {fn_name}"


def _both_tiers(spark, raster, fn):
    """``{tier: decoded output raster}`` of ``fn(module, tile)`` in the light and the heavy tier."""
    return {
        tier: _decoded(_single_tile(spark, raster, tier, fn))
        for tier in ("light", "heavy")
    }


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgb_x_alpha", "rgba", "rgb_nir"])
def test_alpha_statistics_parity(spark_with_jar, name):
    """pixel count, min, max and mean per band agree with the pixels valid for each band (an RGBA
    raster included: heavy's multi-dimensional statistics used to count its transparent pixels), and
    isempty (the light level alone) reads the raster as not empty.

    The pattern's data bands hold 7 over rows 0 and 3 and 0 over rows 1 and 2 (mean 3.5 over the
    pixels valid for them); the last band holds 255 over rows 1 and 3 and 0 over the others, and it is
    valid everywhere."""
    spark = spark_with_jar
    raster = _pattern_raster(name)
    n = len(al.LAYOUTS[name][0])
    counts = _valid_counts(name)
    want = [(counts[i], 0, 7, 3.5) for i in range(n - 1)] + [(16, 0, 255, 127.5)]
    _assert_band_statistics(spark, raster, want, name)
    assert (
        _scalar(
            spark, raster, "light", lambda m, t: f.call_function("gbx_rst_isempty", t)
        )
        is False
    )


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgb_x_alpha", "rgba"])
def test_an_entirely_transparent_raster_is_empty(spark_with_jar, name):
    """isempty has the light level alone: a raster whose alpha is 0 everywhere is empty."""
    count = len(al.LAYOUTS[name][0])
    data = np.full((count, 4, 4), 9, dtype="uint8")
    data[-1] = 0
    raster = al.layout_bytes(name, data=data)
    got = _scalar(
        spark_with_jar,
        raster,
        "light",
        lambda m, t: f.call_function("gbx_rst_isempty", t),
    )
    assert got is True, f"{name}: light reads a fully transparent raster as data"


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
def test_tessellate_alpha_and_own_mask(spark_with_jar, name, assignment, grid):
    """A source with an alpha band AND its own mask band: the chips (the light level alone) are valid
    on exactly the source rows that are neither masked (rows 3-4) nor transparent (row 8).
    """
    from rasterio.transform import from_origin

    spark = spark_with_jar
    sql_name, _, bng = _TESS_CONFIG[grid]
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
    assert light, f"{label}: no chips"
    assert not tp.error_rows(light), f"{label}: errors"
    want = set(al.valid_rows(name, 12, (3, 4), (8,)))
    assert _chip_valid_rows(light) == want, f"{label}: light"


# ---------------------------------------------------------------------------
# Readers and operations that judge each band alone: histogram, statistics of a masked raster,
# convolution, isoband, derived band
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


def _sql_rows(spark, raster, tier, sql, jvm=()):
    """The rows of ``sql`` over a one-row view ``_alpha_t(tile)`` made from ``raster`` in one tier.

    The heavyweight register puts the manager's choice back for every name, so a function whose default is the
    lightweight level runs light in the "heavy" tier. ``jvm`` names the functions to run on their JVM companion
    (L1), registered after the tier's own register; use it where the test is about the JVM's behaviour.
    """
    _, df = _tile_df(spark, raster, tier)
    if tier == "heavy" and jvm:
        from databricks.labs.gbx import register

        register(spark, only=list(jvm), _levels={n: ("L1", "-") for n in jvm})
    df.createOrReplaceTempView("_alpha_t")
    return spark.sql(sql).collect()


def _raster_of(name):
    """The fixture of a test case: the RGB+NIR raster with an internal mask, or the 4x4 pattern of a layout."""
    return _masked_rgb_nir() if name == "masked_rgb_nir" else _pattern_raster(name)


@pytest.mark.parametrize(
    "name", ["gray_x_alpha", "rgb_x_alpha", "rgba", "masked_rgb_nir"]
)
def test_histogram_over_the_pixels_valid_for_each_band(spark_with_jar, name):
    """Per band, the histogram counts the pixels valid for it. The range [0, 8) holds the data values
    (0 and 7) and not the alpha band's 255."""
    raster = _raster_of(name)
    sql = "SELECT gbx_rst_histogram(tile, 8, 0.0, 8.0, false) AS h FROM _alpha_t"
    light = dict(_sql_rows(spark_with_jar, raster, "light", sql)[0]["h"])
    # band 1 is 0 over rows 1 and 2 and 7 over rows 0 and 3 of the pattern; an alpha layout counts rows 1 and 3
    # (4 zeros and 4 sevens), the masked raster all rows but column 0 (6 and 6)
    want = (6, 6) if name == "masked_rgb_nir" else (4, 4)
    assert (light["band_1"][0], light["band_1"][7]) == want, light["band_1"]


def test_statistics_of_a_raster_with_its_own_mask_parity(spark_with_jar):
    """pixel count, min, max, mean per band: the masked column is left out (heavy's multi-dimensional
    statistics ignored the mask band). Column 0 is hidden, so each band counts 12 pixels; the colour
    bands hold 7 over two rows and 0 over two (mean 3.5), the last band 255 over two rows and 0 over
    two (mean 127.5)."""
    want = [(12, 0, 7, 3.5)] * 3 + [(12, 0, 255, 127.5)]
    _assert_band_statistics(spark_with_jar, _masked_rgb_nir(), want)


@pytest.mark.parametrize("name", _STRIPED)
def test_convolve_parity_skips_transparent_and_masked_neighbours(spark_with_jar, name):
    """A 3x3 sum of ones over the whole raster: both tiers add only the in-bounds neighbours (the
    positions off the raster add 0, as a hidden neighbour does), so the edges agree too.
    """
    from pyspark.sql import functions as f

    kernel = f.array(*[f.array(*[f.lit(1.0)] * 3)] * 3)
    outs = _both_tiers(
        spark_with_jar,
        _striped(name),
        lambda m, t: f.call_function("gbx_rst_convolve", t, kernel),
    )
    light, heavy = outs["light"]["data"][0], outs["heavy"]["data"][0]
    assert (light == heavy).all(), (light.tolist(), heavy.tolist())
    assert heavy.max() <= 45, "a hidden 99 reached a window"


def _vector_rows(spark, raster, tier, kind):
    """``(area, value)`` per polygon of isoband of band 1 in one tier."""
    import shapely.wkb

    sql = (
        "SELECT p.geom_wkb AS g, p.band AS v FROM "
        "(SELECT explode(gbx_rst_isoband(tile, array(0.0D, 10.0D, 200.0D))) AS p FROM _alpha_t)"
    )
    rows = _sql_rows(spark, raster, tier, sql)
    return sorted(
        (round(shapely.wkb.loads(bytes(r["g"])).area, 6), float(r["v"])) for r in rows
    )


@pytest.mark.parametrize("kind", ["isoband"])
@pytest.mark.parametrize("name", _STRIPED)
def test_isoband_leaves_out_transparent_and_masked_pixels(spark_with_jar, name, kind):
    """isoband has the light level alone: the transparent and masked rows stay out of its polygons."""
    raster = _striped(name)
    light = _vector_rows(spark_with_jar, raster, "light", kind)
    assert light, "isoband gave no polygon"
    assert sum(a for a, _ in light) == 24.0, "the hidden rows must stay out"


_FIRST_FUNC = (
    "def first(in_ar, out_ar, xoff, yoff, xsize, ysize, rxsize, rysize, radius, gt, **kw):\n"
    "    out_ar[:] = in_ar[0]\n"
)


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgba", "masked_rgb_nir"])
def test_derivedband_is_invalid_where_the_input_is(spark_with_jar, name):
    """The single band derived from a raster has no alpha band of its own: the light level (the only one)
    writes the input's validity as its own mask band, and the input's values where it is valid.
    """
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core._nodata import valid_mask

    raster = _raster_of(name)

    def derive(m, t):
        from pyspark.sql import functions as f

        return f.call_function(
            "gbx_rst_derivedband", t, f.lit(_FIRST_FUNC), f.lit("first")
        )

    out = _single_tile(spark_with_jar, raster, "light", derive)
    with MemoryFile(out) as mf, mf.open() as ds:
        valid, data = valid_mask(ds), ds.read(1)
    with MemoryFile(raster) as mf, mf.open() as src:
        want_valid, first = valid_mask(src), src.read(1)
    assert (valid == want_valid).all()
    assert (data.astype("float64")[valid] == first.astype("float64")[valid]).all()
    assert valid.any() and not valid.all()
