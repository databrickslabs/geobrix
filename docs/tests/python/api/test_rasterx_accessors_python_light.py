"""
Tests for the light (pyrx) tier per-function RasterX accessor examples.

Every example in rasterx_accessors_python_light.py runs against the canonical fixtures and meets its check below.
A new example without a check, or without its `*_output` constant, fails.

Fixtures:
- SINGLE-BAND (nyc_sentinel2_red.tif, EPSG:32618, 161x236, 10 m pixels, nodata 0)
- MULTIBAND (rgb_nir_small.tif, EPSG:4326, 8x8, 3 bands, UInt16)
- NETCDF (prAdjust_day_HadGEM2-CC_*.nc, 2 subdatasets)
"""

import pytest

try:
    from . import rasterx_accessors_python_light as light_examples
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_accessors_python_light as light_examples
    except ModuleNotFoundError:
        light_examples = None


def _bands(n=3):
    return lambda r: isinstance(r, list) and len(r) == n and r[0] is not None


def _float(value):
    return lambda r: isinstance(r, float) and r == value


# example name (without `_python_example`) -> what its result must satisfy
CHECKS = {
    # multiband
    "rst_avg": _bands(),
    "rst_bandmetadata": lambda r: isinstance(r, dict) and "name" in r,
    "rst_histogram": lambda r: isinstance(r, dict)
    and len(r) == 3
    and all(isinstance(v, list) for v in r.values()),
    "rst_isempty": lambda r: r is False,
    "rst_max": _bands(),
    "rst_median": _bands(),
    "rst_min": _bands(),
    "rst_numbands": lambda r: r == 3,
    "rst_pixelcount": lambda r: r == [64, 64, 64],
    "rst_summary": lambda r: isinstance(r, str)
    and ("GTiff" in r or "driverShortName" in r),
    "rst_tryopen": lambda r: r is True,
    "rst_type": lambda r: r == ["UInt16"] * 3,
    # single band
    "rst_boundingbox": lambda r: isinstance(r, (bytes, bytearray)) and len(r) > 0,
    "rst_crs": lambda r: isinstance(r, str) and "32618" in r,
    "rst_format": lambda r: r == "GTiff",
    "rst_georeference": lambda r: isinstance(r, dict) and r.get("scaleX") == 10.0,
    "rst_getnodata": lambda r: isinstance(r, list) and r[:1] == [0.0],
    "rst_height": lambda r: r == 161,
    "rst_memsize": lambda r: isinstance(r, int) and r > 0,
    "rst_metadata": lambda r: isinstance(r, dict) and r.get("driver") == "GTiff",
    "rst_pixelheight": _float(10.0),
    "rst_pixelwidth": _float(10.0),
    "rst_rotation": _float(0.0),
    "rst_scalex": _float(10.0),
    "rst_scaley": _float(-10.0),
    "rst_skewx": _float(0.0),
    "rst_skewy": _float(0.0),
    "rst_srid": lambda r: r == 32618,
    "rst_upperleftx": _float(2121950.0),
    "rst_upperlefty": _float(-10790470.0),
    "rst_width": lambda r: r == 236,
    # NetCDF: the extracted subdataset is materialized (bytes, no path)
    "rst_getsubdataset": lambda r: r is not None
    and r["raster"] is not None
    and len(bytes(r["raster"])) > 0,
    "rst_subdatasets": lambda r: isinstance(r, dict)
    and any("SUBDATASET" in k for k in r),
}


@pytest.fixture(autouse=True)
def _light_setup_views(spark):
    """Create the four light-tier Setup views + register pyrx so every example
    can read `spark.table("rasters")` etc. — mirroring the page's Setup section."""
    from databricks.labs.gbx import manager as gbx_manager
    from ._fixtures import create_setup_views_light  # noqa: PLC0415

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    create_setup_views_light(spark)


@pytest.mark.parametrize("name", sorted(CHECKS))
def test_python_example_meets_its_check(spark, name):
    assert light_examples is not None
    result = getattr(light_examples, f"{name}_python_example")(spark)
    assert CHECKS[name](result), f"{name}: unexpected result {result!r}"


def test_every_example_has_a_check_and_an_output():
    """The checks are the module's examples, and each example has its non-empty `*_output` constant."""
    assert light_examples is not None
    examples = {
        n[: -len("_python_example")]
        for n in dir(light_examples)
        if n.endswith("_python_example")
    }
    assert examples == set(CHECKS)
    for name in examples:
        out = getattr(light_examples, f"{name}_python_example_output", None)
        assert isinstance(out, str) and out.strip(), f"empty output constant: {name}"
