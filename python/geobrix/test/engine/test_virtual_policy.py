"""The virtual-tile policy on the engine's L3 input path, without Spark (design section 4).

Each row feeds one virtual tile through ``l3._inputs`` (the engine's read of a virtual tile) and asserts the policy's
own reason, so a row cannot pass on some other error. The GDAL rows need the payload's osgeo (the container with
GBX_NATIVE_ROOT) and skip with the engine's reason elsewhere; the pure rows run on the host. The process-level rows (a
fresh process, the switch, the registry) are in test_driver_policy.py.
"""

import re

import pytest

from databricks.labs.gbx import _gdal_policy, manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.engine import l3, run

# The rows of the GDAL driver policy: a skip where the payload is present fails the meta-row in
# test_driver_policy.py.
pytestmark = pytest.mark.gdal_policy

MAN = manifest.load()
FN = MAN.functions["gbx_rst_slope"]

# GDAL 3.13.3's own messages for the policy's refusals, as the engine reads them (engine/run.py)
GDAL_MESSAGES = [
    (
        "ERROR 4: `/x/dem.gdalg.json' not recognized as being in a supported file format.",
        "a source GDAL has no driver for here",
    ),
    ("VRTRawRasterBand support has been disabled at run-time.", "a VRT raw band"),
    (
        "Python code in /x/py.vrt needs to be executed, but this has been explicitly disabled.",
        "a VRT Python pixel function",
    ),
    (
        "Python code needs to be executed, but it uses inline code in the VRT whereas the current policy",
        "a VRT Python pixel function",
    ),
]


@pytest.mark.parametrize(
    "message,what", GDAL_MESSAGES, ids=[m[1] for m in GDAL_MESSAGES]
)
def test_a_gdal_refusal_of_a_nested_source_names_the_policy_reason(message, what):
    assert run.policy_reason(message) == what


@pytest.mark.parametrize(
    "message",
    ["/x/missing.tif: No such file or directory", "not a raster GDAL can read", ""],
)
def test_an_error_that_is_no_policy_refusal_is_not_named_as_one(message):
    assert run.policy_reason(message) is None


def test_a_deregistered_prefix_reported_as_missing_is_the_policy_reason(monkeypatch):
    """GDAL reports a deregistered driver's name (GTI:/path) as a missing file: that one is the policy's, a missing
    file under an allowed name is not."""
    monkeypatch.setattr(_gdal_policy, "_DEREGISTERED_PREFIXES", {"GTI:"})
    for message in (
        "CPLE_OpenFailedError: GTI:/tmp/se_index.gpkg: No such file or directory",
        "ERROR 4: `gti:/tmp/se_index.gpkg': No such file or directory",
    ):
        assert run.policy_reason(message) == "a source GDAL has no driver for here"
    assert (
        run.policy_reason(
            "CPLE_OpenFailedError: /tmp/se_index.gpkg: No such file or directory"
        )
        is None
    )


@pytest.mark.parametrize(
    "message",
    [
        "/x/missing.mrf: No such file or directory",
        "ERROR 4: /x/tiles_wms: No such file or directory",
        "CPLE_OpenFailedError: /x/a.gti: No such file or directory",
        "/vsicurl/http://h/a.tif: No such file or directory",
        "CPLE_OpenFailedError: http://h/a.tif: No such file or directory",
        "ERROR 4: `HTTP://h/a.tif': No such file or directory",
    ],
)
def test_a_missing_source_named_like_a_refused_prefix_is_no_policy_refusal(
    monkeypatch, message
):
    """A nested source that is missing, whose name only ends like a refused driver's prefix, is a missing source: the
    prefix counts where a name starts, and a URL (``http://``) is no connection string wherever it starts.
    """
    monkeypatch.setattr(
        _gdal_policy, "_DEREGISTERED_PREFIXES", {"MRF:", "WMS:", "GTI:", "HTTP:"}
    )
    assert run.policy_reason(message) is None


def test_the_engine_settings_are_thread_local_and_restored():
    """The engine's settings apply to the calling thread for the block, and only that (the process keeps its own)."""
    _skip_without_gdal()
    gdal = run._gdal()
    keys = list(run.ENGINE_SETTINGS)
    before = {k: gdal.GetThreadLocalConfigOption(k, None) for k in keys}
    with run.engine_settings():
        for k, v in run.ENGINE_SETTINGS.items():
            assert gdal.GetThreadLocalConfigOption(k, None) == v, k
    assert {k: gdal.GetThreadLocalConfigOption(k, None) for k in keys} == before


def test_the_allowlist_is_the_allowed_class_and_is_not_the_refused_one():
    """The engine's virtual-tile allowlist is the manifest's allowed class: a refused driver is never on it."""
    assert set(run.virtual_drivers()) == set(MAN.gdal.allowed_drivers)
    assert not set(run.virtual_drivers()) & set(run.refused_drivers())


def _skip_without_gdal():
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)


# Each refusal, as a virtual tile: the fixture, the reason the policy gives (oracles.py names the same texts).
POLICY_ROWS = [
    ("dem_gdalg", oracles._VT_TOP),  # a stored pipeline, as the tile's own file
    ("dem_vrt_gdalg", oracles._VT_NESTED),  # a VRT whose source is the pipeline
    ("dem_inline_gdalg", oracles._VT_NESTED),  # the same, the source inline in the VRT
    (
        "dem_vrt_gti_gdalg",
        oracles._VT_NESTED,
    ),  # a VRT whose source is a tile index naming the pipeline
    ("dem_gti", oracles._VT_TOP),  # a tile index, as the tile's own file
    ("dem_stacta", oracles._VT_TOP),  # a STAC tiled-asset catalog
    (
        "dem_eedai",
        oracles._VT_TOP,
    ),  # a network API's connection string: refused before any connect
    ("dem_vrt_python", f"a VRT Python pixel function, {oracles._VT_POLICY}"),
    ("dem_vrt_raw", f"a VRT raw band, {oracles._VT_POLICY}"),
    (
        "dem_inline_raw",
        f"a VRT raw band, {oracles._VT_POLICY}",
    ),  # a sibling file, inline
]


@pytest.mark.parametrize("fixture,reason", POLICY_ROWS, ids=[r[0] for r in POLICY_ROWS])
def test_a_refused_virtual_tile_gives_the_policy_reason(tmp_path, fixture, reason):
    _skip_without_gdal()
    tile = oracles._build(fixture, str(tmp_path))
    with pytest.raises(run.DataError, match=re.escape(reason)):
        l3._inputs(FN, tile)


@pytest.mark.parametrize("spelling", ["", "file:"], ids=["bare", "file-scheme"])
def test_a_valid_virtual_tile_is_still_read(tmp_path, spelling):
    """The engine opens the path every light read opens: a file:/ spelling of the tile's path is the same file."""
    _skip_without_gdal()
    tile = dict(oracles._build("dem_vrt", str(tmp_path)))
    tile["path"] = spelling + tile["path"]
    rasters, _p, _c = l3._inputs(FN, tile)
    assert rasters and bytes(rasters[0][:4]) in run._TIFF_MAGIC


def test_a_png_virtual_tile_is_read_because_GeoBrix_reads_PNG(tmp_path):
    """The allowed class is every raster driver a reader can read, so a PNG (not a GeoTIFF, not a VRT) is a
    raster GeoBrix opens: the tile is read, not refused. (The earlier allowlist refused it; the classification does not.)
    """
    _skip_without_gdal()
    rasters, _p, _c = l3._inputs(FN, oracles._build("dem_png", str(tmp_path)))
    assert rasters and bytes(rasters[0][:4]) in run._TIFF_MAGIC


def test_the_python_setting_refuses_a_pixel_function_even_when_the_process_allows_it(
    tmp_path,
):
    """GDAL's default refuses inline Python already, so the setting is the barrier only where the process allows it
    (the JVM's withVrtPython does, for a few calls). Removed from ENGINE_SETTINGS, this row would read the function.
    """
    _skip_without_gdal()
    gdal = run._gdal()
    key = "GDAL_VRT_ENABLE_PYTHON"
    prev = gdal.GetConfigOption(key)
    gdal.SetConfigOption(key, "YES")
    try:
        with pytest.raises(run.DataError, match=re.escape(oracles._VT_POLICY)):
            l3._inputs(FN, oracles._build("dem_vrt_python", str(tmp_path)))
    finally:
        gdal.SetConfigOption(key, prev)
