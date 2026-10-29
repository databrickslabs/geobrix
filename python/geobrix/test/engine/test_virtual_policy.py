"""The virtual-tile driver policy (design section 4; AP-40, I-2, C-1) on the engine's L3 input path, without Spark.

Each row feeds one virtual tile through ``l3._inputs`` (the engine's read of a virtual tile) and asserts the policy's
own reason, so a row cannot pass on some other error. The GDAL rows need the payload's osgeo (the container with
GBX_NATIVE_ROOT) and skip with the engine's reason elsewhere; the pure rows run on the host.
"""

import re

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.engine import l3, run

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
    monkeypatch.setattr(run, "_deregistered_prefixes", {"GTI:"})
    message = "CPLE_OpenFailedError: GTI:/tmp/se_index.gpkg: No such file or directory"
    assert run.policy_reason(message) == "a source GDAL has no driver for here"
    assert (
        run.policy_reason(
            "CPLE_OpenFailedError: /tmp/se_index.gpkg: No such file or directory"
        )
        is None
    )


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


class _FakeDriver:
    def __init__(self, name, log):
        self.name, self.log = name, log

    def GetMetadataItem(self, key):
        return "GTI:" if self.name == "GTI" and key == "DMD_CONNECTION_PREFIX" else None

    def Deregister(self):
        self.log.append(self.name)


class _FakeGdal:
    DCAP_RASTER = "DCAP_RASTER"

    def __init__(self, present):
        self.log = []
        self._drivers = {n: _FakeDriver(n, self.log) for n in present}

    def GetDriverByName(self, name):
        drv = self._drivers.get(name)
        return drv


@pytest.fixture
def fresh_policy():
    """apply_driver_policy runs once per process; each test here starts from a clear cache and clears it after."""
    run.apply_driver_policy.cache_clear()
    yield
    run.apply_driver_policy.cache_clear()


def test_the_policy_deregisters_each_refused_driver_present_once(
    monkeypatch, fresh_policy
):
    refused = list(MAN.gdal.refused_drivers)
    fake = _FakeGdal(
        refused[:3] + ["GTiff"]
    )  # three refused drivers registered, and a driver that is not refused
    monkeypatch.delenv(run.POLICY_OPT_OUT, raising=False)
    monkeypatch.setattr(run, "_gdal", lambda: fake)
    run.apply_driver_policy()
    run.apply_driver_policy()  # the second call is a no-op: applied once
    assert sorted(fake.log) == sorted(refused[:3])


def test_the_opt_out_leaves_the_registry_alone(monkeypatch, fresh_policy):
    fake = _FakeGdal(list(MAN.gdal.refused_drivers))
    monkeypatch.setenv(run.POLICY_OPT_OUT, "off")
    monkeypatch.setattr(run, "_gdal", lambda: fake)
    run.apply_driver_policy()
    assert fake.log == []


def test_the_allowlist_is_not_the_refuse_list_and_names_only_its_drivers():
    """The virtual-tile policy is an allowlist (I-2): a driver on neither list is refused, not allowed."""
    allowed = set(run.virtual_drivers())
    assert allowed == {"GTiff", "VRT"}
    assert not allowed & set(run.refused_drivers())


def _skip_without_gdal():
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)


# Each refusal, as a virtual tile: the fixture, the reason the policy gives (oracles.py names the same texts).
POLICY_ROWS = [
    ("dem_gdalg", oracles._VT_TOP),  # a stored pipeline, as the tile's own file
    ("dem_vrt_gdalg", oracles._VT_NESTED),  # a VRT whose source is the pipeline
    (
        "dem_inline_gdalg",
        oracles._VT_NESTED,
    ),  # the same, the source inline in the VRT (no walk sees it)
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
    (
        "dem_png",
        f"a PNG file, {oracles._VT_POLICY}",
    ),  # a driver GeoBrix emits no virtual tile for
    ("dem_vrt_python", f"a VRT Python pixel function, {oracles._VT_POLICY}"),
    ("dem_vrt_raw", f"a VRT raw band, {oracles._VT_POLICY}"),
    (
        "dem_inline_raw",
        f"a VRT raw band, {oracles._VT_POLICY}",
    ),  # the reviewer's case: a sibling file, inline
]


@pytest.mark.parametrize("fixture,reason", POLICY_ROWS, ids=[r[0] for r in POLICY_ROWS])
def test_a_refused_virtual_tile_gives_the_policy_reason(tmp_path, fixture, reason):
    _skip_without_gdal()
    tile = oracles._build(fixture, str(tmp_path))
    with pytest.raises(run.DataError, match=re.escape(reason)):
        l3._inputs(FN, tile)


def test_a_valid_virtual_tile_is_still_read(tmp_path):
    _skip_without_gdal()
    rasters, _p, _c = l3._inputs(FN, oracles._build("dem_vrt", str(tmp_path)))
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
