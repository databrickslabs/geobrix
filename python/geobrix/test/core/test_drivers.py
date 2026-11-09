"""``gbx_drivers``: the row of one driver, the policy columns, the kind filter, and the as-provided snapshot.

The row logic runs on fake drivers (no GDAL). The live tests need the env wheel's ``osgeo`` on rasterio's libgdal
(run with GBX_NATIVE_ROOT set to a built payload); they skip without it. The function's SQL and Python forms run
against the family oracle table DRIVERS (``test/manager/test_family_cross_tier.py``).

Refresh the snapshot of the env wheel's drivers after the shipped set changes:
    GBX_DRIVERS_SNAPSHOT=write GBX_NATIVE_ROOT=build/env-wheel/env6/payload \\
        gbx:test:python --path python/geobrix/test/core/test_drivers.py
then run gbx:manifest:gen --write (it renders the docs table from the snapshot).
"""

import json
import os

import pytest

from databricks.labs.gbx import _gdal_policy as policy
from databricks.labs.gbx.drivers import _listing as listing


class FakeDriver:
    def __init__(self, name, **md):
        self.ShortName = name
        self._md = md

    def GetMetadata(self):
        return dict(self._md)


class FakeGdal:
    def __init__(self, *drivers):
        self._d = drivers

    def GetDriverCount(self):
        return len(self._d)

    def GetDriver(self, i):
        return self._d[i]


def _live():
    from databricks.labs.gbx.engine import run

    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    from databricks.labs.gbx import _osgeo

    return _osgeo.bindings("test_drivers").gdal


# --- the row of one driver -----------------------------------------------------------------------------------------


def test_a_raster_driver_row_carries_gdals_flags_and_the_policy():
    d = FakeDriver(
        "AAIGrid",
        DCAP_RASTER="YES",
        DCAP_OPEN="YES",
        DCAP_CREATECOPY="YES",
        DMD_LONGNAME="Arc/Info ASCII Grid",
        DMD_EXTENSIONS="asc",
        DMD_HELPTOPIC="drivers/raster/aaigrid.html",
    )
    row = listing.driver_row(d, frozenset({"AAIGrid"}))
    assert row == {
        "driver": "AAIGrid",
        "description": "Arc/Info ASCII Grid",
        "raster": True,
        "vector": False,
        "read": True,
        "write": True,
        "extensions": ["asc"],
        "allowed": True,
        "tile_bytes": True,
        "available": True,
        "requires": None,
        "shipped": True,
        "help_url": "https://gdal.org/drivers/raster/aaigrid.html",
    }


def test_write_is_create_or_create_copy_and_read_is_open():
    only_create = listing.driver_row(
        FakeDriver("X", DCAP_VECTOR="YES", DCAP_CREATE="YES")
    )
    assert (only_create["read"], only_create["write"]) == (False, True)
    nothing = listing.driver_row(FakeDriver("Y", DCAP_VECTOR="YES", DCAP_OPEN="YES"))
    assert (nothing["read"], nothing["write"]) == (True, False)


def test_a_plugin_gdal_has_not_loaded_is_unavailable_and_says_what_to_install():
    msg = "This driver is available as a plugin. Install the gdal-foo package."
    d = FakeDriver(
        "FOO", DCAP_VECTOR="YES", DCAP_OPEN="YES", DMD_PLUGIN_INSTALLATION_MESSAGE=msg
    )
    row = listing.driver_row(d)
    assert row["available"] is False and row["requires"] == msg
    assert row["allowed"] is True  # the policy allows it; the plugin is what is missing


def test_a_connection_string_driver_is_unavailable_with_its_prefix():
    row = listing.driver_row(
        FakeDriver(
            "ES", DCAP_VECTOR="YES", DCAP_OPEN="YES", DMD_CONNECTION_PREFIX="ES:"
        )
    )
    assert row["available"] is False
    assert "'ES:'" in row["requires"]


def test_a_refused_driver_is_not_allowed_and_not_available(monkeypatch):
    monkeypatch.setattr(policy, "refused_names", lambda: ("WMS",))
    row = listing.driver_row(FakeDriver("WMS", DCAP_RASTER="YES", DCAP_OPEN="YES"))
    assert (row["allowed"], row["tile_bytes"], row["available"]) == (
        False,
        False,
        False,
    )


def test_the_policy_off_allows_everything(monkeypatch):
    monkeypatch.setenv(policy.ENV, "off")
    allowed, tile_bytes = policy.driver_verdict("WMS", True)
    assert (allowed, tile_bytes) == (True, True)


def test_the_policy_columns_follow_the_manifest_classes():
    assert policy.driver_verdict("GTiff", True) == (True, True)
    assert policy.driver_verdict("ENVI", True) == (True, False)  # the sidecar class
    assert policy.driver_verdict("WMS", True) == (False, False)  # refused
    assert policy.driver_verdict("GeoJSON", False) == (
        True,
        False,
    )  # vector: no tile bytes
    # a raster driver the manifest does not classify is not allowed
    assert policy.driver_verdict("NotAGdalDriver", True) == (False, False)


def test_a_sidecar_class_raster_driver_names_what_it_needs():
    d = FakeDriver("ENVI", DCAP_RASTER="YES", DCAP_OPEN="YES")
    row = listing.driver_row(d)
    assert row["available"] is True
    assert row["requires"] == policy.BYTES_CLASS_REQUIRES


# --- the listing ---------------------------------------------------------------------------------------------------


def test_the_kind_filter_keeps_one_kind_and_the_rows_are_sorted_once_each():
    gdal = FakeGdal(
        FakeDriver("b_vec", DCAP_VECTOR="YES"),
        FakeDriver("A_ras", DCAP_RASTER="YES"),
        FakeDriver("both", DCAP_RASTER="YES", DCAP_VECTOR="YES"),
        FakeDriver("A_ras", DCAP_RASTER="YES"),
    )
    assert [r["driver"] for r in listing.driver_rows(gdal)] == [
        "A_ras",
        "b_vec",
        "both",
    ]
    assert [r["driver"] for r in listing.driver_rows(gdal, "RASTER")] == [
        "A_ras",
        "both",
    ]
    assert [r["driver"] for r in listing.driver_rows(gdal, "vector")] == [
        "b_vec",
        "both",
    ]


def test_a_kind_that_is_neither_raises_with_a_fix():
    with pytest.raises(ValueError, match="Fix:"):
        listing.check_kind("table")


def test_help_url_is_absolute_or_null():
    assert (
        listing.help_url("drivers/vector/x.html")
        == "https://gdal.org/drivers/vector/x.html"
    )
    assert listing.help_url("https://example.org/x") == "https://example.org/x"
    assert listing.help_url(None) is None and listing.help_url("") is None


def test_without_the_env_wheels_osgeo_the_listing_raises_osgeo_unavailable(monkeypatch):
    from databricks.labs.gbx import _osgeo

    def refuse(fn):
        raise _osgeo.OsgeoUnavailable(f"{fn} needs osgeo. Fix: install the env wheel")

    monkeypatch.setattr(_osgeo, "bindings", refuse)
    with pytest.raises(_osgeo.OsgeoUnavailable, match="Fix:"):
        listing.list_drivers("raster")


# --- the snapshot of the env wheel's own build ---------------------------------------------------------------------


def test_the_snapshot_file_is_well_formed():
    doc = json.loads(listing.SNAPSHOT.read_text())
    names = [r["driver"] for r in doc["drivers"]]
    assert names == sorted(names, key=lambda n: (n.lower(), n)), "sorted by name"
    assert len(names) == len(set(names)), "each driver once"
    for r in doc["drivers"]:
        assert tuple(r) == listing.SNAPSHOT_COLUMNS


def test_the_live_drivers_are_the_snapshot_or_the_snapshot_is_refreshed():
    gdal = _live()
    rows = listing.driver_rows(
        gdal
    )  # activate() ran at import of the policy's first use
    from databricks.labs.gbx import _gdal_policy

    _gdal_policy.activate()
    rows = listing.driver_rows(gdal, None, listing.shipped_names())
    text = listing.snapshot_document(gdal.VersionInfo("RELEASE_NAME"), rows)
    if os.environ.get("GBX_DRIVERS_SNAPSHOT") == "write":
        listing.SNAPSHOT.write_text(text)
        listing.shipped_names.cache_clear()
        return
    assert listing.SNAPSHOT.read_text() == text, (
        "the drivers of this build differ from manifest/shipped_drivers.json. "
        "Fix: GBX_DRIVERS_SNAPSHOT=write gbx:test:python --path python/geobrix/test/core/test_drivers.py "
        "(with GBX_NATIVE_ROOT set to the env6 payload), then gbx:manifest:gen --write"
    )


def test_every_driver_of_this_build_is_shipped_by_definition():
    rows = listing.driver_rows(_live(), None, listing.shipped_names())
    assert all(r["shipped"] for r in rows)


def test_no_driver_of_the_build_is_a_deferred_plugin():
    """GDAL 3.9 and later list a plugin it has not loaded with DMD_PLUGIN_INSTALLATION_MESSAGE. The env wheel's
    build has none, so `requires` never carries a plugin message here; the plugin path is held on fakes above.
    """
    gdal = _live()
    stubs = [
        gdal.GetDriver(i).ShortName
        for i in range(gdal.GetDriverCount())
        if gdal.GetDriver(i).GetMetadataItem("DMD_PLUGIN_INSTALLATION_MESSAGE")
    ]
    assert stubs == []


def test_the_multi_file_vector_map_names_drivers_of_the_build():
    gdal = _live()
    registered = {gdal.GetDriver(i).ShortName for i in range(gdal.GetDriverCount())}
    assert set(policy.VECTOR_MULTI_FILE) <= registered


def test_a_refused_driver_is_not_registered_in_the_full_mode():
    gdal = _live()
    from databricks.labs.gbx import _gdal_policy

    if _gdal_policy.activate()["mode"] != "full":
        pytest.skip("the policy is not in the full mode here")
    names = {r["driver"] for r in listing.driver_rows(gdal)}
    assert not names & set(_gdal_policy.refused_names())
