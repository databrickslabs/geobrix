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
        "built_in": None,
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


def _opt_in_of_the_build():
    """The refused drivers as a process with the policy OFF sees them (they are not registered otherwise)."""
    import subprocess
    import sys

    code = (
        "import json; from databricks.labs.gbx import _osgeo; "
        "from databricks.labs.gbx.drivers import _listing; "
        "print(json.dumps(_listing.opt_in_rows(_osgeo.bindings('opt_in').gdal)))"
    )
    # the parent's GeoBrix import put the refused names in GDAL_SKIP: a child inherits it, so it is dropped here
    env = {k: v for k, v in os.environ.items() if k != "GDAL_SKIP"}
    env[policy.ENV] = "off"
    out = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )
    assert out.returncode == 0, out.stderr[-800:]
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_the_live_drivers_are_the_snapshot_or_the_snapshot_is_refreshed():
    gdal = _live()
    from databricks.labs.gbx import _gdal_policy

    _gdal_policy.activate()  # the refused drivers are deregistered before the registry is read
    rows = listing.driver_rows(gdal, None, listing.shipped_names())
    text = listing.snapshot_document(
        gdal.VersionInfo("RELEASE_NAME"), rows, _opt_in_of_the_build()
    )
    if os.environ.get("GBX_DRIVERS_SNAPSHOT") == "write":
        listing.SNAPSHOT.write_text(text)
        listing.shipped_names.cache_clear()
        return
    assert listing.SNAPSHOT.read_text() == text, (
        "the drivers of this build differ from manifest/shipped_drivers.json. "
        "Fix: GBX_DRIVERS_SNAPSHOT=write gbx:test:python --path python/geobrix/test/core/test_drivers.py "
        "(with GBX_NATIVE_ROOT set to the env6 payload), then gbx:manifest:gen --write"
    )


def test_every_refused_driver_has_a_reason_class_and_a_sentence():
    assert set(policy.REFUSED_WHY) == set(policy.refused_names())
    for name, (cls, why) in policy.REFUSED_WHY.items():
        assert cls and why.endswith("."), name
        assert len(why.split()) <= 25, name


def test_the_snapshot_lists_the_refused_set_and_the_opt_in_page_matches_it():
    """The opt-in section of gdal-drivers.mdx (generated from the snapshot) lists exactly the policy's refused
    drivers, under the title and anchor the Security page links to."""
    import re
    from pathlib import Path

    snap = json.loads(listing.SNAPSHOT.read_text())["opt_in"]
    assert {r["driver"] for r in snap} == set(policy.refused_names())
    root = Path(__file__).resolve().parents[4]
    page = (root / "docs/docs/gdal-drivers.mdx").read_text()
    assert "\n## GeoBrix Opt-In Drivers\n" in page
    section = page.split("\n## GeoBrix Opt-In Drivers\n", 1)[1]
    listed = set(re.findall(r"^\| `([A-Za-z0-9_]+)` \|", section, re.M))
    assert listed == set(policy.refused_names())
    security = (root / "docs/docs/security.mdx").read_text()
    assert "(./gdal-drivers#geobrix-opt-in-drivers)" in security
    assert "\n#### The policy covers the whole process\n" in security
    assert "(./security#the-policy-covers-the-whole-process)" in page


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


# --- the formats GeoBrix handles without the GDAL driver ------------------------------------------------------------


def test_a_driver_with_a_built_in_path_carries_its_note():
    row = listing.driver_row(FakeDriver("Zarr", DCAP_RASTER="YES", DCAP_OPEN="YES"))
    assert row["built_in"] == listing.BUILT_IN["Zarr"]
    assert (
        listing.driver_row(FakeDriver("GTiff", DCAP_RASTER="YES"))["built_in"] is None
    )


def test_the_built_in_formats_name_drivers_of_the_build_and_readers_of_the_source():
    import importlib
    import re

    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.drivers import _builtin

    registered = {
        r["driver"] for r in json.loads(listing.SNAPSHOT.read_text())["drivers"]
    }
    formats = _builtin.BUILT_IN_FORMATS
    anchors = [f["anchor"] for f in formats]
    assert len(set(anchors)) == len(anchors) and all(
        re.fullmatch(r"builtin-[a-z0-9]+", a) for a in anchors
    )
    assert {f["driver"] for f in formats if f["driver"]} <= registered
    assert set(listing.BUILT_IN) == {f["driver"] for f in formats if f["driver"]}
    assert any(f["driver"] is None for f in formats), "LAS and LAZ have no GDAL driver"
    assert not {"GTiff", "COG", "GeoJSON", "GPKG", "ESRI Shapefile"} & set(
        listing.BUILT_IN
    )
    readers = {d.name for d in manifest.load().datasources}
    functions = set(manifest.load().functions)
    helpers = {
        "gbx_zarr_read": "databricks.labs.gbx.zarr._read",
        "gbx_zarr_write": "databricks.labs.gbx.zarr._write",
        "pmtiles_info": "databricks.labs.gbx.pmtiles._inspect",
    }
    for f in formats:
        for text in (f["where"], f["note"] or ""):
            for token in re.findall(r"\b\w+_gbx\b|\bgbx_\w+\b|\bpmtiles_info\b", text):
                if token in helpers:
                    assert hasattr(importlib.import_module(helpers[token]), token)
                else:
                    assert token in readers or token in functions, (token, text)
        for text in (f["note"] or "", f["uses"], f["where"]):
            assert all(len(x.split()) <= 25 for x in re.split(r"(?<=\.) ", text)), text


def test_the_docs_page_links_every_driver_row_and_every_built_in_anchor():
    """gdal-drivers.mdx (generated from the snapshot): each link to an anchor of the page has its target, the
    available drivers come before the unavailable ones, and a driver of both kinds is in both tables.
    """
    import re
    from pathlib import Path

    page = (
        Path(__file__).resolve().parents[4] / "docs/docs/gdal-drivers.mdx"
    ).read_text()
    ids = re.findall(r'<Link id="([^"]+)"></Link>', page)
    assert len(ids) == len(set(ids)), "an anchor is used twice"
    headings = {
        re.sub(r"[^a-z0-9]+", "-", h.lower()).strip("-")
        for h in re.findall(r"^#{1,6} (.+)$", page, re.M)
    }
    for target in re.findall(r"\]\(#([^)]+)\)", page):
        assert target in ids or target in headings, f"no anchor {target}"
    order = [
        page.index(h)
        for h in (
            "## Formats GeoBrix handles without a GDAL driver",
            "### Available vector drivers",
            "### Unavailable vector drivers",
            "### Available raster drivers",
            "### Unavailable raster drivers",
        )
    ]
    assert order == sorted(order)
    drivers = json.loads(listing.SNAPSHOT.read_text())["drivers"]
    for d in drivers:
        name = re.sub(r"[^a-z0-9]+", "-", d["driver"].lower()).strip("-")
        assert (f"vector-{name}" in ids) == bool(d["vector"]), d["driver"]
        assert (f"raster-{name}" in ids) == bool(d["raster"]), d["driver"]
