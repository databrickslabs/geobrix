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
        "tested": True,
        "tested_with": "raster_gbx (read), gbx_rst_asformat (write)",
        "guidance": "raster_gbx reads and writes via gbx_rst_asformat AAIGrid; the format is detected from the file. It has no named reader or writer.",
        "docs": [
            "https://databrickslabs.github.io/geobrix/docs/readers/raster",
            "https://databrickslabs.github.io/geobrix/docs/api/raster-functions",
        ],
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


def test_an_in_process_driver_says_it_lives_in_memory_not_that_it_needs_files():
    """MEM and NUMPY hold their dataset in the process: no file is missing, and the bytes cannot be a tile."""
    for name in ("MEM", "NUMPY"):
        requires, usable = policy.driver_requires(name, {"DCAP_RASTER": "YES"})
        assert usable
        assert requires == policy.IN_PROCESS_REQUIRES
        assert "in-process" in requires and "tile bytes" in requires
        assert "file" not in requires and "sidecar" not in requires
    # the others of the class keep the note about other files
    requires, _ = policy.driver_requires("ENVI", {"DCAP_RASTER": "YES"})
    assert requires == policy.BYTES_CLASS_REQUIRES


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


def test_guidance_and_docs_follow_the_kind_filter_for_a_dual_kind_driver():
    """A driver of both kinds gets the note and the docs links of the kind the listing was asked for, as ``tested`` and
    ``tested_with`` do: the vector note of GPKG does not hold for its raster side, and the other way round.
    """
    gdal = FakeGdal(
        FakeDriver("GPKG", DCAP_RASTER="YES", DCAP_VECTOR="YES", DCAP_OPEN="YES"),
        FakeDriver("MBTiles", DCAP_RASTER="YES", DCAP_VECTOR="YES", DCAP_OPEN="YES"),
        FakeDriver("PCIDSK", DCAP_RASTER="YES", DCAP_VECTOR="YES", DCAP_OPEN="YES"),
    )
    by = {
        k: {r["driver"]: r for r in listing.driver_rows(gdal, k)}
        for k in (None, "raster", "vector")
    }
    gpkg = {k: by[k]["GPKG"] for k in by}
    assert gpkg["vector"]["guidance"].startswith(
        "gpkg_gbx reads and writes GeoPackage vector layers, on this driver."
    )
    assert gpkg["raster"]["guidance"].startswith(
        "raster_gbx reads GPKG; the format is detected from the file."
    )
    assert "gpkg_gbx reads" not in gpkg["raster"]["guidance"]
    assert gpkg["raster"]["docs"] == [
        "https://databrickslabs.github.io/geobrix/docs/readers/raster"
    ]
    assert gpkg["vector"]["docs"][0].endswith("/readers/geopackage")
    # MBTiles (no entry link of the other kind is kept): each kind lists its own reader page only
    assert by["raster"]["MBTiles"]["guidance"].count("_gbx reads") == 1
    assert by["vector"]["MBTiles"]["guidance"].count("_gbx reads") == 1
    assert not any(
        u.endswith("readers/vector") for u in by["raster"]["MBTiles"]["docs"]
    )
    assert not any(
        u.endswith("readers/raster") for u in by["vector"]["MBTiles"]["docs"]
    )
    # an untested driver: one kind's "not tested" sentence, not both
    assert by["raster"]["PCIDSK"]["guidance"].startswith("Not tested with GeoBrix:")
    assert "vector_gbx" not in by["raster"]["PCIDSK"]["guidance"]
    # the unfiltered listing is unchanged: every kind
    assert "Not tested as a raster driver" in by[None]["PCIDSK"]["guidance"]
    assert "Not tested as a vector driver" in by[None]["PCIDSK"]["guidance"]


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
    """The opt-in section of the Other drivers page (generated from the snapshot) lists exactly the policy's refused
    drivers, under the title and anchor the Security page links to; the parent page has no such section.
    """
    import re
    from pathlib import Path

    snap = json.loads(listing.SNAPSHOT.read_text())["opt_in"]
    assert {r["driver"] for r in snap} == set(policy.refused_names())
    root = Path(__file__).resolve().parents[4]
    page = (root / "docs/docs/gdal-drivers/other.mdx").read_text()
    parent = (root / "docs/docs/gdal-drivers.mdx").read_text()
    assert "\n## GeoBrix Opt-In Drivers\n" not in parent
    assert "\n## GeoBrix Opt-In Drivers\n" in page
    section = page.split("\n## GeoBrix Opt-In Drivers\n", 1)[1]
    listed = set(re.findall(r"^\| `([A-Za-z0-9_]+)` \|", section, re.M))
    assert listed == set(policy.refused_names())
    security = (root / "docs/docs/security.mdx").read_text()
    assert "(./gdal-drivers/other#geobrix-opt-in-drivers)" in security
    assert "\n#### The policy covers the whole process\n" in security
    assert "(../security#the-policy-covers-the-whole-process)" in page


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


def test_a_driver_with_a_built_in_path_names_it_in_its_guidance():
    """There is no built_in column: the guidance of each such driver names the GeoBrix reader or function instead."""
    import re

    from databricks.labs.gbx.drivers import _builtin

    assert "built_in" not in listing.driver_row(
        FakeDriver("Zarr", DCAP_RASTER="YES", DCAP_OPEN="YES")
    )
    assert "built_in" not in {n for n, _ in listing.COLUMNS}
    rows = {r["driver"]: r for r in json.loads(listing.SNAPSHOT.read_text())["drivers"]}
    for f in _builtin.BUILT_IN_FORMATS:
        if f["driver"]:
            first = re.search(r"`(\w+)`", f["where"]).group(1)
            assert first in rows[f["driver"]]["guidance"], f["driver"]


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
    assert any(f["driver"] is None for f in formats), "LAS and LAZ have no GDAL driver"
    assert not {"GTiff", "COG", "GeoJSON", "GPKG", "ESRI Shapefile"} & {
        f["driver"] for f in formats
    }
    readers = {d.name for d in manifest.load().datasources}
    functions = set(manifest.load().functions)
    helpers = {
        "gbx_zarr_read": "databricks.labs.gbx.zarr._read",
        "gbx_zarr_write": "databricks.labs.gbx.zarr._write",
        "pmtiles_info": "databricks.labs.gbx.pmtiles._inspect",
    }
    for f in formats:
        for text in (f["where"],):
            for token in re.findall(r"\b\w+_gbx\b|\bgbx_\w+\b|\bpmtiles_info\b", text):
                if token in helpers:
                    assert hasattr(importlib.import_module(helpers[token]), token)
                else:
                    assert token in readers or token in functions, (token, text)
        for text in (f["uses"], f["where"]):
            assert all(len(x.split()) <= 25 for x in re.split(r"(?<=\.) ", text)), text


def test_the_docs_pages_link_every_driver_row_and_every_built_in_anchor():
    """gdal-drivers.mdx and its three subpages (generated from the snapshot): each link into one of the four pages has
    its target anchor there, the available drivers come before the unavailable ones, and a driver of both kinds is
    in both tables, each row linking to the other page; the parent's built-in rows link to rows of the subpages.
    """
    import posixpath
    import re
    from pathlib import Path

    docs = Path(__file__).resolve().parents[4] / "docs/docs"
    urls = {
        "gdal-drivers": docs / "gdal-drivers.mdx",
        "gdal-drivers/vector": docs / "gdal-drivers/vector.mdx",
        "gdal-drivers/raster": docs / "gdal-drivers/raster.mdx",
        "gdal-drivers/other": docs / "gdal-drivers/other.mdx",
    }
    pages = {u: f.read_text() for u, f in urls.items()}

    def slug(h):
        return re.sub(r"[^a-z0-9]+", "-", h.lower()).strip("-")

    anchors = {}
    for u, page in pages.items():
        ids = re.findall(r'<Link id="([^"]+)"></Link>', page)
        assert len(ids) == len(set(ids)), f"{u}: an anchor is used twice"
        anchors[u] = set(ids) | {
            slug(h) for h in re.findall(r"^#{1,6} (.+)$", page, re.M)
        }
    for u, page in pages.items():
        for target in re.findall(r"\]\(([^)\s]+)\)", page):
            path, _, frag = target.partition("#")
            if path.startswith("https://"):
                continue
            dest = (
                posixpath.normpath(posixpath.join(posixpath.dirname(u), path))
                if path
                else u
            )
            if dest not in anchors:
                continue  # a page outside the four: the docs build checks it
            assert not frag or frag in anchors[dest], f"{u}: no anchor {target}"
    vector, raster = pages["gdal-drivers/vector"], pages["gdal-drivers/raster"]
    for page, kind in ((vector, "vector"), (raster, "raster")):
        order = [
            page.index(h)
            for h in (
                f"## Available {kind} drivers: tested with GeoBrix",
                f"## Available {kind} drivers: not tested with GeoBrix",
                f"## Unavailable {kind} drivers",
            )
        ]
        assert order == sorted(order), kind
    drivers = json.loads(listing.SNAPSHOT.read_text())["drivers"]
    for d in drivers:
        name = slug(d["driver"])
        assert (f"vector-{name}" in anchors["gdal-drivers/vector"]) == bool(
            d["vector"]
        ), d["driver"]
        assert (f"raster-{name}" in anchors["gdal-drivers/raster"]) == bool(
            d["raster"]
        ), d["driver"]
        if d["vector"] and d["raster"]:
            assert f"(./raster#raster-{name})" in vector, d["driver"]
            assert f"(./vector#vector-{name})" in raster, d["driver"]


# --- the generated driver pages: counts, Can cells and empty sections ----------------------------------------------


def _surfaces():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[4] / "scripts/manifest/surfaces.py"
    spec = importlib.util.spec_from_file_location("gbx_surfaces_for_drivers_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _cells(line: str) -> list:
    import re

    return re.split(r"(?<!\\) \| ", line.strip()[2:-2])


def _driver_rows(page: str) -> list:
    """The table rows of a driver page (a row starts with its anchor), as cell lists."""
    return [
        _cells(line) for line in page.splitlines() if line.startswith('| <Link id="')
    ]


def test_the_parent_index_counts_equal_the_rows_of_the_subpages():
    """gdal-drivers.mdx says how many drivers each page lists, how many are tested, untested and unavailable: those are
    the numbers of rows in the sections of the subpage, counted from the generated text and not from the snapshot.
    """
    import re
    from pathlib import Path

    docs = Path(__file__).resolve().parents[4] / "docs/docs"
    parent = (docs / "gdal-drivers.mdx").read_text()
    for kind in ("vector", "raster"):
        page = (docs / f"gdal-drivers/{kind}.mdx").read_text()
        sections = re.split(r"^## ", page, flags=re.M)
        count = {}
        for sec in sections:
            for key in (
                "tested with GeoBrix",
                "not tested with GeoBrix",
                "Unavailable",
            ):
                head = sec.split("\n", 1)[0]
                if key == "tested with GeoBrix" and head.startswith(
                    f"Available {kind} drivers: tested"
                ):
                    count["tested"] = len(_driver_rows(sec))
                elif key == "not tested with GeoBrix" and head.startswith(
                    f"Available {kind} drivers: not tested"
                ):
                    count["rest"] = len(_driver_rows(sec))
                elif key == "Unavailable" and head.startswith(
                    f"Unavailable {kind} drivers"
                ):
                    count["down"] = len(_driver_rows(sec))
        assert set(count) == {"tested", "rest", "down"}, (kind, count)
        m = re.search(
            rf"^- \[[^\]]+\]\(\./gdal-drivers/{kind}\): (\d+) drivers, (\d+) tested with GeoBrix, "
            r"(\d+) available and not tested, (\d+) unavailable\.$",
            parent,
            re.M,
        )
        assert m, f"no index line for the {kind} page"
        total, tested, rest, down = map(int, m.groups())
        assert (tested, rest, down) == (
            count["tested"],
            count["rest"],
            count["down"],
        ), kind
        assert total == tested + rest + down == len(_driver_rows(page)), kind
    other = (docs / "gdal-drivers/other.mdx").read_text()
    section = other.split("## Other drivers", 1)[1].split("\n## ", 1)[0]
    n = len(re.findall(r"^\| `", section, re.M))
    m = re.search(
        r"\(\./gdal-drivers/other\): (\d+) drivers that handle neither", parent
    )
    assert m and int(m.group(1)) == n


def test_each_can_cell_of_the_driver_pages_is_the_snapshot_flags():
    """The Can cell of a row is read, write and (raster page) tile bytes, as the snapshot's flags say."""
    import re
    from pathlib import Path

    docs = Path(__file__).resolve().parents[4] / "docs/docs"
    snap = {d["driver"]: d for d in json.loads(listing.SNAPSHOT.read_text())["drivers"]}

    def want(d, kind):
        can = [w for w in ("read", "write") if d[w]]
        if kind == "raster" and d["tile_bytes"]:
            can.append("tile bytes")
        return " · ".join(can)

    seen = set()
    for kind in ("vector", "raster", "other"):
        page = (docs / f"gdal-drivers/{kind}.mdx").read_text()
        page = page.split("## GeoBrix Opt-In Drivers", 1)[0]
        if kind == "other":
            rows = [
                _cells(line)
                for line in page.splitlines()
                if re.match(r"^\| `[^`]+` \|", line)
            ]
            pairs = [(r[0].strip("`"), r[2]) for r in rows]
        else:
            pairs = [
                (re.search(r"</Link>`([^`]+)`", r[0]).group(1), r[2])
                for r in _driver_rows(page)
            ]
        for name, can in pairs:
            assert can == want(snap[name], "other" if kind == "other" else kind), (
                kind,
                name,
            )
            seen.add((kind, name))
    assert seen  # the cells were read
    for name, d in snap.items():
        for kind in ("vector", "raster"):
            if d[kind]:
                assert (kind, name) in seen, (kind, name)


def _snapshot(*drivers, opt_in=()):
    base = dict(
        description="d",
        raster=False,
        vector=False,
        read=True,
        write=False,
        allowed=True,
        tile_bytes=False,
        available=True,
        requires=None,
        extensions=[],
    )
    return {"drivers": [{**base, **d} for d in drivers], "opt_in": list(opt_in)}


def test_the_other_index_entry_is_always_listed_and_an_empty_section_says_none():
    s = _surfaces()
    only_vector = _snapshot({"driver": "V", "vector": True})
    parent = s.render_drivers_page(only_vector)
    assert "(./gdal-drivers/other): 0 drivers that handle neither" in parent
    other = s.render_drivers_subpage(only_vector, "other")
    assert "## Other drivers\n\nNone.\n" in other
    assert "| Driver | Description | Can |" not in other  # no header-only table
    assert "## GeoBrix Opt-In Drivers" in other
    assert other.rstrip().endswith(
        "None."
    )  # no opt-in rows: no header-only table either
    with_other = _snapshot({"driver": "M"}, {"driver": "V", "vector": True})
    other = s.render_drivers_subpage(with_other, "other")
    assert "| Driver | Description | Can |" in other and "None.\n\n## " not in other
    # an empty group of a vector page says so
    vec = s.render_drivers_subpage(only_vector, "vector")
    assert (
        "## Unavailable vector drivers\n\nNo vector driver is unavailable in the env wheel."
        in vec
    )
    assert "## Available vector drivers: tested with GeoBrix\n\nNone.\n" in vec


# --- opt_in_rows and the snapshot document --------------------------------------------------------------------------


class _ByNameGdal:
    """A gdal module with only ``GetDriverByName``: the names given are registered, any other is not."""

    def __init__(self, **drivers):
        self._d = drivers

    def GetDriverByName(self, name):
        return self._d.get(name)


def test_opt_in_rows_for_a_driver_the_build_lacks_has_no_description_and_no_kinds(
    monkeypatch,
):
    monkeypatch.setattr(policy, "refused_names", lambda: ["WMS", "ZZ_PRESENT"])
    monkeypatch.setattr(
        policy,
        "REFUSED_WHY",
        {
            "WMS": ("network", "It reaches a web service."),
            "ZZ_PRESENT": ("pipeline", "It runs a stored pipeline."),
        },
    )
    present = FakeDriver(
        "ZZ_PRESENT", DCAP_RASTER="YES", DCAP_VECTOR="YES", DMD_LONGNAME="Present one"
    )
    rows = listing.opt_in_rows(_ByNameGdal(ZZ_PRESENT=present))
    assert rows == [
        {
            "driver": "WMS",
            "description": None,  # not in this build: no metadata to read
            "raster": False,
            "vector": False,
            "in_build": False,
            "class": "network",
            "why": "It reaches a web service.",
        },
        {
            "driver": "ZZ_PRESENT",
            "description": "Present one",
            "raster": True,
            "vector": True,
            "in_build": True,
            "class": "pipeline",
            "why": "It runs a stored pipeline.",
        },
    ]


def test_opt_in_rows_for_a_driver_without_a_long_name_has_no_description(monkeypatch):
    monkeypatch.setattr(policy, "refused_names", lambda: ["NOLONG"])
    monkeypatch.setattr(policy, "REFUSED_WHY", {"NOLONG": ("network", "Why.")})
    rows = listing.opt_in_rows(
        _ByNameGdal(NOLONG=FakeDriver("NOLONG", DCAP_VECTOR="YES"))
    )
    assert rows[0]["description"] is None and rows[0]["in_build"] is True
    assert (rows[0]["raster"], rows[0]["vector"]) == (False, True)


def test_the_snapshot_document_with_no_opt_in_driver_has_an_empty_list():
    row = {c: None for c in listing.SNAPSHOT_COLUMNS}
    row.update(driver="X", extensions=[], docs=[])
    for opt_in in (None, []):
        doc = json.loads(listing.snapshot_document("3.x", [row], opt_in))
        assert doc["opt_in"] == [] and [d["driver"] for d in doc["drivers"]] == ["X"]
