"""The guidance of ``gbx_drivers`` (``databricks.labs.gbx.drivers._guidance``): every GeoBrix name it mentions is in the
manifest, every link is a page of the docs, every sentence is short, and what a note says about a test is true of the
registry or of a recorded finding. The note of a driver with no entry is built from the registry, so it cannot say
a driver works when no test shows it.
"""

import json
import re
from pathlib import Path
from test.drivers_extended import _raster as R
from test.drivers_extended import _vector as V

from databricks.labs.gbx import manifest
from databricks.labs.gbx.drivers import _guidance as G
from databricks.labs.gbx.drivers import _listing
from databricks.labs.gbx.drivers._tested import TESTED_DRIVERS

DOCS = Path(__file__).resolve().parents[4] / "docs" / "docs"
SNAP = {r["driver"]: r for r in json.loads(_listing.SNAPSHOT.read_text())["drivers"]}
# GeoBrix names that are helpers of a package, not rows of the manifest
HELPERS = {"gbx_zarr_read", "gbx_zarr_write", "pmtiles_info", "StacClient"}
NAME = re.compile(r"`([A-Za-z0-9_]+)`")


def _names(text):
    return [
        n
        for n in NAME.findall(text)
        if n.endswith("_gbx") or n.startswith("gbx_") or n in HELPERS
    ]


def test_every_driver_of_the_guidance_is_in_the_build():
    assert set(G.GUIDANCE) <= set(SNAP) | {"CAD", "SENTINEL2"}, sorted(
        set(G.GUIDANCE) - set(SNAP)
    )
    for name in ("CAD", "SENTINEL2"):
        assert name in SNAP, f"{name} is not a driver of the build"


def test_every_geobrix_name_in_a_note_is_a_reader_a_writer_or_a_function_of_the_manifest():
    m = manifest.load()
    known = {d.name for d in m.datasources} | set(m.functions) | HELPERS
    for driver, entry in G.GUIDANCE.items():
        names = _names(f"{entry.get('use', '')} {entry.get('caveat', '')}")
        names += [entry["reader"]] if "reader" in entry else []
        for n in names:
            assert n in known, f"{driver}: {n} is not in the manifest"


def test_every_link_of_a_note_is_a_docs_page():
    for driver, entry in G.GUIDANCE.items():
        for label, page in entry["links"]:
            if page.startswith(
                "https://"
            ):  # a page of the Databricks docs: not resolved here
                assert page.startswith("https://docs.databricks.com/"), page
                continue
            assert (DOCS / f"{page}.mdx").is_file(), f"{driver}: no docs page {page}"
            assert label and not label.endswith(".")
    # the pages a driver with no named reader or writer is sent to, from what its tests passed
    for name in SNAP:
        for label, page in G.links(name):
            assert page.startswith("https://") or (DOCS / f"{page}.mdx").is_file(), (
                name,
                page,
            )
    # the absolute links of the function are the same pages
    for driver in G.GUIDANCE:
        assert [u.removeprefix(G.DOCS_BASE) for u in G.docs(driver)] == [
            p for _, p in G.links(driver)
        ]
    assert G.docs("CSV") == [
        p for _, p in G.links("CSV")
    ]  # absolute links stay as they are


def _sentences(text):
    return [s for s in re.split(r"(?<=[.!?])\s+", text.strip()) if s]


def test_every_sentence_of_a_note_is_short():
    texts = [str(e.get("caveat", "")) for e in G.GUIDANCE.values()] + [
        r["guidance"] for r in SNAP.values()
    ]
    for text in texts:
        for sentence in _sentences(text):
            assert len(sentence.split()) <= 25, sentence


def test_the_snapshot_holds_the_current_notes():
    for name, r in SNAP.items():
        assert r["guidance"] == G.guidance(r), name
        assert r["docs"] == G.docs(name), name


def test_a_driver_with_no_entry_is_never_said_to_work_without_a_test():
    for name, r in SNAP.items():
        if name in G.GUIDANCE or not (r["raster"] or r["vector"]) or not r["available"]:
            continue
        text = r["guidance"]
        if name in TESTED_DRIVERS:
            for kind, e in TESTED_DRIVERS[name].items():
                verb = "reads and writes" if e["write"] else "reads"
                if kind == "raster":
                    assert (
                        f"raster_gbx reads{' and writes via gbx_rst_asformat' if e['write'] else ''} {name};"
                        in text
                    ), (name, text)
                else:
                    assert (
                        f"vector_gbx {verb} {name} with the driverName option." in text
                    ), (name, text)
            assert "It has no named reader or writer." in text, name
        else:
            assert text.startswith("Not tested") and "may read" in text, name
            assert "works" not in text and "reads it with" not in text, name


def test_every_note_starts_with_exactly_one_approved_lead_phrase():
    """The notes are composed from fixed phrases: a rendered note of every driver starts with exactly one of them."""
    seen = set()
    for name, r in SNAP.items():
        lead = G.lead_of(r["guidance"])
        assert lead is not None, (name, r["guidance"])
        seen.add(lead)
    # the notes of the build use every kind of lead that applies to it
    assert {
        "wrapped",
        "own",
        "raster",
        "vector",
        "untested",
        "redirect",
        "none",
    } <= seen
    # a note that starts any other way is none of them
    assert G.lead_of("Reads DWG drawings.") is None
    assert G.lead_of("It has no named reader or writer; use `raster_gbx`.") is None


def test_the_lead_of_a_driver_with_no_named_reader_or_writer_is_the_template_the_tests_support():
    """ "reads" or "reads and writes" follows a passing write test, and the writer link follows the same rule."""
    kml = G.guidance(
        {
            "driver": "KML",
            "raster": False,
            "vector": True,
            "allowed": True,
            "available": True,
        }
    )
    assert kml.startswith(
        "vector_gbx reads and writes KML with the driverName option. It has no named reader or writer."
    )
    assert [p for _, p in G.links("KML")] == ["readers/vector", "writers/vector"]
    gpx = G.guidance(
        {
            "driver": "GPX",
            "raster": False,
            "vector": True,
            "allowed": True,
            "available": True,
        }
    )
    assert (
        "reads GPX with the driverName option" in gpx and "reads and writes" not in gpx
    )
    assert "writers/vector" not in [p for _, p in G.links("GPX")]
    png = G.guidance(
        {
            "driver": "PNG",
            "raster": True,
            "vector": False,
            "allowed": True,
            "available": True,
        }
    )
    assert png.startswith(
        "raster_gbx reads and writes via gbx_rst_asformat PNG; the format is detected from the file. It has no named reader or writer."
    )
    assert [p for _, p in G.links("PNG")] == ["readers/raster", "api/raster-functions"]
    nitf = G.links("NITF")
    assert [p for _, p in nitf] == ["readers/raster"]


def test_what_a_note_says_about_tests_is_in_the_registry_or_a_finding():
    t = TESTED_DRIVERS
    # GPKG: raster tiles read with raster_gbx, vector read and written with gpkg_gbx; the raster write is a finding
    assert "raster_gbx" in t["GPKG"]["raster"]["read"]
    assert (
        t["GPKG"]["vector"]["write"] == "gpkg_gbx"
        and t["GPKG"]["raster"]["write"] is None
    )
    assert ("asformat", "GPKG-uint8-3857") in R.RASTER_FINDINGS
    # MBTiles: both kinds read by path; the raster write is a finding, and its bytes are not a tile
    assert (
        set(t["MBTiles"]) == {"raster", "vector"}
        and t["MBTiles"]["raster"]["write"] is None
    )
    assert ("asformat", "MBTiles-uint8-3857") in R.RASTER_FINDINGS
    assert SNAP["MBTiles"]["tile_bytes"] is False
    # the raster drivers whose bytes are no tile say so in gbx_drivers
    for name in ("BAG", "GTX", "SRTMHGT", "MBTiles"):
        assert SNAP[name]["tile_bytes"] is False and SNAP[name]["requires"], name
    # NITF, SRTMHGT, BAG, GTX: read listed, the write is a finding
    for name, case in (
        ("NITF", "NITF-int16-4326"),
        ("SRTMHGT", "SRTMHGT-int16-4326"),
        ("BAG", "BAG-float32-4326"),
        ("GTX", "GTX-float32-4326"),
    ):
        assert (
            t[name]["raster"]["write"] is None
            and ("asformat", case) in R.RASTER_FINDINGS
        )
    # GeoBrix has its own reader and writer for netCDF, Zarr, PMTiles and MVT: not listed, whatever the tests show
    assert not {"netCDF", "Zarr", "PMTiles", "MVT"} & set(t)
    assert ("folder", "MVT") in V.VECTOR_FINDINGS
    assert not [c for c in V.VECTOR_CASES if c.driver == "MVT"][0].counts
    assert not any(c.counts for c in R.RASTER_CASES if c.driver == "netCDF")
    # DXF, KML and GML write from any number of partitions
    assert t["DXF"]["vector"]["write"] == "vector_gbx"
    assert not [
        k
        for k in V.VECTOR_FINDINGS
        if k[0] == "write2" and k[1] in ("DXF", "KML", "GML")
    ]
    # GPX: read listed, the write is a finding
    assert (
        t["GPX"]["vector"]["write"] is None and ("write1", "GPX") in V.VECTOR_FINDINGS
    )
    # OGR_VRT and VRT are read under the policy
    assert (
        "vector_gbx" in t["OGR_VRT"]["vector"]["read"]
        and "raster_gbx" in t["VRT"]["raster"]["read"]
    )
    # CSV: the tests run, but the driver is not counted; Databricks' CSV reader and ST functions are the route
    assert (
        "CSV" not in t
        and not [c for c in V.VECTOR_CASES if c.driver == "CSV"][0].counts
    )
    assert (
        "read_files" in G.GUIDANCE["CSV"]["caveat"]
        and "st_point" in G.GUIDANCE["CSV"]["caveat"]
    )
    # CAD: read as a vector driver, never written
    assert t["CAD"] == {"vector": {"read": "vector_gbx", "write": None}}
    # MEM is internal: no test lists it
    assert "MEM" not in t
    # SENTINEL2 has no test
    assert "SENTINEL2" not in t
    # OpenFileGDB is tested as a vector driver only
    assert set(t["OpenFileGDB"]) == {"vector"}
    # the built-in readers named for the drivers GeoBrix reads itself are the ones of the built-in table
    for driver, token in (
        ("netCDF", "netcdf_gbx"),
        ("Zarr", "zarr_gbx"),
        ("PMTiles", "pmtiles_gbx"),
        ("JPEG", "exif_gbx"),
    ):
        assert token in SNAP[driver]["guidance"]
    for driver in ("netCDF", "Zarr", "PMTiles", "MVT"):
        assert "instead of this GDAL driver" in SNAP[driver]["guidance"]


def test_a_note_for_a_dual_kind_driver_names_the_kind_that_is_tested():
    row = {
        "driver": "JP2OpenJPEG",
        "raster": True,
        "vector": True,
        "allowed": True,
        "available": True,
    }
    text = G.guidance(row)
    assert text.startswith(
        "raster_gbx reads and writes via gbx_rst_asformat JP2OpenJPEG;"
    )
    assert "Not tested as a vector driver" in text
