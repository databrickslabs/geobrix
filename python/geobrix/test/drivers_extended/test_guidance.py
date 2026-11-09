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
        for n in _names(entry["text"]):
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
    texts = [e["text"] for e in G.GUIDANCE.values()] + [
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
            assert "Tested" in text, name
            for entry in re.findall(r"read it with ([^.]*?)(?: and write|\.|$)", text):
                for n in re.findall(r"[a-z_]+gbx|gbx_[a-z_]+", entry):
                    assert n in str(TESTED_DRIVERS[name]["read"]), (name, n)
        else:
            assert "Not tested" in text and "may read" in text, name
            assert "works" not in text and "reads it with" not in text, name


def test_what_a_note_says_about_tests_is_in_the_registry_or_a_finding():
    t = TESTED_DRIVERS
    # GPKG: raster tiles read with raster_gbx; the raster write is a finding
    assert "raster_gbx" in t["GPKG"]["read"] and t["GPKG"]["write"] == "gpkg_gbx"
    assert ("asformat", "GPKG-uint8-3857") in R.RASTER_FINDINGS
    # MBTiles: vector tested; the raster bytes are a finding
    assert t["MBTiles"]["kinds"] == ("vector",)
    assert ("fromcontent", "MBTiles-uint8-3857") in R.RASTER_FINDINGS
    # MVT: the tile folder is a finding, so the driver is not listed
    assert ("folder", "MVT") in V.VECTOR_FINDINGS and "MVT" not in t
    # DXF: read listed, the multi-partition write is a finding
    assert t["DXF"]["write"] is None and ("write2", "DXF") in V.VECTOR_FINDINGS
    # OGR_VRT and VRT are read under the policy
    assert "vector_gbx" in t["OGR_VRT"]["read"] and "raster_gbx" in t["VRT"]["read"]
    # CSV: the tests run, but the driver is not counted; Databricks' CSV reader and ST functions are the route
    assert (
        "CSV" not in t
        and not [c for c in V.VECTOR_CASES if c.driver == "CSV"][0].counts
    )
    assert (
        "read_files" in G.GUIDANCE["CSV"]["text"]
        and "st_point" in G.GUIDANCE["CSV"]["text"]
    )
    # CAD and SENTINEL2 have no test
    assert "CAD" not in t and "SENTINEL2" not in t
    # OpenFileGDB is tested as a vector driver only
    assert t["OpenFileGDB"]["kinds"] == ("vector",)
    # the built-in readers named for the drivers GeoBrix reads itself are the ones of the built-in table
    for driver, token in (
        ("netCDF", "netcdf_gbx"),
        ("Zarr", "zarr_gbx"),
        ("PMTiles", "pmtiles_gbx"),
        ("JPEG", "exif_gbx"),
    ):
        assert token in G.GUIDANCE[driver]["text"]


def test_a_note_for_a_dual_kind_driver_names_the_kind_that_is_tested():
    row = {
        "driver": "JP2OpenJPEG",
        "raster": True,
        "vector": True,
        "allowed": True,
        "available": True,
    }
    text = G.guidance(row)
    assert (
        "Tested as a raster driver" in text and "Not tested as a vector driver" in text
    )
