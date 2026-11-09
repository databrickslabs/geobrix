"""The ``guidance`` of each GDAL driver: where GeoBrix has a reader, writer or function for the format, and what to know
before using the driver.

Pure data and text (no GDAL, no PySpark), read by the ``guidance`` column of ``gbx_drivers`` and by the docs generator
for ``docs/docs/gdal-drivers.mdx``. Every note is composed by ``note()`` from a fixed lead phrase, then (for a driver
GeoBrix has no named reader or writer for) the tested registry's status, then the driver's own caveat as a sentence of
its own. The lead phrases are ``LEADS``; ``test/drivers_extended/test_guidance.py`` holds every rendered note to
exactly one of them, every GeoBrix name to the manifest, every link to a docs page, and every claim to the registry or
to a recorded finding.

The lead says how GeoBrix relates to the driver:

- ``wrapped``: a named reader and writer (``shapefile_gbx``) runs on the driver. Prefer it to calling the driver.
- ``own``: GeoBrix has its own reader and writer, not built on the driver. Use it instead.
- ``unnamed`` (the default): no named reader or writer. The note says what ``raster_gbx`` and ``vector_gbx`` (with
  ``driverName``) were tested to do with the driver: "reads" or "reads and writes", by what a passing test shows.
- ``redirect``: the product's own reader does the job (CSV).
- ``none``: GeoBrix has no reader or writer for it (an in-process dataset, a driver of neither kind).

A listing filtered by kind (``gbx_drivers('raster')``) gets the note of that kind only: ``note``, ``guidance``, ``links``
and ``docs`` take the kind. A ``wrapped`` entry whose reader serves one kind (``reader_kinds``) gives the other kind the
note of a driver with no named reader (the registry's status for that kind), and a link to a page about the other kind
is left out.

Entry keys: ``kind`` (one of the above; ``unnamed`` when absent), ``reader`` and ``what`` (``wrapped``), ``reader_kinds``
(``wrapped``: the kinds the reader serves, every kind when absent), ``use``
(``own``), ``caveat`` (a sentence or two, GeoBrix names in backticks) and ``links`` (a list of ``(label, docs page)``
pairs, the pages under ``docs/docs`` without the extension, or an absolute https URL to a page of the Databricks docs).
"""

import re
from typing import Dict, List, Optional, Tuple

from databricks.labs.gbx.drivers._tested import TESTED_DRIVERS

GUIDANCE: Dict[str, Dict[str, object]] = {
    "COG": {
        "kind": "wrapped",
        "reader": "cog_gbx",
        "what": "Cloud Optimized GeoTIFFs",
        "links": (("COG reader", "readers/cog"), ("COG writer", "writers/cog")),
    },
    "GTiff": {
        "kind": "wrapped",
        "reader": "gtiff_gbx",
        "what": "GeoTIFFs",
        "links": (
            ("GeoTIFF reader", "readers/geotiff"),
            ("GeoTIFF writer", "writers/geotiff"),
        ),
    },
    "GPKG": {
        "kind": "wrapped",
        "reader": "gpkg_gbx",
        "what": "GeoPackage vector layers",
        "reader_kinds": ("vector",),
        "caveat": "`raster_gbx` reads GeoPackage raster tiles, and `gbx_rst_asformat` does not write them yet.",
        "links": (
            ("GeoPackage reader", "readers/geopackage"),
            ("GeoPackage writer", "writers/geopackage"),
        ),
    },
    "OpenFileGDB": {
        "kind": "wrapped",
        "reader": "file_gdb_gbx",
        "what": "File Geodatabase feature classes",
        "reader_kinds": ("vector",),
        "caveat": "Its named reader and writer are vector only. Raster data in a geodatabase is not tested with GeoBrix.",
        "links": (
            ("GeoDatabase reader", "readers/filegdb"),
            ("GeoDatabase writer", "writers/filegdb"),
        ),
    },
    "ESRI Shapefile": {
        "kind": "wrapped",
        "reader": "shapefile_gbx",
        "what": "shapefiles",
        "links": (
            ("Shapefile reader", "readers/shapefile"),
            ("Shapefile writer", "writers/shapefile"),
        ),
    },
    "GeoJSON": {
        "kind": "wrapped",
        "reader": "geojson_gbx",
        "what": "GeoJSON",
        "links": (
            ("GeoJSON reader", "readers/geojson"),
            ("GeoJSON writer", "writers/geojson"),
        ),
    },
    "GeoJSONSeq": {
        "kind": "wrapped",
        "reader": "geojsonl_gbx",
        "what": "GeoJSON Sequence (one feature per line)",
        "caveat": "It writes in shards for large data.",
        "links": (
            ("GeoJSON reader", "readers/geojson"),
            ("GeoJSONL writer", "writers/geojsonl"),
        ),
    },
    "netCDF": {
        "kind": "own",
        "use": "`netcdf_gbx`",
        "caveat": "It reads CF grids as rasters and as vector features.",
        "links": (
            ("NetCDF reader", "readers/netcdf"),
            ("NetCDF writer", "writers/netcdf"),
            ("Zarr", "readers/zarr"),
        ),
    },
    "Zarr": {
        "kind": "own",
        "use": "`zarr_gbx`",
        "caveat": "`gbx_zarr_read` and `gbx_zarr_write` cover stores and transactions.",
        "links": (
            ("Zarr reader", "readers/zarr"),
            ("Zarr writer", "writers/zarr"),
            ("Zarr functions", "api/zarr-functions"),
        ),
    },
    "PMTiles": {
        "kind": "own",
        "use": "`pmtiles_gbx`",
        "caveat": "`gbx_pmtiles_agg` builds an archive from tiles.",
        "links": (
            ("PMTiles reader", "readers/pmtiles"),
            ("PMTiles writer", "writers/pmtiles"),
            ("PMTiles functions", "api/pmtiles-functions"),
            ("gbx_st_asmvt", "api/vectorx-functions"),
        ),
    },
    "MVT": {
        "kind": "own",
        "use": "`gbx_st_asmvt`, `gbx_st_asmvt_pyramid` and `pmtiles_gbx`",
        "caveat": "A single tile opens through `vector_gbx`, a tile folder does not.",
        "links": (
            ("Vector functions", "api/vectorx-functions"),
            ("PMTiles writer", "writers/pmtiles"),
        ),
    },
    "CSV": {
        "kind": "redirect",
        "caveat": "Use Databricks' native CSV reader, `read_files`, then `st_point` for X and Y columns or `st_geomfromtext` for WKT.",
        "links": (
            ("Read CSV files", "https://docs.databricks.com/aws/en/query/formats/csv"),
            (
                "ST functions",
                "https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-st-geospatial-functions",
            ),
        ),
    },
    "MEM": {
        "kind": "none",
        "caveat": "GDAL's in-process dataset cannot be held as tile bytes or cross tasks. GeoBrix uses it inside functions such as `gbx_rst_contour`, so there is nothing to call directly.",
        "links": (("Virtual Tiles", "api/virtual-tiles"),),
    },
    "NUMPY": {
        "kind": "none",
        "caveat": "GDAL's in-process array dataset cannot be held as tile bytes or cross tasks. Use plain GDAL (`from osgeo import gdal`).",
        "links": (),
    },
    "JPEG": {
        "kind": "unnamed",
        "caveat": "`exif_gbx` reads the EXIF and GPS metadata, which this driver does not.",
        "links": (("Raster reader", "readers/raster"), ("EXIF reader", "readers/exif")),
    },
    "MBTiles": {
        "kind": "unnamed",
        "caveat": "Raster MBTiles open by path, and their bytes do not load as a tile. `gbx_rst_asformat` does not write MBTiles. For tile pyramids use `pmtiles_gbx`.",
        "links": (
            ("PMTiles writer", "writers/pmtiles"),
            ("Vector reader", "readers/vector"),
        ),
    },
    "VRT": {
        "kind": "unnamed",
        "caveat": "A VRT names other files. `raster_gbx` reads one you write. For GeoBrix's own mosaics, see the VRT mosaic and virtual tile pages.",
        "links": (
            ("VRT & Mosaics", "api/vrt-mosaic"),
            ("Virtual Tiles", "api/virtual-tiles"),
        ),
    },
    "OGR_VRT": {
        "kind": "unnamed",
        "caveat": "The driver policy allows an OGR VRT that you write. It names other datasets, so keep it to sources you trust.",
        "links": (("Vector reader", "readers/vector"), ("Security", "security")),
    },
    "DXF": {
        "kind": "unnamed",
        "caveat": "The writer takes a DataFrame with any number of partitions. For DWG drawings, see the CAD driver.",
        "links": (
            ("Vector reader", "readers/vector"),
            ("Vector writer", "writers/vector"),
        ),
    },
    "CAD": {
        "kind": "unnamed",
        "caveat": "It is read-only. Only AutoCAD R2000 (AC1015) drawings are tested. To round-trip drawings, use DXF.",
        "links": (("Vector reader", "readers/vector"),),
    },
    "SQLite": {
        "kind": "unnamed",
        "caveat": "SpatiaLite is not in the env wheel's GDAL.",
        "links": (
            ("Vector reader", "readers/vector"),
            ("Vector writer", "writers/vector"),
        ),
    },
    "FlatGeobuf": {
        "kind": "unnamed",
        "links": (
            ("Vector reader", "readers/vector"),
            ("Vector writer", "writers/vector"),
        ),
    },
    "NITF": {
        "kind": "unnamed",
        "caveat": "`gbx_rst_asformat` does not write it, because NITF needs a creation option to store a CRS. Use plain GDAL to write NITF.",
        "links": (("Raster reader", "readers/raster"),),
    },
    "SRTMHGT": {
        "kind": "unnamed",
        "caveat": "GDAL identifies SRTM HGT tiles by the tile name, so their bytes do not load as a tile, and `gbx_rst_asformat` does not write them.",
        "links": (("Raster reader", "readers/raster"),),
    },
    "BAG": {
        "kind": "unnamed",
        "caveat": "Its bytes do not load as a tile, and `gbx_rst_asformat` does not write BAG.",
        "links": (("Raster reader", "readers/raster"),),
    },
    "GTX": {
        "kind": "unnamed",
        "caveat": "GDAL identifies GTX grids by the .gtx extension, so their bytes do not load as a tile, and `gbx_rst_asformat` does not write them.",
        "links": (("Raster reader", "readers/raster"),),
    },
    "GPX": {
        "kind": "unnamed",
        "caveat": "The writer refuses a column that shares a name with a field GPX makes itself, such as `name`. Rename that column first.",
        "links": (("Vector reader", "readers/vector"),),
    },
    "SENTINEL2": {
        "kind": "unnamed",
        "caveat": "It opens Sentinel-2 SAFE products. For cloud-native imagery, search STAC with `StacClient` and read the COG bands with `raster_gbx`.",
        "links": (("STAC client", "api/stac"), ("COG reader", "readers/cog")),
    },
}

DOCS_BASE = "https://databrickslabs.github.io/geobrix/docs/"
_DIRECT = "plain GDAL (`from osgeo import gdal`)"

# The lead phrases of every note, one regular expression each (GeoBrix names in backticks). A rendered note starts with
# exactly one of them.
LEADS = {
    "wrapped": r"`[a-z_]+_gbx` reads and writes [^.]+, on this driver\. Prefer it to calling the driver directly\.",
    "own": r"GeoBrix has its own reader and writer for this format: use [^.]+ instead of this GDAL driver\.",
    "raster": r"`raster_gbx` reads( and writes via `gbx_rst_asformat`)? [^;]+; the format is detected from the file\.",
    "vector": r"`vector_gbx` reads( and writes)? [^.]+ with the `driverName` option\.",
    "untested": r"Not tested (with GeoBrix|as a (raster|vector) driver): ",
    "redirect": r"Do not read CSV through this driver or `vector_gbx`\.",
    "none": r"GeoBrix has no reader or writer for it\.",
    "unavailable": r"GeoBrix does not use this driver: it needs a service, program or plugin this runtime lacks\.",
}
LEAD_PATTERNS = {k: re.compile(v) for k, v in LEADS.items()}


def lead_of(text: str):
    """The one lead phrase a plain-text note starts with, or None (none or several match)."""
    plain = text.replace("`", "")
    hits = [
        k
        for k, p in LEAD_PATTERNS.items()
        if re.match(p.pattern.replace("`", ""), plain)
    ]
    return hits[0] if len(hits) == 1 else None


def _entries(text: str) -> str:
    return " or ".join(f"`{e}`" for e in text.split(", "))


def _kinds(row: Dict[str, object], kind: Optional[str]) -> List[str]:
    """The kinds a note covers: the driver's kinds, or the one the listing was filtered to."""
    return [k for k in ("raster", "vector") if row.get(k) and kind in (None, k)]


def _serves(entry: Dict[str, object], kind: Optional[str]) -> bool:
    """Whether the entry's named reader (or own reader) serves ``kind`` (None: any kind)."""
    served = entry.get("reader_kinds")
    return kind is None or served is None or kind in served  # type: ignore[operator]


def _lead(entry: Dict[str, object], row: Dict[str, object]) -> str:
    kind = str(entry.get("kind", "unnamed"))
    if kind == "wrapped":
        return (
            f"`{entry['reader']}` reads and writes {entry['what']}, on this driver. "
            "Prefer it to calling the driver directly."
        )
    if kind == "own":
        return (
            "GeoBrix has its own reader and writer for this format: "
            f"use {entry['use']} instead of this GDAL driver."
        )
    if kind == "redirect":
        return "Do not read CSV through this driver or `vector_gbx`."
    if kind == "none":
        return "GeoBrix has no reader or writer for it."
    return ""


def _unnamed(row: Dict[str, object], kind: Optional[str] = None) -> str:
    """The note of a driver with no named reader or writer, from the tested registry: for each kind that passed, what
    ``raster_gbx`` or ``vector_gbx`` does with it ("reads", and "writes" only when a write test passed), then that it has
    no named reader or writer, then each kind that is not tested. With ``kind``, only that kind is covered.
    """
    name = str(row["driver"])
    kinds = _kinds(row, kind)
    e = TESTED_DRIVERS.get(name) or {}
    use = {"raster": "`raster_gbx`", "vector": "`vector_gbx`"}
    tested = [k for k in kinds if k in e]
    untested = [k for k in kinds if k not in tested]
    parts = []
    for k in tested:
        wrote = bool(e[k]["write"])
        if k == "raster":
            via = " and writes via `gbx_rst_asformat`" if wrote else ""
            parts.append(
                f"`raster_gbx` reads{via} {name}; the format is detected from the file."
            )
        else:
            parts.append(
                f"`vector_gbx` reads{' and writes' if wrote else ''} {name} with the `driverName` option."
            )
    if tested:
        parts.append("It has no named reader or writer.")
    if untested and len(kinds) == 1:
        parts.append(
            f"Not tested with GeoBrix: {use[kinds[0]]} may read it; plain GDAL may suit it better."
        )
    else:
        for k in untested:
            parts.append(
                f"Not tested as a {k} driver: {use[k]} may read it; plain GDAL may suit it better."
            )
    return " ".join(parts)


def generic(row: Dict[str, object], kind: Optional[str] = None) -> str:
    """The note of a driver with no entry in ``GUIDANCE``, from the tested registry and the driver's kinds (or the
    one ``kind`` the listing was filtered to)."""
    kinds = _kinds(row, kind)
    if not kinds:
        return f"GeoBrix has no reader or writer for it. Open it with {_DIRECT}."
    if not (row.get("allowed") and row.get("available")):
        return "GeoBrix does not use this driver: it needs a service, program or plugin this runtime lacks."
    return _unnamed(row, kind)


def note(row: Dict[str, object], kind: Optional[str] = None) -> str:
    """The guidance text for a driver row (a dict with ``driver``, ``raster``, ``vector``, ``allowed``, ``available``),
    with GeoBrix names in backticks: the lead phrase, the registry's status for a driver with no named reader or writer,
    then the driver's caveat. A driver GeoBrix has its own reader for, or none, also says its GDAL driver is not tested.
    With ``kind``, the note covers that kind only (see the module doc).
    """
    entry = GUIDANCE.get(str(row["driver"]))
    if not entry:
        return generic(row, kind)
    kind_of_entry = str(entry.get("kind", "unnamed"))
    if kind_of_entry == "wrapped" and not _serves(entry, kind):
        # the named reader is for the other kind: this kind has the registry's status, then the entry's caveat
        kind_of_entry = "unnamed"
        parts = []
    else:
        parts = [_lead(entry, row)]
    if kind_of_entry == "unnamed":
        # generic() says "no reader or writer" for a driver of neither kind and "does not use this driver" where the
        # runtime lacks it, before any word on what the tests passed
        parts.append(generic(row, kind))
    caveat = entry.get("caveat")
    if caveat:
        parts.append(str(caveat))
    if kind_of_entry in ("own", "none") and row["driver"] not in TESTED_DRIVERS:
        parts.append("The GDAL driver itself is not tested with GeoBrix.")
    return " ".join(p for p in parts if p)


_PAGES = {
    "reader:raster": ("Raster reader", "readers/raster"),
    "writer:raster": ("gbx_rst_asformat", "api/raster-functions"),
    "reader:vector": ("Vector reader", "readers/vector"),
    "writer:vector": ("Vector writer", "writers/vector"),
}


# The kind a docs page is about, for the pages that are about one (the others are about every kind).
_PAGE_KIND = {
    "readers/raster": "raster",
    "api/raster-functions": "raster",
    "readers/vector": "vector",
    "writers/vector": "vector",
}


def links(driver: str, kind: Optional[str] = None) -> Tuple[Tuple[str, str], ...]:
    """The ``(label, docs page)`` links of a driver's note. A driver with no named reader or writer gets the pages of
    what its tests passed (the reader page of a kind it reads; the writer page only for a kind it writes), then the
    links of its entry; any other driver gets those of its entry. With ``kind``, only that kind's pages: a page about
    the other kind is left out, and an entry whose reader is for the other kind is treated as one with none.
    """
    entry = GUIDANCE.get(driver)
    own = tuple(entry["links"]) if entry else ()
    if kind is not None:
        own = tuple(lk for lk in own if _PAGE_KIND.get(lk[1], kind) == kind)
    other_reader = (
        bool(entry) and entry.get("kind") == "wrapped" and not _serves(entry, kind)
    )
    if other_reader:
        own = ()  # the pages of a reader for the other kind
    elif entry and str(entry.get("kind", "unnamed")) != "unnamed":
        return own
    out: List[Tuple[str, str]] = []
    for k, e in (TESTED_DRIVERS.get(driver) or {}).items():
        if kind is not None and k != kind:
            continue
        out.append(_PAGES[f"reader:{k}"])
        if e["write"]:
            out.append(_PAGES[f"writer:{k}"])
    for link in own:
        if link[1] not in {p for _, p in out}:
            out.append(link)
    return tuple(out)


def guidance(row: Dict[str, object], kind: Optional[str] = None) -> str:
    """The ``guidance`` column of ``gbx_drivers``: the note as plain text (of ``kind`` when the listing is filtered)."""
    return note(row, kind).replace("`", "")


def docs(driver: str, kind: Optional[str] = None) -> List[str]:
    """The ``docs`` column of ``gbx_drivers``: the absolute URLs of the pages of a driver's note (of ``kind`` when the
    listing is filtered)."""
    return [_href(page, DOCS_BASE) for _, page in links(driver, kind)]


def _href(page: str, prefix: str) -> str:
    return page if page.startswith("https://") else prefix + page


def guidance_markdown(
    row: Dict[str, object], prefix: str = "./", kind: Optional[str] = None
) -> str:
    """The note for the docs page: names in code, then the pages that cover them, inline. ``kind`` is the page's kind."""
    text = note(row, kind)
    ls = links(str(row["driver"]), kind)
    if ls:
        text += " " + " · ".join(
            f"[{label}]({_href(page, prefix)})" for label, page in ls
        )
    return text
