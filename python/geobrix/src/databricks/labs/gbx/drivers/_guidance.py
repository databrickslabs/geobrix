"""The ``guidance`` of each GDAL driver: where GeoBrix has a built-in reader, writer or function for the format, and
what it covers.

Pure data and text (no GDAL, no PySpark), read by the ``guidance`` column of ``gbx_drivers`` and by the docs generator
for ``docs/docs/gdal-drivers.mdx``. A driver with an entry in ``GUIDANCE`` gets that note. Every other driver gets a
note built from the tested registry (``_tested.py``) and the driver's kinds, so it never says a driver works unless a
test shows it. ``test_drivers.py`` and ``test/drivers_extended/test_guidance.py`` hold every GeoBrix name here to the
manifest, every link to a docs page, and every claim to the registry or to a recorded finding.

Entry keys: ``text`` (the note; GeoBrix names in backticks) and ``links`` (a list of ``(label, docs page)`` pairs, the
pages under ``docs/docs`` without the extension, or an absolute https URL to a page of the Databricks docs; as many
as the note has pages worth reading).
"""

from typing import Dict, List, Tuple

from databricks.labs.gbx.drivers._tested import TESTED_DRIVERS

GUIDANCE: Dict[str, Dict[str, object]] = {
    "netCDF": {
        "text": "`netcdf_gbx` reads and writes netCDF as rasters (CF grids) and as vector features. Prefer it to this driver.",
        "links": (
            ("NetCDF reader", "readers/netcdf"),
            ("NetCDF writer", "writers/netcdf"),
            ("Zarr", "readers/zarr"),
        ),
    },
    "Zarr": {
        "text": "`zarr_gbx` reads and writes Zarr, and `gbx_zarr_read` and `gbx_zarr_write` cover stores and transactions. Prefer them to this driver.",
        "links": (
            ("Zarr reader", "readers/zarr"),
            ("Zarr writer", "writers/zarr"),
            ("Zarr functions", "api/zarr-functions"),
        ),
    },
    "PMTiles": {
        "text": "`pmtiles_gbx` reads and writes PMTiles archives, and `gbx_pmtiles_agg` builds one from tiles. Prefer them to this driver.",
        "links": (
            ("PMTiles reader", "readers/pmtiles"),
            ("PMTiles writer", "writers/pmtiles"),
            ("PMTiles functions", "api/pmtiles-functions"),
            ("gbx_st_asmvt", "api/vectorx-functions"),
        ),
    },
    "MVT": {
        "text": "`gbx_st_asmvt` and `gbx_st_asmvt_pyramid` encode vector tiles, and `pmtiles_gbx` stores them. A tile folder does not read through `vector_gbx` yet.",
        "links": (
            ("Vector functions", "api/vectorx-functions"),
            ("PMTiles writer", "writers/pmtiles"),
        ),
    },
    "JPEG": {
        "text": "`raster_gbx` reads the pixels. `exif_gbx` reads the EXIF and GPS metadata, which this driver does not.",
        "links": (("Raster reader", "readers/raster"), ("EXIF reader", "readers/exif")),
    },
    "MBTiles": {
        "text": "`vector_gbx` reads MBTiles vector tiles, and `raster_gbx` opens raster MBTiles by path, but its bytes do not load as a tile yet. For tile pyramids use `pmtiles_gbx`: MBTiles is one SQLite file, which suits a single writer, not distributed writes.",
        "links": (
            ("PMTiles writer", "writers/pmtiles"),
            ("Vector reader", "readers/vector"),
        ),
    },
    "VRT": {
        "text": "A VRT names other files. `raster_gbx` reads one you write. For GeoBrix's own mosaics, see the VRT mosaic and virtual tile pages.",
        "links": (
            ("VRT & Mosaics", "api/vrt-mosaic"),
            ("Virtual Tiles", "api/virtual-tiles"),
        ),
    },
    "OGR_VRT": {
        "text": "`vector_gbx` reads an OGR VRT that you write, and the driver policy allows it. It names other datasets, so keep it to sources you trust.",
        "links": (("Vector reader", "readers/vector"), ("Security", "security")),
    },
    "MEM": {
        "text": "GDAL's in-process dataset. It cannot be held as tile bytes or cross tasks. GeoBrix uses it inside functions such as `gbx_rst_contour`, so there is nothing to call directly.",
        "links": (("Virtual Tiles", "api/virtual-tiles"),),
    },
    "COG": {
        "text": "`cog_gbx` reads and writes Cloud Optimized GeoTIFFs. Prefer it to this driver.",
        "links": (("COG reader", "readers/cog"), ("COG writer", "writers/cog")),
    },
    "GTiff": {
        "text": "`gtiff_gbx` reads and writes GeoTIFFs. Prefer it to this driver.",
        "links": (
            ("GeoTIFF reader", "readers/geotiff"),
            ("GeoTIFF writer", "writers/geotiff"),
        ),
    },
    "GPKG": {
        "text": "`gpkg_gbx` reads and writes GeoPackage vector layers. `raster_gbx` reads GeoPackage raster tiles, and `gbx_rst_asformat` does not write them yet.",
        "links": (
            ("GeoPackage reader", "readers/geopackage"),
            ("GeoPackage writer", "writers/geopackage"),
        ),
    },
    "OpenFileGDB": {
        "text": "`file_gdb_gbx` reads and writes File Geodatabase feature classes, which are vector only. Raster data in a geodatabase is not tested with GeoBrix.",
        "links": (
            ("GeoDatabase reader", "readers/filegdb"),
            ("GeoDatabase writer", "writers/filegdb"),
        ),
    },
    "ESRI Shapefile": {
        "text": "`shapefile_gbx` reads and writes shapefiles. Prefer it to this driver.",
        "links": (
            ("Shapefile reader", "readers/shapefile"),
            ("Shapefile writer", "writers/shapefile"),
        ),
    },
    "GeoJSON": {
        "text": "`geojson_gbx` reads and writes GeoJSON. Prefer it to this driver.",
        "links": (
            ("GeoJSON reader", "readers/geojson"),
            ("GeoJSON writer", "writers/geojson"),
        ),
    },
    "GeoJSONSeq": {
        "text": "`geojsonl_gbx` reads and writes GeoJSON Sequence (one feature per line), in shards for large data.",
        "links": (
            ("GeoJSON reader", "readers/geojson"),
            ("GeoJSONL writer", "writers/geojsonl"),
        ),
    },
    "FlatGeobuf": {
        "text": "`vector_gbx` reads and writes FlatGeobuf with the `driverName` option. It has no named reader or writer.",
        "links": (
            ("Vector reader", "readers/vector"),
            ("Vector writer", "writers/vector"),
        ),
    },
    "DXF": {
        "text": "`vector_gbx` reads DXF drawings. The writer does not yet write DXF when the DataFrame has more than one partition.",
        "links": (("Vector reader", "readers/vector"),),
    },
    "CSV": {
        "text": "Do not read CSV through this driver or `vector_gbx`. Use Databricks' native CSV reader, `read_files`, then `st_point` for X and Y columns or `st_geomfromtext` for WKT.",
        "links": (
            ("Read CSV files", "https://docs.databricks.com/aws/en/query/formats/csv"),
            (
                "ST functions",
                "https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-st-geospatial-functions",
            ),
        ),
    },
    "SQLite": {
        "text": "`vector_gbx` reads and writes SQLite. SpatiaLite is not in the env wheel's GDAL.",
        "links": (
            ("Vector reader", "readers/vector"),
            ("Vector writer", "writers/vector"),
        ),
    },
    "NUMPY": {
        "text": "GDAL's in-process array dataset. It cannot be held as tile bytes or cross tasks, so GeoBrix does not read it. Use plain GDAL (`from osgeo import gdal`).",
        "links": (),
    },
    "CAD": {
        "text": "Reads DWG drawings, and only reads. Not tested with GeoBrix, because no sample drawing is available. Plain GDAL may suit it better.",
        "links": (),
    },
    "SENTINEL2": {
        "text": "Opens Sentinel-2 SAFE products. Not tested with GeoBrix. For cloud-native imagery, search STAC with `StacClient` and read the COG bands with `raster_gbx`.",
        "links": (("STAC client", "api/stac"), ("COG reader", "readers/cog")),
    },
}

DOCS_BASE = "https://databrickslabs.github.io/geobrix/docs/"
_DIRECT = "plain GDAL (`from osgeo import gdal`)"


def _entries(text: str) -> str:
    return " or ".join(f"`{e}`" for e in text.split(", "))


def generic(row: Dict[str, object]) -> str:
    """The note of a driver with no entry in ``GUIDANCE``, from the tested registry and the driver's kinds."""
    name = str(row["driver"])
    kinds = [k for k in ("raster", "vector") if row.get(k)]
    if not kinds:
        return f"GeoBrix has no reader or writer for it. Open it with {_DIRECT}."
    if not (row.get("allowed") and row.get("available")):
        return "GeoBrix does not use this driver: it needs a service, program or plugin this runtime lacks."
    e = TESTED_DRIVERS.get(name)
    use = {"raster": "`raster_gbx`", "vector": "`vector_gbx`"}
    tested = [k for k in kinds if e and k in e["kinds"]]
    untested = [k for k in kinds if k not in tested]
    parts = []
    if tested:
        how = f"read it with {_entries(str(e['read']))}"
        if e["write"]:
            how += f" and write it with {_entries(str(e['write']))}"
        if len(kinds) == 1:
            parts.append(f"Tested with GeoBrix: {how}.")
        else:
            parts.append(f"Tested as a {' and '.join(tested)} driver: {how}.")
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


def note(row: Dict[str, object]) -> str:
    """The guidance text for a driver row (a dict with ``driver``, ``raster``, ``vector``, ``allowed``, ``available``),
    with GeoBrix names in backticks. For a driver with an entry, a driver that is not tested also says so.
    """
    entry = GUIDANCE.get(str(row["driver"]))
    if not entry:
        return generic(row)
    text = str(entry["text"])
    if row["driver"] not in TESTED_DRIVERS and "Not tested" not in text:
        text += " The GDAL driver itself is not tested with GeoBrix."
    return text


def links(driver: str) -> Tuple[Tuple[str, str], ...]:
    """The ``(label, docs page)`` links of a driver's note."""
    entry = GUIDANCE.get(driver)
    return tuple(entry["links"]) if entry else ()


def guidance(row: Dict[str, object]) -> str:
    """The ``guidance`` column of ``gbx_drivers``: the note as plain text."""
    return note(row).replace("`", "")


def docs(driver: str) -> List[str]:
    """The ``docs`` column of ``gbx_drivers``: the absolute URLs of the pages of a driver's note."""
    return [_href(page, DOCS_BASE) for _, page in links(driver)]


def _href(page: str, prefix: str) -> str:
    return page if page.startswith("https://") else prefix + page


def guidance_markdown(row: Dict[str, object], prefix: str = "./") -> str:
    """The note for the docs page: names in code, then the pages that cover them, inline."""
    text = note(row)
    ls = links(str(row["driver"]))
    if ls:
        text += " " + " · ".join(
            f"[{label}]({_href(page, prefix)})" for label, page in ls
        )
    return text
