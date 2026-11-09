"""Raster cases of the extended driver tests: a small sample per driver written with GDAL, and the plain-GDAL
reading the GeoBrix tile is held to.

One ``RasterCase`` is one (driver, data type) sample. ``write_sample`` writes it with the driver's own writer.
``reference`` reads the file back in plain GDAL: that is what GeoBrix has to return, whatever the format keeps (a
lossy format returns its own decoded pixels, a format with no CRS returns none).
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

_NP = {
    "uint8": "GDT_Byte",
    "int16": "GDT_Int16",
    "int32": "GDT_Int32",
    "float32": "GDT_Float32",
}

LONLAT = (10.0, 0.1, 0.0, 50.0, 0.0, -0.1)  # a 64 x 64 window of EPSG:4326
UTM = (500000.0, 30.0, 0.0, 5500000.0, 0.0, -30.0)  # a 64 x 64 window of EPSG:32633
MERC = (0.0, 1000.0, 0.0, 64000.0, 0.0, -1000.0)  # EPSG:3857


@dataclass(frozen=True)
class RasterCase:
    """One sample file. ``reader`` is the GeoBrix entry that reads it by path; ``path_only`` is a driver whose
    bytes alone are not a tile (other files, a database), so only a path reads it."""

    driver: str
    ext: str
    dtype: str = "int16"
    bands: int = 1
    nodata: Optional[float] = -9999
    epsg: Optional[int] = 4326
    gt: Tuple[float, ...] = LONLAT
    size: int = 64
    co: Tuple[str, ...] = ()
    reader: str = "raster_gbx"
    path_only: bool = False
    ramp: bool = False  # a smooth surface, for a lossy format
    name: str = ""  # the file's name, when the driver needs one
    note: str = ""

    @property
    def id(self) -> str:
        return f"{self.driver}-{self.dtype}" + (f"-{self.epsg}" if self.epsg else "")


def source_arrays(case: RasterCase) -> List[np.ndarray]:
    """The pixels of each band: a counting pattern (a smooth ramp for a lossy format), with the NoData value in a
    scatter of cells so a reader that drops or rewrites NoData shows."""
    n = case.size
    yy, xx = np.mgrid[0:n, 0:n]
    out = []
    for b in range(case.bands):
        if case.ramp:
            a = (xx + yy) * 2.0 + b * 20
        else:
            a = (yy * n + xx) % 200 + 3 * (b + 1)
        if case.dtype == "float32":
            a = a * 0.5 + 0.25
        a = a.astype(case.dtype)
        if case.nodata is not None:
            a[(yy * 7 + xx * 3) % 97 == 0] = case.nodata
        out.append(a)
    return out


def _srs(gb, epsg):
    srs = gb.osr.SpatialReference()
    srs.ImportFromEPSG(epsg)
    srs.SetAxisMappingStrategy(gb.osr.OAMS_TRADITIONAL_GIS_ORDER)
    return srs


def memory_source(gb, case: RasterCase):
    """The case's pixels, CRS and NoData as a GDAL in-memory dataset."""
    gdal = gb.gdal
    ds = gdal.GetDriverByName("MEM").Create(
        "", case.size, case.size, case.bands, getattr(gdal, _NP[case.dtype])
    )
    ds.SetGeoTransform(case.gt)
    if case.epsg:
        ds.SetProjection(_srs(gb, case.epsg).ExportToWkt())
    for i, a in enumerate(source_arrays(case), start=1):
        band = ds.GetRasterBand(i)
        band.WriteArray(a)
        if case.nodata is not None:
            band.SetNoDataValue(float(case.nodata))
    return ds


def write_sample(gb, case: RasterCase, directory: str) -> str:
    """The sample file of ``case`` under ``directory``; its path."""
    import os

    gdal = gb.gdal
    path = os.path.join(directory, case.name or f"s_{case.id}{case.ext}")
    drv = gdal.GetDriverByName(case.driver)
    src = memory_source(gb, case)
    opts = list(case.co)
    if drv.GetMetadataItem("DCAP_CREATECOPY") == "YES":
        out = drv.CreateCopy(path, src, 0, opts)
    else:
        out = drv.Create(
            path, case.size, case.size, case.bands, getattr(gdal, _NP[case.dtype]), opts
        )
        out.SetGeoTransform(case.gt)
        if case.epsg:
            out.SetProjection(_srs(gb, case.epsg).ExportToWkt())
        for i, a in enumerate(source_arrays(case), start=1):
            band = out.GetRasterBand(i)
            band.WriteArray(a)
            if case.nodata is not None:
                band.SetNoDataValue(float(case.nodata))
    assert out is not None, f"GDAL could not write a {case.driver} sample"
    out.FlushCache()
    out = None
    src = None
    return path


@dataclass
class Reference:
    """What plain GDAL reads from a file."""

    count: int
    size: Tuple[int, int]  # (width, height)
    dtypes: List[str]
    nodata: List[Optional[float]]
    crs: Optional[str]  # WKT, None for none
    gt: Tuple[float, ...]
    arrays: List[np.ndarray] = field(default_factory=list)


def reference(gb, path: str) -> Reference:
    """Open ``path`` in plain GDAL and read it whole."""
    ds = gb.gdal.Open(path)
    arrays = [ds.GetRasterBand(i).ReadAsArray() for i in range(1, ds.RasterCount + 1)]
    wkt = ds.GetProjection()
    return Reference(
        count=ds.RasterCount,
        size=(ds.RasterXSize, ds.RasterYSize),
        dtypes=[a.dtype.name for a in arrays],
        nodata=[
            ds.GetRasterBand(i).GetNoDataValue() for i in range(1, ds.RasterCount + 1)
        ],
        crs=wkt or None,
        gt=tuple(ds.GetGeoTransform()),
        arrays=arrays,
    )


# --- the cases -----------------------------------------------------------------------------------------------------

_LOSSLESS_JP2 = ("QUALITY=100", "REVERSIBLE=YES")

RASTER_CASES: List[RasterCase] = [
    RasterCase("GTiff", ".tif", "int16", 2, reader="gtiff_gbx"),
    RasterCase("GTiff", ".tif", "float32", 3, -9999.0, 32633, UTM, reader="gtiff_gbx"),
    RasterCase("COG", ".tif", "float32", 1, -9999.0, 32633, UTM, reader="cog_gbx"),
    RasterCase("PNG", ".png", "uint8", 3, None),
    RasterCase("JPEG", ".jpg", "uint8", 3, None, ramp=True),
    RasterCase("JP2OpenJPEG", ".jp2", "int16", 1, co=_LOSSLESS_JP2),
    RasterCase("HFA", ".img", "int16", 1),
    RasterCase("HFA", ".img", "float32", 2, -9999.0, 32633, UTM),
    RasterCase("AAIGrid", ".asc", "float32"),
    RasterCase("AAIGrid", ".asc", "int32", 1, -9999, 32633, UTM),
    RasterCase("XYZ", ".xyz", "float32", 1, None, None),
    RasterCase("GSAG", ".grd", "float32", 1, None, None),
    RasterCase("GRIB", ".grb2", "float32"),
    RasterCase("NITF", ".ntf", "int16", 1, None),
    RasterCase("EHdr", ".bil", "int16", 1, path_only=True),
    RasterCase("EHdr", ".bil", "float32", 3, -9999.0, 32633, UTM, path_only=True),
    RasterCase("ENVI", ".dat", "float32", 2, -9999.0, path_only=True),
    RasterCase("SAGA", ".sdat", "float32", path_only=True),
    RasterCase(
        "SRTMHGT",
        ".hgt",
        "int16",
        1,
        -32768,
        4326,
        (10 - 1 / 2400, 1 / 1200, 0.0, 51 + 1 / 2400, 0.0, -1 / 1200),
        1201,
        name="N50E010.hgt",
        path_only=True,
    ),
    RasterCase("RMF", ".mtw", "int16", 1, co=("MTW=YES",)),
    RasterCase("BAG", ".bag", "float32", 1, 1000000.0, path_only=True),
    RasterCase("GTX", ".gtx", "float32", 1, -9999.0, path_only=True),
    RasterCase("WEBP", ".webp", "uint8", 3, None, co=("LOSSLESS=YES",)),
    RasterCase("JPEGXL", ".jxl", "uint8", 3, None),
    RasterCase("RMF", ".rsw", "uint8", 3, None),
    RasterCase("netCDF", ".nc", "float32", 1, -9999.0, reader="netcdf_gbx"),
    RasterCase("netCDF", ".nc", "int16", 1, -9999, 32633, UTM),
    RasterCase("MBTiles", ".mbtiles", "uint8", 3, None, 3857, MERC, path_only=True),
    RasterCase(
        "GPKG",
        ".gpkg",
        "uint8",
        3,
        None,
        3857,
        MERC,
        co=("RASTER_TABLE=tiles", "TILE_FORMAT=PNG"),
    ),
]

# the case table by driver, in the order above
BY_DRIVER: Dict[str, List[RasterCase]] = {}
for _c in RASTER_CASES:
    BY_DRIVER.setdefault(_c.driver, []).append(_c)


# --- what does not work today -------------------------------------------------------------------------------------
# (path, case id) -> why the call fails. The test of that path is an expected failure with this reason: it stays red
# the day the call works (strict), and a driver is claimed as tested only when none of its paths is listed.
# Paths: "reader:<name>", "fromfile", "fromcontent", "asformat".
RASTER_FINDINGS: Dict[Tuple[str, str], str] = {
    (
        "asformat",
        "JP2OpenJPEG-int16-4326",
    ): "the encoded tile has no CRS; GDAL's own JP2 writer keeps EPSG:4326",
    (
        "asformat",
        "NITF-int16-4326",
    ): "raises: NITF needs the creation option ICORDS=G or ICORDS=D to store a CRS, and the call passes none",
    (
        "asformat",
        "SRTMHGT-int16-4326",
    ): "raises SystemError 'Unknown GDAL Error' from rasterio's buffered writer",
    (
        "asformat",
        "MBTiles-uint8-3857",
    ): "raises TypeError 'not all arguments converted during string formatting' in rasterio's write_transform",
    (
        "asformat",
        "BAG-float32-4326",
    ): "the encoded tile opens with 0 bands; GDAL's own BAG writer gives 2",
    (
        "asformat",
        "GTX-float32-4326",
    ): "returns an error tile: 'Attempt to create gtx file with extension other than gtx'",
    (
        "asformat",
        "netCDF-float32-4326",
    ): "returns an error tile: 'Unable to create netCDF file ... .tif (Error code 2)'",
    (
        "asformat",
        "GPKG-uint8-3857",
    ): "returns an error tile: \"A file system object called '/vsimem/... .tif' already exists\"",
    (
        "fromcontent",
        "BAG-float32-4326",
    ): "gbx_drivers reports tile_bytes true, but the bytes open with 0 bands; GDAL reads 2 from the file",
    (
        "fromcontent",
        "GTX-float32-4326",
    ): "gbx_drivers reports tile_bytes true, but the bytes do not open: the tile's temporary file is named .tif and GTX identifies by extension",
    (
        "fromcontent",
        "SRTMHGT-int16-4326",
    ): "gbx_drivers reports tile_bytes true, but the bytes do not open: SRTMHGT identifies by the file name",
    (
        "fromcontent",
        "MBTiles-uint8-3857",
    ): "gbx_drivers reports tile_bytes true, but the bytes do not open: the tile's temporary file is named .tif",
}


def read_paths(case: RasterCase) -> List[str]:
    """The GeoBrix paths the read tests run for a case (a ``path_only`` driver is refused as bytes, and that is the
    expected result)."""
    paths = [f"reader:{r}" for r in dict.fromkeys([case.reader, "raster_gbx"])]
    return paths + ["fromfile", "fromcontent"]


def param(case: RasterCase, path: str, *prefix):
    """A pytest parameter for ``case`` on ``path``: an expected failure when the path is a known finding."""
    import pytest

    why = RASTER_FINDINGS.get((path, case.id))
    marks = [pytest.mark.xfail(strict=True, reason=why)] if why else []
    return pytest.param(*prefix, case, id=f"{case.id}-{path}", marks=marks)
