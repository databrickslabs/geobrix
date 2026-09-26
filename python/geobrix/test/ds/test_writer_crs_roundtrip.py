"""Task 3 (SP2-CRS): round-trip regression for writers: _write_lidar, cog_writer, vector, _write_netcdf, gridagg.

Each test calls the ACTUAL production writer code path so it would FAIL if
the corresponding writer fix were reverted (with Tasks 1-2 helpers intact).
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

import numpy as np
import pytest
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_bounds

# ── 1. _crs_to_srid_proj (vector.py lines 182-198) ───────────────────────────


@pytest.mark.parametrize(
    "src,expected_srid",
    [
        ("EPSG:4326", "4326"),
        ("ESRI:54008", "54008"),
        ("OGC:CRS84", "0"),  # non-integer authority code → no integer srid
    ],
    ids=["epsg", "esri", "ogc"],
)
def test_crs_to_srid_proj_production(src, expected_srid):
    """ds/vector.py _crs_to_srid_proj: production function returns correct srid.

    Old pyproj.CRS.from_user_input(crs).to_authority()[1] returns "CRS84" for
    OGC:CRS84 (non-integer code) instead of "0".  FAILS on revert for OGC case.
    """
    from databricks.labs.gbx.ds.vector import _crs_to_srid_proj

    srid, proj4 = _crs_to_srid_proj(src)
    assert srid == expected_srid, f"srid for {src}: got {srid!r}"
    assert "+proj=" in proj4, f"proj4 for {src}: got {proj4!r}"


# ── 2. _make_layer_srs (vector.py OGR-SRS branch, lines 1387-1391) ───────────


@pytest.mark.parametrize(
    "src,expected_auth,expected_code",
    [
        ("EPSG:4326", "EPSG", "4326"),
        ("ESRI:54008", "ESRI", "54008"),
    ],
    ids=["epsg", "esri"],
)
def test_make_layer_srs_authority(src, expected_auth, expected_code):
    """ds/vector.py _make_layer_srs: SetFromUserInput carries ESRI authority.

    Old ImportFromProj4("ESRI:54008") fails silently → null SRS → GetAuthorityName
    returns None.  FAILS for ESRI:54008 on revert.
    """
    pytest.importorskip("osgeo.osr")
    from databricks.labs.gbx.ds.vector import _make_layer_srs

    srs = _make_layer_srs(src)
    assert srs is not None, f"SRS for {src} is None"
    auth_name = srs.GetAuthorityName(None)
    auth_code = srs.GetAuthorityCode(None)
    assert auth_name == expected_auth, f"Authority name for {src}: {auth_name!r}"
    assert auth_code == expected_code, f"Authority code for {src}: {auth_code!r}"


def test_make_layer_srs_ogc_crs84_is_geographic():
    """ds/vector.py _make_layer_srs: OGC:CRS84 produces a valid geographic SRS.

    Old ImportFromProj4("OGC:CRS84") fails silently → null/empty SRS → IsGeographic()
    returns 0.  FAILS on revert.
    """
    pytest.importorskip("osgeo.osr")
    from databricks.labs.gbx.ds.vector import _make_layer_srs

    srs = _make_layer_srs("OGC:CRS84")
    assert srs is not None, "SRS for OGC:CRS84 is None"
    assert srs.IsGeographic(), "OGC:CRS84 should be recognized as geographic"


# ── 3. _build_mosaic_vrt SRS element (cog_writer.py line 320) ────────────────


@pytest.mark.parametrize(
    "src,expected",
    [
        ("EPSG:4326", "EPSG:4326"),
        ("ESRI:54008", "ESRI:54008"),
    ],
    ids=["epsg", "esri"],
)
def test_build_mosaic_vrt_srs_preserves_authority(src, expected, tmp_path):
    """ds/cog_writer._build_mosaic_vrt: <SRS> element is crs_to_canonical, not raw WKT.

    Old crs.to_wkt() embeds a long WKT blob; new crs_to_canonical(crs) emits the
    authority string.  FAILS for ESRI:54008 on revert.
    """
    from databricks.labs.gbx.ds.cog_writer import _build_mosaic_vrt

    crs = CRS.from_user_input(src)
    transform = from_bounds(-180, -90, 180, 90, 4, 4)
    tile = tmp_path / "tile.tif"
    with rasterio.open(
        str(tile),
        "w",
        driver="GTiff",
        crs=crs,
        transform=transform,
        count=1,
        dtype="uint8",
        width=4,
        height=4,
    ) as dst:
        dst.write(np.zeros((1, 4, 4), dtype="uint8"))

    vrt = _build_mosaic_vrt([str(tile)], str(tmp_path))
    srs_text = ET.parse(vrt).getroot().find("SRS").text
    assert srs_text == expected, f"SRS element for {src}: {srs_text!r}"


# ── 4. _write_crs_var (_write_netcdf.py lines 67-71) ─────────────────────────


def test_write_crs_var_esri_sets_spatial_epsg(tmp_path):
    """_write_netcdf._write_crs_var: ESRI:54008 sets spatial_epsg=54008.

    Old .to_epsg() returns None for ESRI codes → spatial_epsg NOT set.
    New authority_srid_of returns 54008 → spatial_epsg = 54008.
    FAILS on revert.  Skips when netCDF4 is not installed (runs in Docker).
    """
    netCDF4 = pytest.importorskip("netCDF4")
    from databricks.labs.gbx.ds._write_netcdf import _write_crs_var

    nc_path = str(tmp_path / "test.nc")
    with netCDF4.Dataset(nc_path, "w") as nc:
        _write_crs_var(nc, "ESRI:54008")
        epsg = getattr(nc.variables["crs"], "spatial_epsg", None)
    assert epsg == 54008, f"Expected spatial_epsg=54008, got {epsg!r}"


# ── 5. _infer_crs_from_parts (_write_lidar.py lines 147-151) ─────────────────


def _write_las_esri54008(path: str) -> None:
    """Write a minimal LAS 1.4 file with ESRI:54008 embedded as OGC WKT VLR."""
    import laspy
    import pyproj

    esri_crs = pyproj.CRS.from_authority("ESRI", "54008")
    hdr = laspy.LasHeader(point_format=0, version="1.4")
    hdr.offsets = np.zeros(3)
    hdr.scales = np.full(3, 0.01)
    las = laspy.LasData(header=hdr)
    las.x = np.array([0.0, 1.0, 2.0])
    las.y = np.zeros(3)
    las.z = np.full(3, 10.0)
    vlr = laspy.VLR(
        user_id="LASF_Projection",
        record_id=2112,
        description="OGC Transformation Record",
        record_data=esri_crs.to_wkt().encode("utf-8"),
    )
    las.vlrs.append(vlr)
    las.write(path)


def test_infer_crs_from_parts_esri54008(tmp_path):
    """_write_lidar._infer_crs_from_parts: ESRI:54008 header → canonical "ESRI:54008".

    Old code: _parsed.to_epsg()→None, fallback to _parsed.to_wkt() → raw WKT blob.
    New code: crs_to_canonical(resolve_crs(_parsed.to_wkt())) → "ESRI:54008".
    FAILS on revert.
    """
    from databricks.labs.gbx.ds._write_lidar import _infer_crs_from_parts

    path = str(tmp_path / "esri.las")
    _write_las_esri54008(path)
    crs = _infer_crs_from_parts([path])
    assert crs == "ESRI:54008", f"Expected 'ESRI:54008', got {crs!r}"


# ── 6. _warp_to_4326_if_needed (gridagg.py line 499) ─────────────────────────


def _make_raster_bytes(crs_str: str, size: int = 4):
    """Return in-memory GTiff bytes with the given CRS (arbitrary valid transform)."""
    from rasterio.io import MemoryFile

    crs = CRS.from_user_input(crs_str)
    transform = from_bounds(-180, -90, 180, 90, size, size)
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            crs=crs,
            transform=transform,
            count=1,
            dtype="uint8",
            width=size,
            height=size,
        ) as dst:
            dst.write(np.ones((1, size, size), dtype="uint8"))
        return mf.read()


def test_warp_to_4326_if_needed_4326_returns_none():
    """gridagg._warp_to_4326_if_needed: EPSG:4326 raster → None (already grid-native).

    resolve_crs(4326) replaces _RioCRS.from_epsg(4326) for _WGS84 constant;
    both produce the same rasterio CRS → equality holds → no warp needed.
    """
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core.gridagg import _warp_to_4326_if_needed

    data = _make_raster_bytes("EPSG:4326")
    with MemoryFile(data) as mf, mf.open() as ds:
        result = _warp_to_4326_if_needed(ds, None)
    assert (
        result is None
    ), f"EPSG:4326 raster should skip warp, got bytes of len {len(result) if result else 0}"
