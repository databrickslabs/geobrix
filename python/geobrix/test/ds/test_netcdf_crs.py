"""Task 5: NetCDF writer CRS preservation for non-EPSG CRS.

Two test levels:
1. Unit tests of the CF crs_wkt extraction helper (no netCDF4 needed).
   Asserts that a non-EPSG CRS (ESRI:54008) produces a non-null WKT string
   from ds.crs, and that crs_to_canonical returns the authority string.
2. Round-trip integration tests (netCDF4-guarded): write a non-EPSG raster
   tile to NetCDF, read back, assert CRS is preserved. These run in Docker
   where netCDF4 is installed; locally they are skipped with a clear message.

The module-level importorskip is NOT used so that unit tests always collect
and run. The integration tests carry individual skip marks instead.
"""

from __future__ import annotations

import pytest
from rasterio.crs import CRS

# -----------------------------------------------------------------------
# Unit tests — no netCDF4 required, test the CRS-string helpers directly.
# These MUST run (and be RED before the fix, GREEN after) even locally.
# -----------------------------------------------------------------------


def test_crs_to_canonical_epsg():
    """EPSG CRS produces authority string, not WKT."""
    from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical

    crs = CRS.from_epsg(4326)
    result = crs_to_canonical(crs)
    assert result == "EPSG:4326"


def test_crs_to_canonical_esri54008():
    """ESRI:54008 (non-EPSG, World Sinusoidal) produces authority string."""
    from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical

    crs = CRS.from_user_input("ESRI:54008")
    result = crs_to_canonical(crs)
    assert result is not None
    assert result == "ESRI:54008"


def test_crs_to_canonical_none_safe():
    """crs_to_canonical(None) returns None without raising."""
    from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical

    assert crs_to_canonical(None) is None


def test_esri54008_wkt_is_non_null():
    """For ESRI:54008, to_wkt() is non-null — the CF crs_wkt attribute value.

    This is the core property the NetCDF writer relies on: even when EPSG
    to_epsg() returns None, crs.to_wkt() gives a string we can store in the
    CF grid_mapping crs_wkt attribute so the CRS survives round-trip.
    """
    crs = CRS.from_user_input("ESRI:54008")
    assert crs.to_epsg() is None, "ESRI:54008 must have no EPSG code"
    wkt = crs.to_wkt()
    assert wkt is not None and len(wkt) > 10, "Expected non-trivial WKT for ESRI:54008"
    # The WKT round-trips: parsing it back gives the same CRS
    crs_rt = CRS.from_wkt(wkt)
    assert crs_rt == crs, "WKT round-trip must recover the same CRS"


def test_crs_canonical_for_proj4_string():
    """A PROJ4 CRS that has an authority maps to its authority string."""
    from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical, resolve_crs

    proj4 = "+proj=longlat +datum=WGS84 +no_defs"
    crs = resolve_crs(proj4)
    result = crs_to_canonical(crs)
    assert result is not None
    # WGS84 PROJ4 should map to EPSG:4326 or at worst return WKT
    assert "EPSG" in result or len(result) > 10


# -----------------------------------------------------------------------
# Integration tests — need netCDF4.  Skip locally; run in Docker.
# Use pytest.importorskip inside each test to skip cleanly on absence.
# -----------------------------------------------------------------------


def _netcdf4():
    """Return the netCDF4 module or skip the test."""
    return pytest.importorskip("netCDF4", reason="netCDF4 not installed; run in Docker")


def _make_esri54008_tile_bytes():
    """Build an in-memory GeoTIFF with ESRI:54008 CRS and return (bytes, array)."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    crs = CRS.from_user_input("ESRI:54008")
    transform = from_origin(0.0, 100_000.0, 10_000.0, 10_000.0)
    arr = np.arange(12, dtype="float32").reshape(3, 4)
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(arr, 1)
        return mf.read(), arr


def _raster_df_from_bytes(spark, source, tile_bytes):
    """Build a minimal (source, tile) DataFrame from raw tile bytes."""
    from pyspark.sql import Row
    from pyspark.sql.types import (
        BinaryType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    tile_schema = StructType(
        [
            StructField("cellid", LongType(), True),
            StructField("raster", BinaryType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )
    outer_schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("tile", tile_schema, True),
        ]
    )
    rows = [
        Row(
            source=source,
            tile=Row(cellid=0, raster=bytearray(tile_bytes), metadata={}),
        )
    ]
    return spark.createDataFrame(rows, schema=outer_schema)


def test_netcdf_raster_write_esri54008_crs_preserved_in_file(tmp_path):
    """Multifile write of ESRI:54008 tile: crs variable has crs_wkt attribute.

    Pre-fix: crs.to_epsg() returns None -> no crs variable written -> CRS lost.
    Post-fix: crs_wkt attribute carries the WKT so the CRS is preserved.
    """
    import os

    nc4 = _netcdf4()
    import numpy as np
    from pyspark.sql import Row
    from pyspark.sql.types import (
        BinaryType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    from databricks.labs.gbx.ds._write_netcdf import NetcdfRasterGbxWriter

    tile_bytes, arr = _make_esri54008_tile_bytes()

    tile_schema = StructType(
        [
            StructField("cellid", LongType(), True),
            StructField("raster", BinaryType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )
    outer_schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("tile", tile_schema, True),
        ]
    )

    outdir = str(tmp_path / "out_esri")
    writer = NetcdfRasterGbxWriter(
        {"path": outdir},
        outer_schema,
        overwrite=True,
    )
    row = Row(
        source='NETCDF:"/tmp/x.nc":band',
        tile=Row(cellid=0, raster=bytearray(tile_bytes), metadata={}),
    )
    writer.write(iter([row]))

    nc_files = [f for f in os.listdir(outdir) if f.endswith(".nc")]
    assert len(nc_files) == 1, f"Expected 1 .nc, got {nc_files}"

    with nc4.Dataset(os.path.join(outdir, nc_files[0]), "r") as nc:
        assert "crs" in nc.variables, "Expected a 'crs' grid_mapping variable"
        crs_var = nc.variables["crs"]
        has_wkt = hasattr(crs_var, "crs_wkt")
        has_canonical = hasattr(crs_var, "crs_canonical")
        assert (
            has_wkt or has_canonical
        ), f"crs variable must carry crs_wkt or crs_canonical; attrs: {crs_var.ncattrs()}"
        if has_wkt:
            wkt_stored = getattr(crs_var, "crs_wkt")
            assert wkt_stored is not None and len(str(wkt_stored)) > 10
            crs_rt = CRS.from_wkt(str(wkt_stored))
            expected = CRS.from_user_input("ESRI:54008")
            assert crs_rt == expected, f"Round-trip CRS mismatch: {crs_rt}"
        if has_canonical:
            canonical_stored = getattr(crs_var, "crs_canonical")
            assert canonical_stored == "ESRI:54008"


def test_netcdf_reader_crs_string_reads_crs_wkt(tmp_path):
    """_crs_string reads crs_wkt attribute from a CF grid_mapping variable.

    Write a minimal .nc manually with a 'crs' variable that has a crs_wkt
    attribute (no spatial_epsg). Then call _netcdf._crs_string and confirm it
    returns a CRS string parseable as ESRI:54008.
    """
    import numpy as np

    nc4 = _netcdf4()

    from databricks.labs.gbx.ds._netcdf import _crs_string

    esri_wkt = CRS.from_user_input("ESRI:54008").to_wkt()
    nc_path = str(tmp_path / "test_crs_wkt.nc")
    with nc4.Dataset(nc_path, "w") as nc:
        nc.createDimension("lat", 2)
        nc.createDimension("lon", 2)
        lat = nc.createVariable("lat", "f8", ("lat",))
        lat.standard_name = "latitude"
        lat[:] = [10.0, 9.5]
        lon = nc.createVariable("lon", "f8", ("lon",))
        lon.standard_name = "longitude"
        lon[:] = [5.0, 5.5]
        crs_var = nc.createVariable("crs", "i4")
        crs_var.grid_mapping_name = "transverse_mercator"
        crs_var.crs_wkt = esri_wkt
        dv = nc.createVariable("data", "f4", ("lat", "lon"))
        dv.grid_mapping = "crs"
        dv[:] = np.arange(4, dtype="float32").reshape(2, 2)

    import xarray as xr

    with xr.open_dataset(nc_path) as ds:
        result = _crs_string(ds)

    assert result is not None
    assert result != "EPSG:4326", "Should have returned non-default CRS from crs_wkt"
    crs_rt = CRS.from_user_input(result)
    expected = CRS.from_user_input("ESRI:54008")
    assert crs_rt == expected, f"CRS from crs_wkt didn't round-trip: {result}"


def test_netcdf_roundtrip_esri54008_via_spark(spark, tmp_path):
    """Full Spark round-trip: write ESRI:54008 tile -> netcdf_gbx writer -> re-read.

    The re-read uses the netcdf_gbx reader which goes through _netcdf._crs_string.
    Assert the decoded tile's CRS == ESRI:54008.
    """
    _netcdf4()

    from rasterio.io import MemoryFile

    from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource

    spark.dataSource.register(NetcdfGbxDataSource)

    tile_bytes, arr = _make_esri54008_tile_bytes()
    df = _raster_df_from_bytes(spark, 'NETCDF:"/tmp/x.nc":band', tile_bytes)

    outdir = tmp_path / "rt_esri54008"
    df.write.format("netcdf_gbx").mode("overwrite").save(str(outdir))

    rows = spark.read.format("netcdf_gbx").load(str(outdir)).collect()
    assert len(rows) == 1
    with MemoryFile(bytes(rows[0]["tile"]["raster"])) as mf, mf.open() as ds:
        crs_out = ds.crs

    expected = CRS.from_user_input("ESRI:54008")
    assert (
        crs_out == expected
    ), f"CRS not preserved after NetCDF round-trip: got {crs_out}"
