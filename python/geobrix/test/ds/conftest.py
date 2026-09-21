"""Shared fixtures for the gbx.ds DataSource tests.

Also pre-caches the real third-party ``pmtiles`` package so pytest's ``prepend``
importmode cannot shadow it with a same-named test directory. The test directory
was renamed from ``test/pmtiles/`` to ``test/pmtiles_bindings/`` to eliminate the
collision; the guard below is retained as defensive insurance.
"""

import logging
import os
import sys
from pathlib import Path

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin


def _guard_real_pmtiles() -> None:
    """Cache the real third-party ``pmtiles`` in sys.modules before any test
    module imports it, guarding against any future same-named test directory
    under pytest's ``prepend`` importmode."""
    test_root = str(Path(__file__).resolve().parents[1])  # .../python/geobrix/test
    saved = [p for p in sys.path if p == test_root]
    sys.path = [p for p in sys.path if p != test_root]
    try:
        import pmtiles  # noqa: F401
        import pmtiles.reader  # noqa: F401
        import pmtiles.tile  # noqa: F401
        import pmtiles.writer  # noqa: F401
    finally:
        sys.path = saved + sys.path


_guard_real_pmtiles()

# Ensure the PySpark worker uses the same Python as the driver.  Without this
# a local run against a system python3 silently picks the wrong interpreter
# and every DataSource test fails with PYTHON_VERSION_MISMATCH.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(autouse=True)
def _isolate_gdal_env():
    """Snapshot and restore GDAL/PROJ env vars around every test."""
    keys = ("GDAL_DATA", "PROJ_DATA", "PROJ_LIB")
    saved = {k: os.environ.get(k) for k in keys}
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def make_geotiff_bytes(width=4, height=3, count=1, epsg=4326, nodata=-9999.0):
    """Return in-memory single/multi-band GTiff bytes with a known georeference.

    Origin (ulx, uly) = (10.0, 50.0); pixel size 0.5 x 0.5 (north-up).
    So extent = (10.0, 50.0 - 0.5*height) .. (10.0 + 0.5*width, 50.0).
    """
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=nodata,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            for b in range(1, count + 1):
                ds.write(data + (b - 1) * 100, b)
        return mf.read()


@pytest.fixture(scope="session")
def gtiff_bytes():
    return make_geotiff_bytes()


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-ds-tests")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session


@pytest.fixture
def exif_jpeg(tmp_path):
    """Synthesize a JPEG with known EXIF GPS/focal data for exif_gbx tests.

    Coordinates: 37.75 N, 122.45 W, 30 m altitude.
    Focal length: 1733.30 (173330/100 rational).
    Camera: GoPro HERO5, image 4000x3000.

    Uses Pillow's native ``Image.Exif`` / ``IFDRational`` API — no piexif dep.
    EXIF IFD tag numbers follow the TIFF/EXIF spec directly:
      0th IFD  271=Make, 272=Model
      GPS IFD  1=LatRef, 2=Lat(DMS), 3=LonRef, 4=Lon(DMS), 5=AltRef, 6=Alt
      Exif IFD 37386=FocalLength
    """
    from PIL import Image
    from PIL.TiffImagePlugin import IFDRational

    p = tmp_path / "GOPR0001.JPG"
    img = Image.new("RGB", (4000, 3000), (128, 128, 128))
    exif = img.getexif()

    # 0th IFD — camera identity
    exif[271] = "GoPro"  # Make
    exif[272] = "HERO5"  # Model

    # GPS IFD (tag 34853): 37.75 N, 122.45 W, 30 m above sea level
    gps_ifd = exif.get_ifd(34853)
    gps_ifd[1] = "N"  # GPSLatitudeRef
    gps_ifd[2] = (  # GPSLatitude: degrees=37.75, minutes=0, seconds=0
        IFDRational(3775, 100),
        IFDRational(0, 1),
        IFDRational(0, 1),
    )
    gps_ifd[3] = "W"  # GPSLongitudeRef
    gps_ifd[4] = (  # GPSLongitude: degrees=122.45, minutes=0, seconds=0
        IFDRational(12245, 100),
        IFDRational(0, 1),
        IFDRational(0, 1),
    )
    gps_ifd[5] = b"\x00"  # GPSAltitudeRef: 0 = above sea level (BYTE)
    gps_ifd[6] = IFDRational(30, 1)  # GPSAltitude: 30 m

    # Exif IFD (tag 34665): focal length 173330/100 = 1733.30 mm
    exif_ifd = exif.get_ifd(34665)
    exif_ifd[37386] = IFDRational(173330, 100)  # FocalLength

    img.save(str(p), "jpeg", exif=exif.tobytes())
    return p
