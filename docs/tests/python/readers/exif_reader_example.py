"""exif_gbx reader — EXIF/GPS metadata example (light-only, single source of truth).

Code shown in docs/docs/readers/exif.mdx is imported from here via raw-loader.
Tests verify metadata mode (header-only, no pixel decode) and QC mode (sharpness +
brightness) against a synthetic JPEG fixture created inline with Pillow.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Code snippets (imported into docs/docs/readers/exif.mdx via raw-loader)
# ---------------------------------------------------------------------------

EXIF_METADATA_MODE = """\
# Register the lightweight DataSources (once per session).
from databricks.labs.gbx.ds.register import register
register(spark)

telemetry = (
    spark.read.format("exif_gbx")
    .option("mode", "metadata")
    .load("/Volumes/main/geobrix_samples/geobrix-examples/orthomosaic/raw/")
)
telemetry.select(
    "path", "camera_make", "camera_model",
    "latitude", "longitude", "altitude",
    "image_width", "image_height",
).show(truncate=False)
"""

EXIF_QC_MODE = """\
# QC mode — appends sharpness and brightness (reads pixels; opt-in).
qc = (
    spark.read.format("exif_gbx")
    .option("mode", "qc")
    .load("/Volumes/main/geobrix_samples/geobrix-examples/orthomosaic/raw/")
)
qc.select("path", "sharpness", "brightness").show()
"""

EXIF_FILTER_REGEX = """\
# Scan only JPEG files (skip any non-image files in the directory):
telemetry = (
    spark.read.format("exif_gbx")
    .option("filterRegex", r".*\\.(jpg|jpeg)$")
    .load("/Volumes/main/geobrix_samples/geobrix-examples/orthomosaic/raw/")
)
"""

EXIF_SENSOR_OVERRIDE = """\
# Override sensor width and focal length for cameras without EXIF calibration data:
telemetry = (
    spark.read.format("exif_gbx")
    .option("sensorWidthMm", "6.17")      # DJI Mini 3 — 1/1.3-inch sensor
    .option("focalLengthMm", "4.49")      # 24 mm equiv focal length
    .load("/Volumes/main/geobrix_samples/geobrix-examples/orthomosaic/raw/")
)
"""


# ---------------------------------------------------------------------------
# Fixture helper: synthesise a minimal JPEG with GPS EXIF (no sample-data dep)
# ---------------------------------------------------------------------------


def _make_exif_jpeg(tmp_dir: Path) -> Path:
    """Write a 4000x3000 JPEG with known EXIF GPS/camera tags to tmp_dir.

    Coordinates: 37.75 N, 122.45 W, 30 m altitude.
    Camera: GoPro HERO5; focal length 1733.30 mm rational.
    """
    from PIL import Image
    from PIL.TiffImagePlugin import IFDRational

    p = tmp_dir / "GOPR0001.JPG"
    img = Image.new("RGB", (4000, 3000), (128, 128, 128))
    exif = img.getexif()

    # 0th IFD — camera identity (Make=271, Model=272)
    exif[271] = "GoPro"
    exif[272] = "HERO5"

    # GPS IFD (tag 34853): 37.75 N, 122.45 W, 30 m above sea level
    gps = exif.get_ifd(34853)
    gps[1] = "N"
    gps[2] = (IFDRational(3775, 100), IFDRational(0, 1), IFDRational(0, 1))
    gps[3] = "W"
    gps[4] = (IFDRational(12245, 100), IFDRational(0, 1), IFDRational(0, 1))
    gps[5] = b"\x00"          # AltitudeRef: 0 = above sea level
    gps[6] = IFDRational(30, 1)  # Altitude: 30 m

    # Exif IFD (tag 34665): focal length 173330/100 = 1733.30 mm
    exif.get_ifd(34665)[37386] = IFDRational(173330, 100)

    img.save(str(p), "jpeg", exif=exif.tobytes())
    return p


# ---------------------------------------------------------------------------
# Demo functions (called by test_exif_reader_example.py)
# ---------------------------------------------------------------------------


def demo_exif_metadata_mode(spark, image_dir: str):
    """Metadata mode: one row per file, header-only; GPS + camera fields populated."""
    from databricks.labs.gbx.ds.register import register

    register(spark, only=["exif_gbx"])

    df = (
        spark.read.format("exif_gbx")
        .option("mode", "metadata")
        .load(image_dir)
    )
    rows = df.collect()
    assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
    r = rows[0]
    assert r.camera_make == "GoPro", f"camera_make: {r.camera_make!r}"
    assert r.camera_model == "HERO5", f"camera_model: {r.camera_model!r}"
    assert abs(r.latitude - 37.75) < 1e-4, f"latitude: {r.latitude}"
    assert abs(r.longitude - (-122.45)) < 1e-4, f"longitude: {r.longitude}"
    assert abs(r.altitude - 30.0) < 1e-6, f"altitude: {r.altitude}"
    assert r.image_width == 4000, f"image_width: {r.image_width}"
    assert r.image_height == 3000, f"image_height: {r.image_height}"
    assert r.geom_srid == 4326, f"geom_srid: {r.geom_srid}"
    assert r.geom_wkb is not None, "geom_wkb should not be None when GPS coords are present"


def demo_exif_schema_fields(spark, image_dir: str):
    """All EXIF_META_SCHEMA fields are present in the collected row."""
    from databricks.labs.gbx.ds.register import register

    register(spark, only=["exif_gbx"])
    df = spark.read.format("exif_gbx").load(image_dir)
    row = df.collect()[0].asDict()
    for field in (
        "path", "camera_make", "camera_model", "timestamp",
        "latitude", "longitude", "altitude",
        "focal_length_mm", "sensor_width_mm",
        "image_width", "image_height",
        "geom_wkb", "geom_srid",
    ):
        assert field in row, f"Missing schema field: {field}"


def demo_exif_geom_wkb_point(spark, image_dir: str):
    """geom_wkb decodes to a 2D WKB POINT with the expected coordinates."""
    import struct
    from databricks.labs.gbx.ds.register import register

    register(spark, only=["exif_gbx"])
    df = spark.read.format("exif_gbx").load(image_dir)
    row = df.collect()[0]
    wkb = bytes(row.geom_wkb)
    # WKB little-endian POINT: [byte_order=1][geom_type=1][lon_f64][lat_f64] = 21 bytes
    assert len(wkb) == 21, f"WKB should be 21 bytes, got {len(wkb)}"
    byte_order, geom_type, x, y = struct.unpack("<BIdd", wkb)
    assert byte_order == 1  # little-endian
    assert geom_type == 1   # POINT
    assert abs(x - (-122.45)) < 1e-4, f"lon: {x}"
    assert abs(y - 37.75) < 1e-4, f"lat: {y}"
