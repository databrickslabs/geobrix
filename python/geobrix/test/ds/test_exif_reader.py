"""Tests for exif_gbx DataSource — metadata mode (header-only, one row per file).

Task 2 of the v0.5.2 photogrammetry phase: EXIF/GPS telemetry extraction
without decoding pixels.
"""

import inspect


def test_exif_gbx_metadata_mode(spark, exif_jpeg):
    from databricks.labs.gbx.ds.register import register

    register(spark, only=["exif_gbx"])
    df = (
        spark.read.format("exif_gbx")
        .option("mode", "metadata")
        .load(str(exif_jpeg.parent))
    )
    rows = df.collect()
    assert len(rows) == 1
    r = rows[0]
    assert r.camera_make == "GoPro" and r.camera_model == "HERO5"
    assert abs(r.latitude - 37.75) < 1e-4 and abs(r.longitude + 122.45) < 1e-4
    assert abs(r.altitude - 30.0) < 1e-6
    assert abs(r.focal_length_mm - 1733.30) < 1e-2
    assert r.image_width == 4000 and r.image_height == 3000
    assert r.geom_srid == 4326 and r.geom_wkb is not None


def test_exif_gbx_schema_completeness(spark, exif_jpeg):
    """All EXIF_META_SCHEMA fields are present in collected rows."""
    from databricks.labs.gbx.ds.register import register

    register(spark, only=["exif_gbx"])
    df = spark.read.format("exif_gbx").load(str(exif_jpeg.parent))
    row = df.collect()[0]
    expected_fields = (
        "path",
        "camera_make",
        "camera_model",
        "timestamp",
        "latitude",
        "longitude",
        "altitude",
        "focal_length_mm",
        "sensor_width_mm",
        "image_width",
        "image_height",
        "geom_wkb",
        "geom_srid",
    )
    for field in expected_fields:
        assert field in row.asDict(), f"Missing field: {field}"


def test_exif_gbx_geom_wkb_is_point(spark, exif_jpeg):
    """geom_wkb decodes to a 2D POINT with the expected coordinates."""
    import struct

    from databricks.labs.gbx.ds.register import register

    register(spark, only=["exif_gbx"])
    df = spark.read.format("exif_gbx").load(str(exif_jpeg.parent))
    row = df.collect()[0]
    wkb = bytes(row.geom_wkb)
    # WKB little-endian POINT: [0x01][0x01,0,0,0][lon_f64][lat_f64]
    assert len(wkb) == 21
    byte_order, geom_type, x, y = struct.unpack("<BIdd", wkb)
    assert byte_order == 1  # little-endian
    assert geom_type == 1  # POINT
    assert abs(x - (-122.45)) < 1e-4
    assert abs(y - 37.75) < 1e-4


def test_exif_gbx_default_mode_is_metadata(spark, exif_jpeg):
    """Omitting mode= defaults to metadata (no error)."""
    from databricks.labs.gbx.ds.register import register

    register(spark, only=["exif_gbx"])
    df = spark.read.format("exif_gbx").load(str(exif_jpeg.parent))
    rows = df.collect()
    assert len(rows) == 1


def test_exif_gbx_serverless_safe():
    """No forbidden Spark APIs (sparkContext, _jvm, .rdd, _jsc) in exif.py."""
    from databricks.labs.gbx.ds import exif

    src = inspect.getsource(exif)
    for forbidden in ("sparkContext", "_jvm", ".rdd", "_jsc"):
        assert forbidden not in src, f"forbidden API {forbidden!r} found in exif.py"


def test_exif_gbx_qc_mode_adds_metrics(spark, exif_jpeg):
    """qc mode appends sharpness and brightness; brightness ~128 on solid-gray fixture."""
    from databricks.labs.gbx.ds.register import register

    register(spark, only=["exif_gbx"])
    df = spark.read.format("exif_gbx").option("mode", "qc").load(str(exif_jpeg.parent))
    r = df.collect()[0]
    assert r.sharpness is not None and r.brightness is not None
    assert 0.0 <= r.brightness <= 255.0
    assert abs(r.brightness - 128.0) < 5.0  # solid gray fixture
