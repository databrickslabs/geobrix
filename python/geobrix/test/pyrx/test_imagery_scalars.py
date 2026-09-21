"""Tests for imagery scalars: gsd_from_telemetry, image_sharpness, image_brightness.

TDD: these tests were written BEFORE the implementation (RED → GREEN).
"""

import io

import numpy as np
from PIL import Image

from databricks.labs.gbx.pyrx.imagery import (
    gsd_from_telemetry,
    image_brightness,
    image_sharpness,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_jpeg_bytes(width=64, height=64, mode="L", value=128) -> bytes:
    """Synthesize a flat grayscale (or RGB) JPEG image in memory."""
    arr = np.full(
        (height, width) if mode == "L" else (height, width, 3), value, dtype=np.uint8
    )
    img = Image.fromarray(arr, mode=mode)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    return buf.getvalue()


def _make_png_bytes(width=64, height=64, mode="L", value=128) -> bytes:
    arr = np.full(
        (height, width) if mode == "L" else (height, width, 3), value, dtype=np.uint8
    )
    img = Image.fromarray(arr, mode=mode)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# gsd_from_telemetry
# ---------------------------------------------------------------------------


def test_gsd_from_telemetry_known_value():
    # Formula: (sensor_mm * alt_m * 100) / (focal_mm * width_px)
    gsd = gsd_from_telemetry(
        alt_m=30.0, focal_mm=1733.30, sensor_mm=6.17, width_px=4000
    )
    expected = (6.17 * 30.0 * 100) / (1733.30 * 4000)
    assert abs(gsd - expected) < 1e-9


def test_gsd_from_telemetry_returns_float():
    result = gsd_from_telemetry(
        alt_m=50.0, focal_mm=2000.0, sensor_mm=8.0, width_px=5000
    )
    assert isinstance(result, float)


def test_gsd_from_telemetry_units_cm_per_px():
    # At alt=100 m, focal=100 mm, sensor=10 mm, width=1000 px:
    # gsd = (10 * 100 * 100) / (100 * 1000) = 100000 / 100000 = 1.0 cm/px
    gsd = gsd_from_telemetry(alt_m=100.0, focal_mm=100.0, sensor_mm=10.0, width_px=1000)
    assert abs(gsd - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# image_sharpness
# ---------------------------------------------------------------------------


def test_image_sharpness_flat_image_is_zero():
    """A completely uniform image has zero gradient variance."""
    img_bytes = _make_png_bytes(width=64, height=64, value=128)
    sharp = image_sharpness(img_bytes)
    # Flat image: gradient is all zeros → variance should be near zero
    assert sharp < 1e-3, f"Expected near-zero sharpness for flat image, got {sharp}"


def test_image_sharpness_edge_image_is_high():
    """An image with a hard edge has high gradient variance."""
    arr = np.zeros((64, 64), dtype=np.uint8)
    arr[:, 32:] = 255  # sharp vertical edge in the middle
    img = Image.fromarray(arr, mode="L")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    img_bytes = buf.getvalue()

    sharp = image_sharpness(img_bytes)
    assert sharp > 1.0, f"Expected high sharpness for edge image, got {sharp}"


def test_image_sharpness_returns_float():
    img_bytes = _make_png_bytes()
    result = image_sharpness(img_bytes)
    assert isinstance(result, float)


def test_image_sharpness_rgb_input():
    """RGB image should still return a valid float (converted to grayscale internally)."""
    img_bytes = _make_jpeg_bytes(mode="RGB", value=100)
    result = image_sharpness(img_bytes)
    assert isinstance(result, float)
    assert result >= 0.0


# ---------------------------------------------------------------------------
# image_brightness
# ---------------------------------------------------------------------------


def test_image_brightness_known_value():
    """Flat grayscale image with value V should have mean brightness ≈ V."""
    for val in (0, 128, 255):
        img_bytes = _make_png_bytes(width=32, height=32, value=val)
        bright = image_brightness(img_bytes)
        assert abs(bright - val) < 2.0, f"Expected brightness ≈ {val}, got {bright}"


def test_image_brightness_returns_float():
    img_bytes = _make_png_bytes()
    result = image_brightness(img_bytes)
    assert isinstance(result, float)


def test_image_brightness_rgb_input():
    """RGB input should be converted to grayscale."""
    img_bytes = _make_jpeg_bytes(mode="RGB", value=200)
    result = image_brightness(img_bytes)
    assert isinstance(result, float)
    assert 0.0 <= result <= 255.0


def test_image_brightness_range():
    dark = image_brightness(_make_png_bytes(value=10))
    bright = image_brightness(_make_png_bytes(value=245))
    assert dark < bright


# ---------------------------------------------------------------------------
# pandas_udf wrappers (column-level; require Spark session)
# ---------------------------------------------------------------------------


def test_gsd_udf_spark(spark):
    """gsd_udf should compute the same value as gsd_from_telemetry."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx.imagery import gsd_udf

    df = spark.createDataFrame(
        [(30.0, 1733.30, 6.17, 4000)],
        ["alt_m", "focal_mm", "sensor_mm", "width_px"],
    )
    row = df.select(
        gsd_udf(
            f.col("alt_m"), f.col("focal_mm"), f.col("sensor_mm"), f.col("width_px")
        ).alias("gsd")
    ).first()
    expected = gsd_from_telemetry(30.0, 1733.30, 6.17, 4000)
    assert abs(row["gsd"] - expected) < 1e-9


def test_sharpness_udf_spark(spark):
    """sharpness_udf should return float on a flat image."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx.imagery import sharpness_udf

    img_bytes = _make_png_bytes(value=128)
    df = spark.createDataFrame([(img_bytes,)], ["img"])
    row = df.select(sharpness_udf(f.col("img")).alias("s")).first()
    assert row["s"] < 1e-3


def test_brightness_udf_spark(spark):
    """brightness_udf should return near-128 for a flat gray image."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx.imagery import brightness_udf

    img_bytes = _make_png_bytes(value=128)
    df = spark.createDataFrame([(img_bytes,)], ["img"])
    row = df.select(brightness_udf(f.col("img")).alias("b")).first()
    assert abs(row["b"] - 128.0) < 2.0
