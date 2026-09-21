"""Imagery scalar utilities for photogrammetry pipelines.

Pure-Python / NumPy / PIL cores (no Spark session access) plus Arrow-based
pandas_udf wrappers for DataFrame use.

These helpers are imported directly by the ``exif_gbx`` ``qc`` mode and by the
orthomosaic Phase-1 example notebook — they must remain importable without a
SparkContext:

    from databricks.labs.gbx.pyrx.imagery import (
        gsd_from_telemetry,
        image_sharpness,
        image_brightness,
    )

Serverless-safe: no ``spark``, no ``sparkContext``, no ``_jvm``, no ``.rdd``.
"""

from __future__ import annotations

import io

import numpy as np
import pandas as pd
from PIL import Image
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

# ---------------------------------------------------------------------------
# Pure-Python cores
# ---------------------------------------------------------------------------


def gsd_from_telemetry(
    alt_m: float,
    focal_mm: float,
    sensor_mm: float,
    width_px: int,
) -> float:
    """Compute ground sampling distance in **centimetres per pixel**.

    Uses the standard pinhole-camera formula:

        GSD (cm/px) = (sensor_mm * alt_m * 100) / (focal_mm * width_px)

    Parameters
    ----------
    alt_m:
        Flight altitude above ground (metres).
    focal_mm:
        Lens focal length (millimetres).  For a 35 mm-equivalent focal length
        stored in EXIF, divide by the crop factor first.
    sensor_mm:
        Sensor width (millimetres) — the physical dimension that corresponds
        to ``width_px``.
    width_px:
        Image width (pixels) matching ``sensor_mm``.

    Returns
    -------
    float
        GSD in cm/px.  Smaller values indicate higher spatial resolution.
    """
    return float((sensor_mm * alt_m * 100.0) / (focal_mm * width_px))


def image_sharpness(img_bytes: bytes) -> float:
    """Estimate image sharpness as the variance of the grayscale gradient magnitude.

    Applies ``numpy.gradient`` (first-order central differences) on both axes of
    the grayscale image to obtain the gradient components, then computes the
    variance of the resulting gradient magnitude.  A flat, blurry image returns a
    value near zero; a sharp image with edges returns a larger value.

    Parameters
    ----------
    img_bytes:
        Raw image bytes (any PIL-readable format: JPEG, PNG, TIFF, …).

    Returns
    -------
    float
        Variance of the gradient magnitude (non-negative).
    """
    img = Image.open(io.BytesIO(img_bytes)).convert("L")
    arr = np.asarray(img, dtype=np.float32)
    gy, gx = np.gradient(arr)
    magnitude = np.sqrt(gx**2 + gy**2)
    return float(np.var(magnitude))


def image_brightness(img_bytes: bytes) -> float:
    """Compute mean grayscale intensity (0–255).

    Parameters
    ----------
    img_bytes:
        Raw image bytes (any PIL-readable format).

    Returns
    -------
    float
        Mean pixel intensity of the grayscale image, in the range [0, 255].
    """
    img = Image.open(io.BytesIO(img_bytes)).convert("L")
    arr = np.asarray(img, dtype=np.float32)
    return float(np.mean(arr))


# ---------------------------------------------------------------------------
# pandas_udf wrappers for DataFrame / SQL use
# ---------------------------------------------------------------------------


@pandas_udf(DoubleType())
def gsd_udf(
    alt_m: pd.Series,
    focal_mm: pd.Series,
    sensor_mm: pd.Series,
    width_px: pd.Series,
) -> pd.Series:
    """Arrow-vectorised wrapper for :func:`gsd_from_telemetry`.

    All four series must have matching length (one row per image).
    """
    return (sensor_mm * alt_m * 100.0) / (focal_mm * width_px)


@pandas_udf(DoubleType())
def sharpness_udf(img: pd.Series) -> pd.Series:
    """Arrow-vectorised wrapper for :func:`image_sharpness`.

    Parameters
    ----------
    img:
        Series of ``bytes`` — raw image file contents.
    """
    return img.apply(image_sharpness)


@pandas_udf(DoubleType())
def brightness_udf(img: pd.Series) -> pd.Series:
    """Arrow-vectorised wrapper for :func:`image_brightness`.

    Parameters
    ----------
    img:
        Series of ``bytes`` — raw image file contents.
    """
    return img.apply(image_brightness)
