"""Light Spark Python DataSource for EXIF/GPS metadata from drone/camera images (exif_gbx).

mode="metadata" (default): one row per file, header-only (cheap; never loads pixels).
mode="qc":                  appends sharpness + brightness pixel metrics (opt-in).

Serverless-safe: session-free partitions()/read(); one InputPartition per file.
Uses no forbidden Spark internal APIs — safe on Spark Connect / Serverless.
"""

import os
import struct
import warnings
from typing import TYPE_CHECKING, Dict, Iterator, Optional, Sequence

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    BinaryType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.gbx.ds import _listing
from databricks.labs.gbx.ds.file_gbx import list_local_files

if TYPE_CHECKING:
    import pyarrow as pa

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

EXIF_META_SCHEMA = StructType(
    [
        StructField("path", StringType(), False),
        StructField("camera_make", StringType(), True),
        StructField("camera_model", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("altitude", DoubleType(), True),
        StructField("focal_length_mm", DoubleType(), True),
        StructField("sensor_width_mm", DoubleType(), True),
        StructField("image_width", IntegerType(), True),
        StructField("image_height", IntegerType(), True),
        StructField("geom_wkb", BinaryType(), True),
        StructField("geom_srid", IntegerType(), True),
    ]
)

# QC mode extends metadata schema with pixel-derived metrics.
EXIF_QC_SCHEMA = StructType(
    EXIF_META_SCHEMA.fields
    + [
        StructField("sharpness", DoubleType(), True),
        StructField("brightness", DoubleType(), True),
    ]
)

# Extensions recognized as camera images for EXIF extraction.
_EXIF_EXTS = (".jpg", ".jpeg", ".tif", ".tiff")


# ---------------------------------------------------------------------------
# EXIF parsing helpers
# ---------------------------------------------------------------------------


def _dms_to_decimal(dms_values, ref_str: str) -> Optional[float]:
    """Convert an exifread DMS rational list to decimal degrees.

    ``dms_values`` is the ``.values`` attribute of an exifread GPS DMS ``IfdTag``
    — a three-element list of ``Ratio`` objects (degrees, minutes, seconds).
    ``ref_str`` is the corresponding reference string (e.g. "N", "S", "E", "W").
    Returns ``None`` on any parse error so a missing/malformed tag is silently
    skipped rather than crashing the reader.
    """
    try:
        d = float(dms_values[0])
        m = float(dms_values[1])
        s = float(dms_values[2])
        decimal = d + m / 60.0 + s / 3600.0
        if str(ref_str).strip().upper() in ("S", "W"):
            decimal = -decimal
        return decimal
    except Exception:  # noqa: BLE001 — malformed/absent DMS, skip silently
        return None


def _rational_first(tag) -> Optional[float]:
    """Extract the first rational value from an exifread ``IfdTag`` as a float.

    Returns ``None`` on any parse error.
    """
    try:
        vals = tag.values
        if vals:
            return float(vals[0])
        return None
    except Exception:  # noqa: BLE001
        return None


def _parse_timestamp(ts_str: str):
    """Parse an EXIF datetime string ``YYYY:MM:DD HH:MM:SS`` to a Python datetime.

    Returns ``None`` when the string is blank or malformed.
    """
    import datetime

    try:
        return datetime.datetime.strptime(ts_str.strip(), "%Y:%m:%d %H:%M:%S")
    except Exception:  # noqa: BLE001
        return None


def _encode_point_wkb(lon: float, lat: float) -> bytes:
    """Encode a 2-D POINT(lon lat) as standard WKB (OGC; little-endian; no SRID).

    Byte layout: [0x01][wkbType=1 LE u32][x f64][y f64] = 21 bytes.
    Compatible with Spark's ``ST_GeomFromWKB`` and the native ``ST_*`` functions.
    """
    return struct.pack("<BIdd", 1, 1, lon, lat)


# ---------------------------------------------------------------------------
# DataSource V2 partitioning
# ---------------------------------------------------------------------------


class _ExifFilePartition(InputPartition):
    def __init__(self, file_path: str):
        self.file_path = file_path


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------


def _gps_altitude(tags) -> Optional[float]:
    """Extract GPS altitude (m) from exifread tags, applying AltitudeRef sign."""
    if "GPS GPSAltitude" not in tags:
        return None
    alt_val = _rational_first(tags["GPS GPSAltitude"])
    if alt_val is None:
        return None
    alt_sign = 1
    if "GPS GPSAltitudeRef" in tags:
        try:
            ref_vals = tags["GPS GPSAltitudeRef"].values
            first = ref_vals[0] if isinstance(ref_vals, list) else ref_vals
            if int(first) == 1:
                alt_sign = -1
        except Exception:  # noqa: BLE001
            pass
    return alt_val * alt_sign


def _image_dimensions(local: str, tags) -> "tuple[Optional[int], Optional[int]]":
    """Read image pixel dimensions without decoding pixels.

    Tries PIL's lazy ``Image.open()`` (header-only), then falls back to
    EXIF pixel-dimension tags written by some cameras.
    """
    try:
        from PIL import Image as _PILImage

        with _PILImage.open(local) as img:
            return img.size  # (width, height)
    except Exception:  # noqa: BLE001 — PIL absent or corrupt image
        pass
    # Fallback: EXIF pixel-dimension tags.
    width: Optional[int] = None
    height: Optional[int] = None
    for w_key in ("EXIF ExifImageWidth", "EXIF PixelXDimension"):
        if w_key in tags:
            try:
                width = int(str(tags[w_key]))
                break
            except Exception:  # noqa: BLE001
                pass
    for h_key in ("EXIF ExifImageLength", "EXIF PixelYDimension"):
        if h_key in tags:
            try:
                height = int(str(tags[h_key]))
                break
            except Exception:  # noqa: BLE001
                pass
    return width, height


class ExifGbxReader(DataSourceReader):
    """Session-free reader for ``exif_gbx``.

    One ``InputPartition`` per image file; ``partitions()`` and ``read()`` are
    Serverless-safe: no SparkSession, no JVM, no forbidden Spark internal APIs.
    """

    def __init__(self, options: Dict[str, str]):
        raw_path = options.get("path")
        if not raw_path:
            raise ValueError("exif_gbx requires a 'path' (e.g. .load(path)).")
        self.path = _listing.to_local_path(raw_path)
        self.mode = (options.get("mode") or "metadata").lower()
        self.filter_regex = options.get("filterRegex") or None
        self.chunk_size = int(options.get("chunkSize", "1000"))
        # Override options for absent/imprecise EXIF tags
        _sw = options.get("sensorWidthMm")
        self.sensor_width_mm: Optional[float] = float(_sw) if _sw is not None else None
        _fl = options.get("focalLengthMm")
        self.focal_length_mm_override: Optional[float] = (
            float(_fl) if _fl is not None else None
        )
        # Pre-bake the connect-aware materialize cap (driver-side) so that
        # _read_qc() workers can use it without resolving a session on the executor.
        if self.mode == "qc":
            from databricks.labs.gbx.ds.file_gbx import report_detected_cap

            self._qc_cap_bytes: Optional[int] = report_detected_cap()
        else:
            self._qc_cap_bytes = None

    def partitions(self) -> Sequence[InputPartition]:
        import re as _re

        files = list_local_files(self.path, extensions=_EXIF_EXTS)
        if self.filter_regex:
            pat = _re.compile(self.filter_regex)
            files = [f for f in files if pat.search(f)]
        return [_ExifFilePartition(f) for f in files]

    def read(self, partition: "_ExifFilePartition") -> Iterator["pa.RecordBatch"]:
        if self.mode == "metadata":
            yield from self._read_metadata(partition.file_path)
        elif self.mode == "qc":
            yield from self._read_qc(partition.file_path)
        else:
            warnings.warn(
                f"exif_gbx: mode={self.mode!r} is not implemented; "
                "supported modes are 'metadata' and 'qc'",
                stacklevel=2,
            )

    def _open_file(self, local: str):
        """Return (stat_result, exifread_tags) or raise/warn and return None."""
        try:
            st = _listing._retry_transient(lambda: os.stat(local))
            if st.st_size == 0:
                warnings.warn(
                    f"exif_gbx: skipping empty file {local!r} (0 bytes)",
                    stacklevel=2,
                )
                return None, None
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"exif_gbx: skipping unreadable file {local!r}: {exc}",
                stacklevel=2,
            )
            return None, None

        try:
            import exifread

            # Retry the open() separately from stat() — a transient FUSE
            # FileNotFoundError on /Volumes would otherwise be swallowed by the
            # broad except below, silently dropping files during eventually-consistent
            # UC Volume reads.  Mirrors ds/lidar.py's two-level retry pattern.
            fh = _listing._retry_transient(lambda: open(local, "rb"))  # noqa: WPS515
            with fh:
                return st, exifread.process_file(fh, details=False)
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"exif_gbx: skipping file with unreadable EXIF {local!r}: {exc}",
                stacklevel=2,
            )
            return None, None

    def _build_metadata_row(self, local: str, source: str, tags) -> dict:
        """Parse EXIF tags into a dict matching EXIF_META_SCHEMA (no pixel decode)."""
        # --- camera make / model ---
        camera_make = str(tags["Image Make"]) if "Image Make" in tags else None
        camera_model = str(tags["Image Model"]) if "Image Model" in tags else None

        # --- timestamp ---
        timestamp = None
        for ts_key in ("EXIF DateTimeOriginal", "Image DateTime"):
            if ts_key in tags:
                timestamp = _parse_timestamp(str(tags[ts_key]))
                if timestamp is not None:
                    break

        # --- GPS lat / lon ---
        lat: Optional[float] = None
        if "GPS GPSLatitude" in tags and "GPS GPSLatitudeRef" in tags:
            lat = _dms_to_decimal(
                tags["GPS GPSLatitude"].values, str(tags["GPS GPSLatitudeRef"])
            )
        lon: Optional[float] = None
        if "GPS GPSLongitude" in tags and "GPS GPSLongitudeRef" in tags:
            lon = _dms_to_decimal(
                tags["GPS GPSLongitude"].values, str(tags["GPS GPSLongitudeRef"])
            )
        alt: Optional[float] = _gps_altitude(tags)

        # --- focal length ---
        focal_length_mm: Optional[float] = self.focal_length_mm_override
        if focal_length_mm is None and "EXIF FocalLength" in tags:
            focal_length_mm = _rational_first(tags["EXIF FocalLength"])

        # --- image dimensions ---
        image_width, image_height = _image_dimensions(local, tags)

        # --- geometry: POINT(lon lat) WKB, SRID 4326 ---
        geom_wkb: Optional[bytes] = None
        geom_srid: Optional[int] = None
        if lon is not None and lat is not None:
            geom_wkb = _encode_point_wkb(lon, lat)
            geom_srid = 4326

        return {
            "path": source,
            "camera_make": camera_make,
            "camera_model": camera_model,
            "timestamp": timestamp,
            "latitude": lat,
            "longitude": lon,
            "altitude": alt,
            "focal_length_mm": focal_length_mm,
            "sensor_width_mm": self.sensor_width_mm,
            "image_width": image_width,
            "image_height": image_height,
            "geom_wkb": geom_wkb,
            "geom_srid": geom_srid,
        }

    def _read_metadata(self, file_path: str) -> Iterator["pa.RecordBatch"]:
        """Emit one Arrow RecordBatch row per file (header-only; no pixel decode)."""
        # metadata mode yields 1 row per file — chunkSize has no effect here.
        import pyarrow as pa
        from pyspark.sql.pandas.types import to_arrow_schema

        local = _listing.to_local_path(file_path)
        source = _listing.to_spark_uri(file_path)

        _, tags = self._open_file(local)
        if tags is None:
            return

        row = self._build_metadata_row(local, source, tags)
        yield pa.RecordBatch.from_pylist(
            [row], schema=to_arrow_schema(EXIF_META_SCHEMA)
        )

    def _read_qc(self, file_path: str) -> Iterator["pa.RecordBatch"]:
        """Emit one row per file: metadata columns + sharpness + brightness."""
        import pyarrow as pa
        from pyspark.sql.pandas.types import to_arrow_schema

        from databricks.labs.gbx.ds.file_gbx import materialize_decision
        from databricks.labs.gbx.pyrx.imagery import image_brightness, image_sharpness

        local = _listing.to_local_path(file_path)
        source = _listing.to_spark_uri(file_path)

        st, tags = self._open_file(local)
        if tags is None:
            return

        # Serverless-safe materialize gate: reject files too large for executor RAM.
        # Cap was pre-baked on the driver (self._qc_cap_bytes) to avoid session-less
        # fallback to the classic 256 MiB cap on Serverless workers.
        decision = materialize_decision(
            st.st_size, kind="read", cap_bytes=self._qc_cap_bytes
        )
        if decision != "stream":
            warnings.warn(
                f"exif_gbx qc: skipping {local!r} — file size {st.st_size} bytes "
                "exceeds the Serverless materialize cap; use a classic cluster "
                "for large images",
                stacklevel=2,
            )
            return

        # Read full bytes once for pixel metric computation.
        try:
            fh = _listing._retry_transient(lambda: open(local, "rb"))  # noqa: WPS515
            with fh:
                raw = fh.read()
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"exif_gbx qc: skipping {local!r} — cannot read bytes: {exc}",
                stacklevel=2,
            )
            return

        row = self._build_metadata_row(local, source, tags)
        row["sharpness"] = image_sharpness(raw)
        row["brightness"] = image_brightness(raw)
        yield pa.RecordBatch.from_pylist([row], schema=to_arrow_schema(EXIF_QC_SCHEMA))


# ---------------------------------------------------------------------------
# DataSource
# ---------------------------------------------------------------------------


class ExifGbxDataSource(DataSource):
    """DataSource V2 for ``exif_gbx`` — EXIF/GPS metadata from drone/camera images."""

    @classmethod
    def name(cls) -> str:
        return "exif_gbx"

    def schema(self) -> StructType:
        mode = (self.options.get("mode") or "metadata").lower()
        if mode == "metadata":
            return EXIF_META_SCHEMA
        if mode == "qc":
            return EXIF_QC_SCHEMA
        raise ValueError(
            f"exif_gbx: unknown mode={mode!r}; supported modes are 'metadata' and 'qc'"
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        return ExifGbxReader(self.options)
