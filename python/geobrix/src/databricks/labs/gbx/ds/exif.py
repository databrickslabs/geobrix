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


def _exif_arrow_schema(spark_schema: StructType) -> "pa.Schema":
    """Build a pyarrow schema from a Spark ``StructType`` without a session timezone.

    ``pyspark.sql.pandas.types.to_arrow_schema`` funnels ``TimestampType`` through
    ``to_arrow_type``, which asserts a non-null session timezone — unset on
    Spark-Connect / Serverless DataSource workers, so it raises ``AssertionError``
    there. This reader plans session-free, so we map ``TimestampType`` to a fixed
    UTC arrow type ourselves (matching ``timestamp_utc=True`` semantics; naive EXIF
    datetimes are treated as UTC wall-clock) and defer every other field to
    ``to_arrow_type`` (which needs no timezone).
    """
    import pyarrow as pa
    from pyspark.sql.pandas.types import to_arrow_type

    fields = []
    for f in spark_schema.fields:
        if isinstance(f.dataType, TimestampType):
            arrow_type = pa.timestamp("us", tz="UTC")
        else:
            arrow_type = to_arrow_type(f.dataType)
        fields.append(pa.field(f.name, arrow_type, nullable=f.nullable))
    return pa.schema(fields)


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

        local = _listing.to_local_path(file_path)
        source = _listing.to_spark_uri(file_path)

        _, tags = self._open_file(local)
        if tags is None:
            return

        row = self._build_metadata_row(local, source, tags)
        yield pa.RecordBatch.from_pylist(
            [row], schema=_exif_arrow_schema(EXIF_META_SCHEMA)
        )

    def _read_qc(self, file_path: str) -> Iterator["pa.RecordBatch"]:
        """Emit one row per file: metadata columns + sharpness + brightness."""
        import pyarrow as pa

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
        yield pa.RecordBatch.from_pylist(
            [row], schema=_exif_arrow_schema(EXIF_QC_SCHEMA)
        )


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


# ---------------------------------------------------------------------------
# Sensor width database
# ---------------------------------------------------------------------------

# camera_model (normalized: str.strip().lower()) → sensor width (mm).
# Values derived from OpenDroneMap's sensor_data.json; a manual sensorWidthMm
# override always wins, and unknown models fall through to needs_supply.
_CAMERA_SENSOR_WIDTH_MM: dict[str, float] = {
    "hero5 black": 6.17,
    "hero6 black": 6.17,
    "hero7 black": 6.17,
    "hero8 black": 6.17,
    "fc220": 6.17,  # DJI Mavic Pro
    "fc330": 6.17,  # DJI Phantom 4
    "fc6310": 13.2,  # DJI Phantom 4 Pro (1")
    "fc7203": 6.17,  # DJI Mavic Mini
    "l1d-20c": 13.2,  # DJI Mavic 2 Pro (1")
}


def _lookup_sensor_width_mm(camera_model) -> Optional[float]:
    """Sensor width (mm) for *camera_model* via the starter DB, or None.

    Lookup is case- and whitespace-insensitive.
    """
    if not camera_model:
        return None
    return _CAMERA_SENSOR_WIDTH_MM.get(str(camera_model).strip().lower())


# ---------------------------------------------------------------------------
# Selection helper
# ---------------------------------------------------------------------------


def _select_subset(seq, mode: str, limit: int) -> list:
    """Choose a subset of *seq* for EXIF profiling.

    * ``"all"``    → all items.
    * ``"limit"``  → first *limit* items (``seq[:limit]``).
    * ``"sample"`` → evenly spaced: ``seq[::max(1, len//limit)][:limit]``.

    Raises ``ValueError`` for an unknown mode.
    """
    lst = list(seq)
    if mode == "all":
        return lst
    if mode == "limit":
        return lst[:limit]
    if mode == "sample":
        step = max(1, len(lst) // limit)
        return lst[::step][:limit]
    raise ValueError(f"mode must be 'all', 'limit', or 'sample'; got {mode!r}")


# ---------------------------------------------------------------------------
# Pure EXIF aggregator
# ---------------------------------------------------------------------------

_INTRINSIC_FIELDS = (
    "focal_length_mm",
    "camera_make",
    "camera_model",
    "image_width",
    "image_height",
)
_PER_IMAGE_FIELDS = (
    "latitude",
    "longitude",
    "altitude",
    "timestamp",
)  # expected present, vary per image
_PROFILE_FIELDS = _INTRINSIC_FIELDS + _PER_IMAGE_FIELDS


def _aggregate_exif(
    records: list[dict], *, sensor_db: bool = True
) -> tuple[dict, dict]:
    """Aggregate a list of per-file EXIF records into common-opts and a report.

    Parameters
    ----------
    records:
        List of dicts with keys ``camera_make``, ``camera_model``,
        ``focal_length_mm``, ``image_width``, ``image_height``.  Any value
        may be ``None``.
    sensor_db:
        When True (default), attempt to resolve sensor width from
        ``_CAMERA_SENSOR_WIDTH_MM`` for a uniform camera model.

    Returns
    -------
    opts:
        Splat-ready ``exif_gbx`` reader overrides dict.  Keys:
        ``"focalLengthMm"`` (str) if focal is uniform across every record;
        ``"sensorWidthMm"`` (str) if sensor width was resolved from the DB.
        The reader ``mode`` key is intentionally excluded.
    report:
        ``{"fields": {…}, "sensor_width_mm": {…}, "needs_supply": […]}``.

    Field summary shape (one entry per field in ``_PROFILE_FIELDS``)::

        {
            "present_frac": float,   # fraction of records with a non-None value
            "distinct":     list,    # sorted list of distinct non-None values
            "is_uniform":   bool,    # exactly one distinct value AND present in every record
            "value":        Any,     # that single value, or None
        }

    Sensor-width summary shape::

        {
            "resolved":      bool,
            "value":         float | None,
            "source":        "camera_db" | "none",
            "camera_model":  str | None,
        }
    """
    n = len(records)

    # --- per-field statistics ---
    fields: dict[str, dict] = {}
    for fname in _PROFILE_FIELDS:
        non_null = [r[fname] for r in records if r.get(fname) is not None]
        present_frac = len(non_null) / n if n > 0 else 0.0
        try:
            distinct = sorted(set(non_null))
        except TypeError:
            distinct = sorted(set(non_null), key=str)
        is_uniform = len(distinct) == 1 and present_frac == 1.0
        fields[fname] = {
            "present_frac": present_frac,
            "distinct": distinct,
            "is_uniform": is_uniform,
            "value": distinct[0] if is_uniform else None,
        }

    # --- opts ---
    opts: dict[str, str] = {}
    focal_field = fields["focal_length_mm"]
    if focal_field["is_uniform"]:
        opts["focalLengthMm"] = str(focal_field["value"])

    # --- sensor width resolution ---
    model_field = fields["camera_model"]
    sw_value: Optional[float] = None
    sw_source = "none"
    sw_camera_model: Optional[str] = (
        model_field["value"] if model_field["is_uniform"] else None
    )

    if model_field["is_uniform"] and sensor_db:
        sw_value = _lookup_sensor_width_mm(sw_camera_model)
        if sw_value is not None:
            sw_source = "camera_db"
            opts["sensorWidthMm"] = str(sw_value)

    sensor_resolved = sw_source == "camera_db"
    sensor_width_info: dict = {
        "resolved": sensor_resolved,
        "value": sw_value,
        "source": sw_source,
        "camera_model": sw_camera_model,
    }

    # --- needs_supply ---
    needs_supply: list[str] = []
    if not sensor_resolved:
        if not model_field["is_uniform"]:
            needs_supply.append(
                "sensor_width_mm: camera_model is not uniform; "
                "cannot auto-resolve sensor width — supply sensorWidthMm manually"
            )
        else:
            needs_supply.append("sensor_width_mm")

    if len(focal_field["distinct"]) > 1:
        needs_supply.append(
            "focalLengthMm: not uniform across collection; focalLengthMm override not set"
        )

    # --- classification ---
    common = {f: fields[f]["value"] for f in _PROFILE_FIELDS if fields[f]["is_uniform"]}
    variable: dict[str, dict] = {
        f: {
            "n_distinct": len(fields[f]["distinct"]),
            "examples": fields[f]["distinct"][:6],
        }
        for f in _PROFILE_FIELDS
        if fields[f]["present_frac"] == 1.0 and len(fields[f]["distinct"]) > 1
    }
    missing_cls: dict[str, float] = {
        f: fields[f]["present_frac"]
        for f in _PROFILE_FIELDS
        if fields[f]["present_frac"] < 1.0
    }
    if not sensor_resolved:
        missing_cls["sensor_width_mm"] = 0.0
    classification = {"common": common, "variable": variable, "missing": missing_cls}

    # --- suggestions ---
    suggestions: list[str] = []

    # sensor
    if not sensor_resolved:
        if not model_field["is_uniform"]:
            _examples = model_field["distinct"][:6]
            suggestions.append(
                f"multiple camera models {_examples} — supply per-camera sensorWidthMm; "
                "sensor width cannot be auto-resolved"
            )
        else:
            suggestions.append(
                "sensor_width_mm absent from EXIF — supply sensorWidthMm= "
                "(needed for focal-in-pixels; e.g. GoPro HERO5 ≈ 6.17 mm)"
            )

    # focal
    if focal_field["present_frac"] < 1.0:
        _frac = focal_field["present_frac"]
        suggestions.append(
            f"focal_length_mm absent/partial ({_frac:.0%}) — supply focalLengthMm="
        )
    elif "focal_length_mm" in variable:
        _examples = variable["focal_length_mm"]["examples"]
        suggestions.append(
            f"focal_length_mm varies {_examples} — "
            "pin focalLengthMm only if the lens is truly fixed"
        )

    # georef: lat/lon/altitude with partial presence
    for _gf in ("latitude", "longitude", "altitude"):
        _frac = fields[_gf]["present_frac"]
        if _frac < 1.0:
            if _gf == "altitude":
                suggestions.append(
                    f"altitude present on {_frac:.0%} of images — missing altitude degrades "
                    "Sim(3) scale/height; supply altitudes or use a DEM"
                )
            else:
                suggestions.append(
                    f"{_gf} present on {_frac:.0%} — those images cannot be georeferenced"
                )

    # mixed hardware
    for _hf in ("camera_model", "image_width", "image_height"):
        if _hf in variable:
            _examples = variable[_hf]["examples"]
            suggestions.append(
                f"{_hf} is not uniform {_examples} — mixed cameras/resolutions; "
                "consider splitting the collection or per-camera intrinsics"
            )

    report = {
        "fields": fields,
        "sensor_width_mm": sensor_width_info,
        "needs_supply": needs_supply,
        "classification": classification,
        "suggestions": suggestions,
    }
    return opts, report


# ---------------------------------------------------------------------------
# Per-file EXIF parse helper (module-level so tests can monkeypatch it)
# ---------------------------------------------------------------------------


def _parse_exif_record(fp: str) -> Optional[dict]:
    """Open *fp* header-only and return a profiling record dict, or None.

    Extracted keys: ``camera_make``, ``camera_model``, ``focal_length_mm``,
    ``image_width``, ``image_height``, ``latitude``, ``longitude``,
    ``altitude``, ``timestamp``.  Unreadable files are skipped with a
    ``warnings.warn`` rather than raising so the caller can keep going.
    """
    try:
        import exifread

        with open(fp, "rb") as fh:
            tags = exifread.process_file(fh, details=False)
    except Exception as exc:  # noqa: BLE001
        warnings.warn(
            f"build_exif_common_opts: skipping unreadable file {fp!r}: {exc}",
            stacklevel=3,
        )
        return None

    camera_make = str(tags["Image Make"]) if "Image Make" in tags else None
    camera_model = str(tags["Image Model"]) if "Image Model" in tags else None
    focal_length_mm: Optional[float] = None
    if "EXIF FocalLength" in tags:
        focal_length_mm = _rational_first(tags["EXIF FocalLength"])
    image_width, image_height = _image_dimensions(fp, tags)

    # --- GPS lat / lon (mirror _build_metadata_row) ---
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

    # --- timestamp ---
    timestamp = None
    for ts_key in ("EXIF DateTimeOriginal", "Image DateTime"):
        if ts_key in tags:
            timestamp = _parse_timestamp(str(tags[ts_key]))
            if timestamp is not None:
                break

    return {
        "camera_make": camera_make,
        "camera_model": camera_model,
        "focal_length_mm": focal_length_mm,
        "image_width": image_width,
        "image_height": image_height,
        "latitude": lat,
        "longitude": lon,
        "altitude": alt,
        "timestamp": timestamp,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def build_exif_common_opts(
    path: str,
    *,
    mode: str = "sample",
    limit: int = 24,
    sensor_db: bool = True,
) -> tuple[dict, dict]:
    """Profile a collection's EXIF and build the common (uniform) ``exif_gbx`` overrides.

    Lists images under *path*, inspects a subset chosen by *mode*/*limit*
    (``"sample"`` = evenly spaced, most trustworthy for "truly common";
    ``"limit"`` = first N; ``"all"``), header-only (no pixel decode), and
    returns ``(opts, report)``.

    Splat *opts* directly into the reader::

        spark.read.format("exif_gbx").options(mode="qc", **opts).load(path)

    Serverless-safe: driver-side pure Python; no Spark session or JVM.
    Requires ``exifread``; no new package dependencies.

    Parameters
    ----------
    path:
        Directory of camera images (local FUSE path or ``/Volumes/…``).
    mode:
        Subset selection mode — ``"sample"`` (default), ``"limit"``, or ``"all"``.
    limit:
        Number of files to inspect for ``"sample"`` and ``"limit"`` modes.
    sensor_db:
        When True (default), attempt to resolve ``sensorWidthMm`` from the
        built-in camera model → sensor-width database.

    Returns
    -------
    opts:
        Dict of uniform override values, splat-ready for ``.options(**opts)``.
    report:
        ``{"n_files", "n_inspected", "mode", "limit", "fields": {…},
        "sensor_width_mm": {…}, "needs_supply": […]}``.
        Do NOT print this inside library code; display it from the caller.
    """
    # --- enumerate (normalize dbfs:/file:/Volumes scheme like the reader does) ---
    local = _listing.to_local_path(path)
    try:
        all_files = list_local_files(local, extensions=_EXIF_EXTS)
    except FileNotFoundError:
        all_files = []

    subset = _select_subset(all_files, mode, limit)

    # --- parse each file header ---
    records: list[dict] = []
    for fp in subset:
        rec = _parse_exif_record(fp)
        if rec is not None:
            records.append(rec)

    # --- aggregate ---
    opts, sub_report = _aggregate_exif(records, sensor_db=sensor_db)
    report = {
        "n_files": len(all_files),
        "n_inspected": len(subset),
        "mode": mode,
        "limit": limit,
        **sub_report,
    }
    return opts, report


def eval_exif_opts(
    path: str,
    *,
    mode: str = "sample",
    limit: int = 24,
    sensor_db: bool = True,
) -> str:
    """Human-readable evaluation of a collection's EXIF.

    Sections: ``COMMON`` / ``PRESENT-BUT-VARIABLE`` / ``MISSING`` /
    ``SUGGESTIONS`` / ``OPTS`` (the splat-ready dict), plus the
    ``n_inspected of n_files`` header.

    Thin formatter over :func:`build_exif_common_opts` — no extra scan.
    Returns the formatted string; does **not** print (library-clean).

    Parameters
    ----------
    path:
        Directory of camera images (same as :func:`build_exif_common_opts`).
    mode:
        Subset selection mode — ``"sample"`` (default), ``"limit"``, ``"all"``.
    limit:
        Number of files to inspect for ``"sample"`` and ``"limit"`` modes.
    sensor_db:
        When True (default), attempt to resolve ``sensorWidthMm`` from the DB.
    """
    opts, report = build_exif_common_opts(
        path, mode=mode, limit=limit, sensor_db=sensor_db
    )

    n_inspected = report["n_inspected"]
    n_files = report["n_files"]
    classification = report.get("classification", {})
    suggestions = report.get("suggestions", [])

    lines: list[str] = [f"{n_inspected} of {n_files} files inspected", ""]

    # COMMON
    lines.append("COMMON")
    common = classification.get("common", {})
    if common:
        for k, v in common.items():
            lines.append(f"  {k}: {v}")
    else:
        lines.append("  (none)")
    lines.append("")

    # PRESENT-BUT-VARIABLE
    lines.append("PRESENT-BUT-VARIABLE")
    variable = classification.get("variable", {})
    if variable:
        for k, info in variable.items():
            lines.append(
                f"  {k}: {info['n_distinct']} distinct values, e.g. {info['examples']}"
            )
    else:
        lines.append("  (none)")
    lines.append("")

    # MISSING
    lines.append("MISSING")
    missing = classification.get("missing", {})
    if missing:
        for k, frac in missing.items():
            lines.append(f"  {k}: {frac:.0%} present")
    else:
        lines.append("  (none)")
    lines.append("")

    # SUGGESTIONS
    lines.append("SUGGESTIONS")
    if suggestions:
        for s in suggestions:
            lines.append(f"  - {s}")
    else:
        lines.append("  (none)")
    lines.append("")

    # OPTS
    lines.append("OPTS")
    lines.append(f"  {opts}")

    return "\n".join(lines)
