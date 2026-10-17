"""Geometry + attribute marshalling for pyvx MVT encoding (Spark-free)."""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from typing import Any, Dict

from pyspark.sql.types import BinaryType, IntegerType, StructField, StructType

# Output tile struct, identical to the heavy generator's row shape.
TILE_SCHEMA = StructType(
    [
        StructField("z", IntegerType(), False),
        StructField("x", IntegerType(), False),
        StructField("y", IntegerType(), False),
        StructField("mvt_bytes", BinaryType(), True),
    ]
)

# TIN triangulate output: one 2D-WKB triangle (Polygon) per row.
TRIANGLE_SCHEMA = StructType([StructField("triangle", BinaryType(), False)])

# Elevation/interpolation output: one WKB elevation point per row.
ELEVATION_SCHEMA = StructType([StructField("elevation_point", BinaryType(), False)])

# Python native types that map to a native MVT Value; everything else -> str().
_NATIVE = (bool, int, float, str)


def to_native_props(attrs: Any) -> Dict[str, Any]:
    """Coerce an attrs mapping/Row into a dict of MVT-native property values.

    bool/int/float/str pass through (mapbox-vector-tile picks the matching MVT
    Value field); any other type (bytes, datetime, list, dict) is str()-ified;
    None values are dropped (no field emitted), matching the heavy writer.
    """
    if attrs is None:
        return {}
    items = attrs.asDict().items() if hasattr(attrs, "asDict") else attrs.items()
    out: Dict[str, Any] = {}
    for k, v in items:
        if v is None:
            continue
        out[str(k)] = v if isinstance(v, _NATIVE) else str(v)
    return out
