"""MVT property coercion (no pyspark): shared by the Spark-free MVT encoder and the Spark schemas."""

from typing import Any, Dict

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
