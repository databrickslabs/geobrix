"""Environment checks for the pyvx light tier."""


def assert_mvt_available() -> None:
    """Raise a clear ImportError if the MVT light deps are missing."""
    missing = []
    try:
        import mapbox_vector_tile  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("mapbox-vector-tile")
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("shapely")
    if missing:
        raise ImportError(
            "pyvx requires the [light] extra; missing: "
            + ", ".join(missing)
            + ". Install with: pip install 'geobrix[light]'"
        )


def assert_legacy_available() -> None:
    """Raise a clear ImportError if the legacy decode light deps are missing.

    Legacy decode (st_legacyaswkb) needs only shapely — not scipy. Keep this
    separate from assert_tin_available so an MVT/legacy-only user without scipy
    can still register the legacy UDF.
    """
    missing = []
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("shapely")
    if missing:
        raise ImportError(
            "pyvx legacy requires the [light] extra; missing: "
            + ", ".join(missing)
            + ". Install with: pip install 'geobrix[light]'"
        )


def assert_tin_available() -> None:
    """Raise a clear ImportError if the TIN/legacy light deps are missing."""
    missing = []
    try:
        import scipy  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("scipy")
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("shapely")
    if missing:
        raise ImportError(
            "pyvx TIN/legacy requires the [light] extra; missing: "
            + ", ".join(missing)
            + ". Install with: pip install 'geobrix[light]'"
        )


def assert_crs_available() -> None:
    """Raise a clear ImportError if the CRS light deps are missing."""
    missing = []
    try:
        import pyproj  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("pyproj")
    try:
        import rasterio  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("rasterio")
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("shapely")
    if missing:
        raise ImportError(
            "pyvx CRS requires the [light] extra; missing: "
            + ", ".join(missing)
            + ". Install with: pip install 'geobrix[light]'"
        )
