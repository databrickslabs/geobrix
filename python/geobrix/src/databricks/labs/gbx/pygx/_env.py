"""Environment checks for the pygx light tier."""


def assert_quadbin_available() -> None:
    """Raise a clear ImportError if the quadbin light deps are missing."""
    missing = []
    try:
        import quadbin  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("quadbin")
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("shapely")
    if missing:
        raise ImportError(
            "pygx quadbin requires the lightweight tier; missing: "
            + ", ".join(missing)
            + ". Install the env wheel for the runtime (on Databricks) or pip install geobrix (off Databricks)"
        )


def assert_bng_available() -> None:
    """Raise a clear ImportError if shapely (the only pygx BNG dep) is missing.

    BNG is a pure-Python port of BNG.scala; it needs only shapely (geometry +
    WKB I/O), not the quadbin PyPI library.
    """
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        raise ImportError(
            "pygx BNG requires the lightweight tier (shapely). "
            "Install the env wheel for the runtime (on Databricks) or pip install geobrix (off Databricks)"
        )


def assert_custom_available() -> None:
    """Raise a clear ImportError if shapely (the only pygx custom dep) is missing.

    Custom gridding is a pure-Python port of CustomGridSystem.scala; it needs only
    shapely (geometry + WKB/WKT I/O), no quadbin/BNG PyPI library.
    """
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        raise ImportError(
            "pygx custom gridding requires the lightweight tier (shapely). "
            "Install the env wheel for the runtime (on Databricks) or pip install geobrix (off Databricks)"
        )


def assert_h3_available() -> None:
    """Raise a clear ImportError if the h3 library is missing.

    The pygx h3 cellfill aggregator needs only the h3 library (cell-ID
    conversion and grid_ring neighbour lookup); no native GDAL or shapely.
    """
    try:
        import h3  # noqa: F401
    except Exception:  # noqa: BLE001
        raise ImportError(
            "pygx h3 cellfill requires the h3 library. "
            "Install the env wheel for the runtime (on Databricks) or pip install geobrix (off Databricks)"
        )
