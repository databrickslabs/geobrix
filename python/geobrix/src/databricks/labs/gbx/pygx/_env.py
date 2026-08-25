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
            "pygx quadbin requires a light-tier extra; missing: "
            + ", ".join(missing)
            + ". Install with: pip install 'geobrix[light_env6]'"
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
            "pygx BNG requires a light-tier extra (shapely). "
            "Install with: pip install 'geobrix[light_env6]'"
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
            "pygx custom gridding requires a light-tier extra (shapely). "
            "Install with: pip install 'geobrix[light_env6]'"
        )
