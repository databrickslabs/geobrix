"""Environment checks for the pygx functions at the Python level."""


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
            "pygx quadbin needs "
            + ", ".join(missing)
            + ", which the env wheel provides. Fix: install the env wheel for this runtime."
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
            "pygx BNG needs shapely, which the env wheel provides. "
            "Fix: install the env wheel for this runtime."
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
            "pygx custom gridding needs shapely, which the env wheel provides. "
            "Fix: install the env wheel for this runtime."
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
            "pygx h3 cellfill needs h3, which the env wheel provides. "
            "Fix: install the env wheel for this runtime."
        )
