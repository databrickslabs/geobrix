"""Shared Spark-free helpers for pyrx core ops."""

from rasterio.enums import Resampling

# The resampling words of gdalwarp -r that the functions accept, as the heavy tier's RST_ResampleHelper allows them
# (plus ``nearest``, GDAL's ``near``), each with the rasterio member that does the same work.
_GDAL_WORDS = {
    "near": "nearest",
    "nearest": "nearest",
    "bilinear": "bilinear",
    "cubic": "cubic",
    "cubicspline": "cubic_spline",
    "lanczos": "lanczos",
    "average": "average",
    "mode": "mode",
    "max": "max",
    "min": "min",
    "med": "med",
    "q1": "q1",
    "q3": "q3",
}


def resampling_enum(name: str) -> Resampling:
    """Map a gdalwarp-style resampling name to rasterio's Resampling enum.

    The word is case-insensitive and is one of the words the heavy tier accepts: ``near`` (or ``nearest``),
    ``bilinear``, ``cubic``, ``cubicspline``, ``lanczos``, ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``
    and ``q3``. Any other word, rasterio's ``gauss`` or ``sum`` among them, raises a ``ValueError`` that names it and
    lists the ones that work.
    """
    word = str(name).strip().lower()
    key = _GDAL_WORDS.get(word)
    if key is None:
        raise ValueError(
            f"unknown resampling algorithm {name!r}. "
            f"Fix: use one of {', '.join(sorted(_GDAL_WORDS))}"
        )
    return Resampling[key]
