"""Shared Spark-free helpers for pyrx core ops."""

from rasterio.enums import Resampling

# GDAL's own spellings of a resampling word that rasterio spells another way.
_GDAL_WORDS = {"near": "nearest", "cubicspline": "cubic_spline"}


def resampling_enum(name: str) -> Resampling:
    """Map a gdalwarp-style resampling name to rasterio's Resampling enum.

    The word is case-insensitive and may be GDAL's own spelling (``near``, ``cubicspline``). A word that names
    no resampling raises a ``ValueError`` that names it and lists the ones that do.
    """
    word = str(name).strip().lower()
    key = _GDAL_WORDS.get(word, word)
    try:
        return Resampling[key]
    except KeyError:
        raise ValueError(
            f"unknown resampling algorithm {name!r}. "
            f"Fix: use one of {', '.join(sorted(m.name for m in Resampling))}"
        ) from None
