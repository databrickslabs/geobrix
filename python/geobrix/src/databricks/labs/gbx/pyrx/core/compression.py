"""Single source of truth for raster creation-options (compression + predictor).

Every light-tier write site routes profile-building through creation_opts so the
materialize story is consistent. Mirrors heavy OperatorOptions.appendOptions.
"""

import warnings

_FLOAT = {"float32", "float64"}
_SMALL_INT = {"uint8", "int8"}

# Grounded by bench/compression_sweep.py (see .superpowers/sdd/compression-benchmark.md).
# Breakpoints 1–128 MiB data-confirmed (Darwin arm64, rasterio 1.5.0/GDAL 3.12).
# Breakpoints 256 MiB–inf extrapolated from 128 MiB trend + guidance; NEEDS
# Serverless confirmation at 256/512/1024 MiB.
# (decoded_bytes_ceiling, zstd_level) ascending; last entry ceiling = float('inf').
_AUTO_LADDER = [
    (4 * 1024**2, 16),  # <=4 MiB   -> L16
    (128 * 1024**2, 12),  # <=128 MiB -> L12
    (1 * 1024**3, 9),  # <=1 GiB   -> L9
    (float("inf"), 6),  # >1 GiB    -> L6 (OOM guard)
]
_AUTO_DEFAULT_LEVEL = 9  # used when decoded size is unknown (balanced default)
DEFAULT_COMPRESS = "auto"


def predictor_for(dtype: str) -> int:
    """Return the TIFF predictor tag for the given numpy dtype string.

    3 for float32/float64 (floating-point horizontal differencing),
    1 (no predictor) for uint8/int8 (byte data; predictor adds no benefit),
    2 for all other integer types (int16/uint16/int32/uint32).
    """
    dtype = str(dtype)
    if dtype in _FLOAT:
        return 3
    if dtype in _SMALL_INT:
        return 1
    return 2  # int16/uint16/int32/uint32


def auto_level(decoded_bytes) -> int:
    """Return the ZSTD level appropriate for the given decoded payload size.

    None -> _AUTO_DEFAULT_LEVEL (balanced, used when size is unknown).
    Otherwise returns the first ladder entry whose ceiling >= decoded_bytes,
    guaranteeing monotonic non-increasing levels as size grows.
    """
    if decoded_bytes is None:
        return _AUTO_DEFAULT_LEVEL
    for ceiling, level in _AUTO_LADDER:
        if decoded_bytes <= ceiling:
            return level
    return _AUTO_LADDER[-1][1]  # unreachable: last ceiling is inf


def creation_opts(
    dtype,
    decoded_bytes=None,
    compress="auto",
    level=None,
    predictor=None,
) -> dict:
    """Return rasterio creation-options for a raster write.

    Parameters
    ----------
    dtype:
        Numpy dtype string (e.g. "float32", "int16", "uint8").
    decoded_bytes:
        Estimated uncompressed tile size in bytes. Used only when compress='auto'
        to select a size-adaptive ZSTD level. Pass None to use the balanced
        default (_AUTO_DEFAULT_LEVEL).
    compress:
        'auto' — size-adaptive ZSTD + dtype-derived predictor (recommended).
        'zstd' — explicit ZSTD; level defaults to _AUTO_DEFAULT_LEVEL.
        'deflate' — DEFLATE; level (zlevel) defaults to 6.
        'lzw' — LZW; predictor derived from dtype.
        'none'/'raw' — no compression.
        Any other string — passed through as-is (no predictor assumption).
    level:
        Optional level override. Ignored (with UserWarning) when compress='auto'.
        For zstd: becomes zstd_level. For deflate: becomes zlevel.
    predictor:
        Optional predictor override. Ignored (with UserWarning) when
        compress='auto' (auto always derives predictor from dtype).

    Returns
    -------
    dict[str, str] suitable for merging into a rasterio profile.
    """
    dtype = str(dtype)
    pred = predictor if predictor is not None else predictor_for(dtype)

    if compress == "auto":
        if level is not None or predictor is not None:
            warnings.warn(
                "creation_opts: compress='auto' ignores explicit level/predictor "
                "(auto derives them from tile size + dtype).",
                UserWarning,
                stacklevel=2,
            )
        return {
            "compress": "zstd",
            "zstd_level": str(auto_level(decoded_bytes)),
            "predictor": str(predictor_for(dtype)),
        }

    c = str(compress).lower()
    if c in ("none", "raw"):
        return {}  # no compression keys

    if c == "zstd":
        return {
            "compress": "zstd",
            "zstd_level": str(level if level is not None else _AUTO_DEFAULT_LEVEL),
            "predictor": str(pred),
        }
    if c == "deflate":
        return {
            "compress": "deflate",
            "zlevel": str(level if level is not None else 6),
            "predictor": str(pred),
        }
    if c == "lzw":
        return {
            "compress": "lzw",
            "predictor": str(pred),
        }
    # GDAL-supported name passed through; no predictor assumption for unknown codecs
    return {"compress": c}
