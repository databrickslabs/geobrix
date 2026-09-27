"""Single source of truth for raster creation-options (compression + predictor).

Every light-tier write site routes profile-building through creation_opts so the
materialize story is consistent. Mirrors heavy OperatorOptions.appendOptions.
"""

import os
import warnings

_FLOAT = {"float32", "float64"}
_SMALL_INT = {"uint8", "int8"}

# Grounded by .superpowers/sdd/2026-09-10-grid-fidelity-stage3/followup-compression-matrix-report.md
# (full ZSTD level sweep across 11 tile types, rasterio 1.5.0/GDAL 3.12, Darwin arm64).
#
# Key finding: L6 is the universal knee.
#   - L9 and L12 add nothing over L6 on output size for any tile type tested.
#   - L16 frequently REGRESSES (e.g. +150 KB on 4-band 1024² float32).
#   - Read time is level-independent: decompress throughput depends on output bytes,
#     not on the level used to produce them.
#   - Peak RAM tracks output size (not level), so higher levels waste CPU with no
#     memory, size, or read benefit on GeoBrix tiles.
#   - Prior ladder (L16/L12/L9) was wrong at both ends; L6 is the correct ceiling.
#
# Serverless confirmation (2026-09-11, env v6, oauth-fe): level selection verified for
# 256/512/1024 MiB and >1 GiB via auto_level assertions (all PASS). Runtime: 256 MiB ok
# (L3, 2.33x, lossless); 512 MiB OOMs the ~1 GiB task. A follow-up write-buffer spike
# corrected the peak model: peak ≈ decoded array + the COMPRESSED OUTPUT bytes (+ ~5 MiB
# GDAL) — NOT a GDAL strip buffer sized to the decode. So the OOM point tracks
# COMPRESSIBILITY: near-incompressible data (output ≈ decoded) OOMs ~256 MiB decoded (worst
# case tested); typical geospatial data (4–20x) pushes the effective ceiling to ~400–700 MiB
# decoded. No write-path lever (tiling / windowed / tempfile / level) reduces this peak — the
# compressed output IS the deliverable. So the >128 MiB rungs (L3/L1) are DEFENSIVE on
# Serverless: the connect-aware stream cap (64 MiB on Serverless — materialize_decision — well
# below the OOM ceiling) means a stream-read tile never reaches them; they apply only to
# operation-produced large tiles or classic compute (256 MiB read cap), where L3 stays correct
# (lower working-set than L6). materialize_decision remains the upstream memory guard.
#
# (decoded_bytes_ceiling, zstd_level) ascending; last entry ceiling = float('inf').
_AUTO_LADDER = [
    (128 * 1024**2, 6),  # <=128 MiB -> L6 (knee level)
    (1 * 1024**3, 3),  # <=1 GiB   -> L3 (balanced for large tiles)
    (float("inf"), 1),  # >1 GiB    -> L1 (OOM guard)
]
_AUTO_DEFAULT_LEVEL = 6  # used when decoded size is unknown (L6 knee; was 9)
DEFAULT_COMPRESS = "auto"

# ZSTD valid level range (rfc 8878 / zstd 1.5.x).
_ZSTD_MIN_LEVEL = 1
_ZSTD_MAX_LEVEL = 22


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

    The ``GBX_ZSTD_LEVEL`` environment variable provides an opt-in override for
    the auto-compression path only (``compress='auto'`` call sites).  It is read
    per-call so executors in Serverless / distributed workers pick it up at runtime.

    Knob semantics (``GBX_ZSTD_LEVEL``):
    - Unset or ``default`` (case-insensitive): use the size-adaptive ladder (default).
    - ``fast``: fixed L1 — highest throughput, ~5–13 % larger output vs L6.
    - ``max``: fixed L9 — heavy-parity ceiling; same output size as L6 on all tile
      types tested; use only when heavy-tier parity is a hard requirement.
    - An integer string in [1, 22] (e.g. ``"3"``): that fixed level, size-independent.
    - Any other value: emits a ``UserWarning`` and falls back to the ladder.

    The knob does **not** affect ``compress='zstd', level=N`` or any other explicit
    codec call site — those already carry an explicit level.

    None → _AUTO_DEFAULT_LEVEL (balanced, used when size is unknown).
    Otherwise returns the first ladder entry whose ceiling >= decoded_bytes,
    guaranteeing monotonic non-increasing levels as size grows.
    """
    # Read env var per-call (propagates to Serverless/executor workers via os.environ).
    # HARD REQ (pyrx-serverless-no-spark-config): never use spark.conf; use os.environ.
    raw = os.environ.get("GBX_ZSTD_LEVEL")
    if raw is not None:
        key = raw.strip().lower()
        if key in ("", "default"):
            pass  # fall through to size-adaptive ladder
        elif key == "fast":
            return 1
        elif key == "max":
            return 9
        else:
            try:
                lvl = int(key)
                if _ZSTD_MIN_LEVEL <= lvl <= _ZSTD_MAX_LEVEL:
                    return lvl
                warnings.warn(
                    f"GBX_ZSTD_LEVEL={raw!r} is out of range "
                    f"[{_ZSTD_MIN_LEVEL}, {_ZSTD_MAX_LEVEL}]; "
                    "falling back to size-adaptive ladder.",
                    UserWarning,
                    stacklevel=2,
                )
            except ValueError:
                warnings.warn(
                    f"GBX_ZSTD_LEVEL={raw!r} is not a recognised value "
                    "(expected 'default', 'fast', 'max', or an integer 1–22); "
                    "falling back to size-adaptive ladder.",
                    UserWarning,
                    stacklevel=2,
                )
    # Size-adaptive ladder (default path, or fallback after bad knob value).
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
    driver="GTiff",
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
        Level is chosen by ``auto_level``; the ``GBX_ZSTD_LEVEL`` env var
        provides an opt-in override (values: ``default``/unset, ``fast``→L1,
        ``max``→L9, or an integer string 1–22). Default behaviour preserves the
        memory-efficient size-adaptive ladder. The knob is an opt-in only.
        'zstd' — explicit ZSTD; level defaults to _AUTO_DEFAULT_LEVEL.
        'deflate' — DEFLATE; level (zlevel) defaults to 6.
        'lzw' — LZW; predictor derived from dtype.
        'none'/'raw' — no compression.
        Any other string — passed through as-is (no predictor assumption).
    level:
        Optional level override. Ignored (with UserWarning) when compress='auto'.
        For zstd: becomes zstd_level (GTiff) or LEVEL (COG).
        For deflate: becomes zlevel (GTiff) or LEVEL (COG).
    predictor:
        Optional predictor override. Ignored (with UserWarning) when
        compress='auto' (auto always derives predictor from dtype).
    driver:
        "GTiff" (default) or "COG". Controls compression option names:
        - GTiff ZSTD uses "zstd_level", DEFLATE uses "zlevel"
        - COG ZSTD/DEFLATE both use "LEVEL"

    Returns
    -------
    dict[str, str] suitable for merging into a rasterio profile.
    """
    dtype = str(dtype)
    pred = predictor if predictor is not None else predictor_for(dtype)
    is_cog = str(driver).upper() == "COG"

    if compress == "auto":
        if level is not None or predictor is not None:
            warnings.warn(
                "creation_opts: compress='auto' ignores explicit level/predictor "
                "(auto derives them from tile size + dtype).",
                UserWarning,
                stacklevel=2,
            )
        auto_lev = str(auto_level(decoded_bytes))
        auto_pred = predictor_for(dtype)
        opts: dict = {"compress": "zstd"}
        # predictor=1 (no prediction) is the COG driver default; omitting it is
        # behaviour-equivalent and avoids a CPLE_NotSupported warning on GDAL
        # versions that reject numeric predictor values for the COG driver.
        # GTiff always receives the numeric predictor (including "1").
        if not (is_cog and auto_pred == 1):
            opts["predictor"] = str(auto_pred)
        # COG driver uses LEVEL; GTiff uses zstd_level
        opts["LEVEL" if is_cog else "zstd_level"] = auto_lev
        return opts

    c = str(compress).lower()
    if c in ("none", "raw"):
        return {}  # no compression keys

    if c == "zstd":
        opts = {"compress": "zstd"}
        if not (is_cog and pred == 1):
            opts["predictor"] = str(pred)
        # COG driver uses LEVEL; GTiff uses zstd_level
        opts["LEVEL" if is_cog else "zstd_level"] = str(
            level if level is not None else _AUTO_DEFAULT_LEVEL
        )
        return opts
    if c == "deflate":
        opts = {"compress": "deflate"}
        if not (is_cog and pred == 1):
            opts["predictor"] = str(pred)
        # COG driver uses LEVEL; GTiff uses zlevel
        opts["LEVEL" if is_cog else "zlevel"] = str(level if level is not None else 6)
        return opts
    if c == "lzw":
        opts = {"compress": "lzw"}
        if not (is_cog and pred == 1):
            opts["predictor"] = str(pred)
        return opts
    # GDAL-supported name passed through; no predictor assumption for unknown codecs
    return {"compress": c}
