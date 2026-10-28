"""Single source of truth for raster creation-options (compression + predictor).

Every light-tier write site routes profile-building through creation_opts so the
materialize story is consistent. Mirrors heavy OperatorOptions.appendOptions.
"""

import os
import warnings

_FLOAT = {"float32", "float64"}
_SMALL_INT = {"uint8", "int8"}

# Grounded by a full ZSTD level sweep across 11 tile types (rasterio 1.5.0/GDAL 3.12,
# Darwin arm64).
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
    nbits=None,
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

    nbits:
        Optional source sample depth (the band's ``NBITS`` metadata), e.g. 15 for
        Sentinel-2 uint16 data. Copying such a band with a horizontal-differencing
        predictor (2) fails in GDAL, which supports it only for 8/16/32/64-bit
        samples, so the samples are then promoted to the dtype's full width
        (``nbits`` = 16 for uint16) -- lossless, since the values already fit.

    Returns
    -------
    dict[str, str] suitable for merging into a rasterio profile.
    """
    opts = _codec_opts(dtype, decoded_bytes, compress, level, predictor, driver)
    if (
        nbits is not None
        and opts.get("predictor") == "2"
        and int(nbits) not in (8, 16, 32, 64)
    ):
        import numpy as np

        opts["nbits"] = str(np.dtype(str(dtype)).itemsize * 8)
    return opts


def _codec_opts(dtype, decoded_bytes, compress, level, predictor, driver) -> dict:
    """Codec / level / predictor keys for creation_opts (see its docstring)."""
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


def drop_ycbcr(profile: dict, *, unless_jpeg: bool = False) -> dict:
    """Copy of *profile* without a YCbCr ``photometric``.

    A JPEG/YCbCr source's profile carries ``photometric="ycbcr"``, which GDAL accepts
    only together with ``COMPRESS=JPEG``. Rasterio hands out decoded RGB samples, so
    for any other codec the key is dropped and GDAL picks its RGB default.

    By default the key is always dropped (the writer is about to apply a non-JPEG
    codec). With ``unless_jpeg=True`` it is kept when the profile's own ``compress``
    is JPEG (case-insensitive), so a writer that honours a caller-chosen codec calls
    this AFTER merging the codec options: a JPEG request keeps its YCbCr encoding,
    any other codec drops it. No other key is touched.
    """
    prof = dict(profile)
    if str(prof.get("photometric", "")).lower() != "ycbcr":
        return prof
    if unless_jpeg and str(prof.get("compress", "")).lower() == "jpeg":
        return prof
    del prof["photometric"]
    return prof


# Keys of a source profile that a write with the COG driver must not inherit: the driver lays the
# file out and picks the photometric itself, and warns once per key it is handed (TILED,
# BLOCKXSIZE, BLOCKYSIZE, PHOTOMETRIC); the codec keys are replaced by ``creation_opts(driver="COG")``.
_COG_OWN_KEYS = (
    "tiled",
    "blockxsize",
    "blockysize",
    "photometric",
    "compress",
    "predictor",
    "zstd_level",
    "zlevel",
    "LEVEL",
)


def cog_profile(profile: dict) -> dict:
    """Copy of the source *profile* for a write with ``driver="COG"``: without the layout and codec keys
    of the source (:data:`_COG_OWN_KEYS`). The caller adds ``driver="COG"``, ``blocksize``,
    ``overview_resampling`` and ``creation_opts(..., driver="COG")``."""
    return {k: v for k, v in profile.items() if k not in _COG_OWN_KEYS}


def match_alpha(dst, alpha_like, all_labels: bool = False) -> None:
    """Label the bands of the new dataset *dst* alpha exactly where *alpha_like* does.

    *alpha_like* is the ``colorinterp`` of the dataset the pixels come from. GDAL gives a
    new GTiff its own labels from the band count and type: band 4 of a 4-band ``uint8``
    raster is alpha, whatever the source was. A non-alpha band 4 (the near-infrared of
    RGB+NIR) then reads as a transparency mask, and a NoData declared on the raster
    shadows it (``NodataShadowWarning``). The converse also happens: the alpha band of a
    16-bit RGBA or a gray+alpha raster is lost. The alpha band is the one the alpha rule
    names (``_nodata.alpha_index``: the LAST band, when labelled alpha); a band labelled
    alpha anywhere else is data and is not carried. A band count that differs from
    *alpha_like* (the caller changed it) is left as GDAL labelled it.

    Nothing else is relabelled, unless ``all_labels``: then every other band takes the
    source's label as well (gray, undefined, red, green, blue), as the chips of a
    tessellation do, so RGB and the data bands of a gray+x+alpha or a 16-bit RGBA raster are
    not left to GDAL's defaults. The rule is ``_nodata.carried_labels``.
    """
    from rasterio.enums import ColorInterp

    from databricks.labs.gbx.pyrx.core._nodata import carried_labels

    labels = list(dst.colorinterp)
    if alpha_like is None or len(alpha_like) != len(labels):
        return
    carried = carried_labels(alpha_like, all_labels)
    want = [
        (
            c
            if c is not None
            else (ColorInterp.undefined if got == ColorInterp.alpha else got)
        )
        for c, got in zip(carried, labels)
    ]
    if want != labels:
        dst.colorinterp = want


def internal_mask_env(mask):
    """The GDAL environment a writer of *mask* (see :func:`write_gtiff`) runs in.

    Without ``GDAL_TIFF_INTERNAL_MASK`` GDAL puts a written mask band in a sidecar file,
    which a ``MemoryFile`` cannot hold on to. A context manager that does nothing when
    there is no mask to write.
    """
    from contextlib import nullcontext

    import rasterio

    return (
        rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True)
        if mask is not None
        else nullcontext()
    )


def write_gtiff(
    profile: dict, data, alpha_like=None, mask=None, all_labels: bool = False
) -> bytes:
    """GTiff bytes of *data* with the canonical creation options for its dtype.

    The single light-tier GTiff writer: every derived raster goes through here so its
    encoding never inherits the source file's codec (a YCbCr photometric, which only
    JPEG can carry, is dropped with it). *profile* is not modified.
    *data* is a ``(bands, rows, cols)`` array matching ``profile['count']``.
    *alpha_like*, the ``colorinterp`` of the dataset *data* derives from, makes the output
    label a band alpha exactly where that dataset does (see :func:`match_alpha`);
    ``all_labels`` also copies the source's other labels band by band.
    *mask*, a ``(rows, cols)`` uint8 array (255 valid, 0 invalid, as ``read_masks`` returns
    it), is written as the raster's own per-dataset mask band, inside the file: the
    source's mask carried over (see ``_nodata.has_own_mask``).
    """
    # Lazy: this module is imported by the Spark data-source writers, which must stay
    # importable where rasterio is absent (the heavy-tier build); numpy likewise above.
    import numpy as np
    from rasterio.io import MemoryFile

    prof = drop_ycbcr(profile)
    prof["driver"] = "GTiff"
    dtype = prof.get("dtype", str(np.asarray(data).dtype))
    decoded = data.nbytes if hasattr(data, "nbytes") else None
    prof.update(creation_opts(dtype, decoded_bytes=decoded, compress="auto"))
    with internal_mask_env(mask):
        with MemoryFile() as mf:
            with mf.open(**prof) as dst:
                match_alpha(dst, alpha_like, all_labels=all_labels)
                dst.write(data)
                if mask is not None:
                    dst.write_mask(mask)
            return mf.read()
