"""Spark-free map algebra over one or more rasters via numexpr (safe math eval).

Band 1 of each input raster (in order) is bound to variables A, B, C, …; the
expression is evaluated with numexpr (no arbitrary code execution). Output is a
single-band Float32 GTiff using the first raster's georeference.
"""

import json
import string

import numexpr as ne
import numpy as np
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core._nodata import emit, read_masked

_VARS = string.ascii_uppercase  # A..Z (up to 26 inputs)


# Envelope keys that are gdal_calc CLI plumbing with no numexpr equivalent.
_UNSUPPORTED_KEYS = ("extra_options",)


def _parse_spec(expression: str, n_rasters: int):
    """Normalize a map-algebra spec to ``(calc, bindings)``.

    Both tiers accept the same gdal_calc JSON envelope (``{"calc": "..."}``);
    the light tier extracts ``calc`` and evaluates it with numexpr. A bare
    expression (not starting with ``{``) is passed through unchanged for
    backward compatibility.

    Per-variable raster/band selection mirrors ``gdal_calc``'s ``A_index`` /
    ``A_band`` (and ``B_``, ``C_``, …). ``bindings`` is a dict
    ``{var_letter: (raster_index, band)}`` for every variable the spec pins;
    a variable with neither key defaults to ``(ordinal, 1)`` — variable ``A`` →
    raster 0 band 1, ``B`` → raster 1 band 1, … — so the legacy "band 1 of each
    input in array order" behavior is unchanged.

    This is what lets NDVI read band 4 (NIR) and band 3 (Red) of a SINGLE
    multiband raster — ``{"calc":"(A-B)/(A+B)","A_index":0,"B_index":0,
    "A_band":4,"B_band":3}`` — without decomposing it into separate tiles.

    ``extra_options`` (gdal_calc CLI flags) has no numexpr equivalent and raises
    rather than being silently dropped.
    """
    s = expression.strip()
    if not s.startswith("{"):
        return expression, {}  # bare numexpr expression — default bindings
    try:
        spec = json.loads(s)
    except (json.JSONDecodeError, ValueError) as e:
        raise ValueError(
            f"map-algebra spec looks like JSON but did not parse: {e}"
        ) from e
    if not isinstance(spec, dict):
        raise ValueError(
            "map-algebra JSON spec must be an object, "
            f'e.g. {{"calc": "A * 2"}} (got {type(spec).__name__})'
        )
    calc = spec.get("calc")
    if calc is None:
        raise ValueError(
            'map-algebra JSON spec requires a "calc" expression, '
            'e.g. {"calc": "A * 2"}'
        )
    present_unsupported = [k for k in _UNSUPPORTED_KEYS if k in spec]
    if present_unsupported:
        raise ValueError(
            "map-algebra JSON keys not supported on the lightweight tier: "
            f"{present_unsupported}. `extra_options` is gdal_calc CLI-only "
            "(heavy tier); express the computation in `calc` instead."
        )

    # Collect per-variable (raster_index, band). A variable is referenced if any
    # of {V_index, V_band} appears; unreferenced variables keep their default.
    bindings = {}
    for ordinal, var in enumerate(_VARS):
        idx_key, band_key = f"{var}_index", f"{var}_band"
        has_idx, has_band = idx_key in spec, band_key in spec
        if not (has_idx or has_band):
            continue
        raster_index = spec[idx_key] if has_idx else ordinal
        band = spec[band_key] if has_band else 1
        if not isinstance(raster_index, int) or not isinstance(band, int):
            raise ValueError(
                f"map-algebra {idx_key}/{band_key} must be integers, "
                f"got index={raster_index!r} band={band!r}"
            )
        if not (0 <= raster_index < n_rasters):
            raise ValueError(
                f"map-algebra {idx_key}={raster_index} is out of range: "
                f"{n_rasters} raster(s) provided (valid 0..{n_rasters - 1})"
            )
        bindings[var] = (raster_index, band)
    return str(calc), bindings


def mapalgebra(rasters, expression: str) -> bytes:
    """Apply a math expression across one or more rasters.

    By default band 1 of each raster (in order) binds to A, B, C, …; the
    expression is evaluated with numexpr (safe math-only evaluator — no
    arbitrary code exec). Output is a single-band Float32 GTiff on the first
    raster's georeference.

    Per-variable raster/band selection (gdal_calc ``A_index`` / ``A_band``) lets
    a single multiband raster feed several variables — e.g. NDVI from one file's
    NIR and Red bands. See :func:`_parse_spec`.

    Args:
        rasters:    Sequence of raster bytes (at least one).
        expression: either a bare numexpr math expression
            (e.g. ``"(A - B) / (A + B)"``) or the same gdal_calc JSON envelope
            the heavy/SQL tier takes. The envelope's ``calc`` is evaluated; its
            ``A_index``/``A_band`` (etc.) select the raster and 1-based band each
            variable reads.

    Returns:
        GTiff bytes of the evaluated single-band Float32 raster.
    """
    if not rasters:
        raise ValueError("mapalgebra requires at least one raster")
    calc, bindings = _parse_spec(str(expression), len(rasters))

    # Which variables does this call need? Those explicitly bound, plus the
    # positional defaults A..(n-1) so a bare "A + B" still works.
    needed = set(bindings) | {_VARS[i] for i in range(len(rasters))}
    resolved = {
        var: bindings.get(var, (_VARS.index(var), 1)) for var in needed
    }

    local_dict = {}
    opened = {}  # raster_index -> (MemoryFile, dataset)
    invalid = None
    template = None
    try:
        def _open(raster_index):
            if raster_index not in opened:
                mf = MemoryFile(bytes(rasters[raster_index]))
                opened[raster_index] = (mf, mf.open())
            return opened[raster_index][1]

        # Open the first raster first so it is always the georeference template,
        # regardless of which variables the spec references.
        template = _open(0)
        for var in sorted(resolved, key=_VARS.index):
            raster_index, band = resolved[var]
            data, valid = read_masked(_open(raster_index), band)
            local_dict[var] = data
            invalid = (~valid) if invalid is None else (invalid | ~valid)
        result = ne.evaluate(calc, local_dict=local_dict)
        # numexpr may broadcast a scalar expression (e.g. "A * 2") to an ndarray
        # when A is an ndarray — the result is already an array in that case, but
        # ensure we always have a 2-D array matching the spatial grid. emit reads
        # template.profile and writes synchronously, before finally closes ds.
        result = np.asarray(result, dtype="float64")
        return emit(template, result, -9999.0, invalid, "float32")
    finally:
        for mf, ds in opened.values():
            ds.close()
            mf.close()
