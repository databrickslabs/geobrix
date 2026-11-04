"""Spark-free spectral indices (NumPy band math). Each returns a single-band
Float32 GTiff (NoData -9999.0); invalid/divide-by-zero results become NoData."""

import numexpr as ne
import numpy as np

from databricks.labs.gbx.pyrx.core._nodata import emit, read_masked

_NODATA = -9999.0

# Built-in named-index registry mirroring the heavyweight RST_Index.Registry.
# ``calc`` uses ``{band}`` placeholders; ``bands`` is the ordered list of band
# names the formula requires (each must be wired in the band_map).
_INDEX_REGISTRY = {
    "ndvi": ("({nir}-{red})/({nir}+{red})", ("red", "nir")),
    "gndvi": ("({nir}-{green})/({nir}+{green})", ("green", "nir")),
    "msavi": (
        "(2*{nir}+1-sqrt((2*{nir}+1)**2-8*({nir}-{red})))/2",
        ("red", "nir"),
    ),
    "ndvi_re": ("({nir}-{red_edge})/({nir}+{red_edge})", ("red_edge", "nir")),
    "ndmi": ("({nir}-{swir})/({nir}+{swir})", ("nir", "swir")),
    "ndsi": ("({green}-{swir})/({green}+{swir})", ("green", "swir")),
}


def _normalized_diff(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        return (a - b) / (a + b)


def ndvi(ds, red_band, nir_band) -> bytes:
    r, vr = read_masked(ds, red_band)
    n, vn = read_masked(ds, nir_band)
    return emit(ds, _normalized_diff(n, r), _NODATA, ~vr | ~vn, "float32")


def ndwi(ds, green_idx, nir_idx) -> bytes:
    g, vg = read_masked(ds, green_idx)
    n, vn = read_masked(ds, nir_idx)
    return emit(ds, _normalized_diff(g, n), _NODATA, ~vg | ~vn, "float32")


def nbr(ds, nir_idx, swir_idx) -> bytes:
    n, vn = read_masked(ds, nir_idx)
    s, vs = read_masked(ds, swir_idx)
    return emit(ds, _normalized_diff(n, s), _NODATA, ~vn | ~vs, "float32")


def savi(ds, red_idx, nir_idx, l=0.5) -> bytes:  # noqa: E741
    r, vr = read_masked(ds, red_idx)
    n, vn = read_masked(ds, nir_idx)
    l_ = float(l)
    with np.errstate(divide="ignore", invalid="ignore"):
        arr = (n - r) / (n + r + l_) * (1.0 + l_)
    return emit(ds, arr, _NODATA, ~vr | ~vn, "float32")


def evi(
    ds, red_idx, nir_idx, blue_idx, l=1.0, c1=6.0, c2=7.5, g=2.5  # noqa: E741
) -> bytes:  # noqa: E741
    r, vr = read_masked(ds, red_idx)
    n, vn = read_masked(ds, nir_idx)
    b, vb = read_masked(ds, blue_idx)
    l_, c1, c2, g = float(l), float(c1), float(c2), float(g)
    with np.errstate(divide="ignore", invalid="ignore"):
        arr = g * (n - r) / (n + c1 * r - c2 * b + l_)
    return emit(ds, arr, _NODATA, ~vr | ~vn | ~vb, "float32")


def builtin_formulae() -> list:
    """Sorted names of all built-in formulae (for docs / error messages)."""
    return sorted(_INDEX_REGISTRY.keys())


def formula_bands(key: str) -> list:
    """The names of the bands the built-in formula ``key`` (as :func:`check_index` returns it) reads."""
    return list(_INDEX_REGISTRY[key][1])


def check_index(formula_name, band_map):
    """``(formula key, band map with lower-case names)`` of ``rst_index``; a ``ValueError`` that names the offender
    for no formula, no band map, an unknown formula, or a band the formula needs that the map lacks.
    """
    if not formula_name:
        raise ValueError("rst_index: formula_name required")
    if not band_map:
        raise ValueError("rst_index: band_map required (e.g. {'red': 1, 'nir': 2})")
    key = str(formula_name).lower()
    band_map_lc = {str(k).lower(): int(v) for k, v in dict(band_map).items()}
    if key not in _INDEX_REGISTRY:
        known = ", ".join(builtin_formulae())
        raise ValueError(f"rst_index: unknown formula '{formula_name}'. Known: {known}")
    for b in _INDEX_REGISTRY[key][1]:
        if b not in band_map_lc:
            have = ", ".join(sorted(band_map_lc.keys()))
            raise ValueError(
                f"rst_index: formula '{formula_name}' requires band '{b}' in "
                f"band_map; got keys {have}"
            )
    return key, band_map_lc


def index(ds, formula_name: str, band_map) -> bytes:
    """Generic named-index dispatcher (mirrors heavyweight ``RST_Index``).

    Looks up ``formula_name`` (case-insensitive) in the built-in registry,
    wires each named band to its 1-based index via ``band_map`` (keys are
    matched case-insensitively), evaluates the per-pixel formula with numexpr,
    and returns a single-band Float32 GTiff preserving georef/CRS.

    Args:
        ds:           Open rasterio ``DatasetReader``.
        formula_name: Built-in index name (e.g. ``"ndvi"``, ``"msavi"``).
        band_map:     Mapping of band name -> 1-based band index.

    Returns:
        Single-band Float32 GTiff bytes (NoData ``-9999.0`` for non-finite).
    """
    key, band_map_lc = check_index(formula_name, band_map)
    calc, bands = _INDEX_REGISTRY[key]
    # Assign A, B, C... aliases to the formula's bands in declared order, then
    # substitute placeholders and bind each alias to its 1-based pixel array.
    local_dict = {}
    invalid = None
    expr = calc
    for i, b in enumerate(bands):
        alias = chr(ord("A") + i)
        expr = expr.replace("{" + b + "}", alias)
        data, valid = read_masked(ds, band_map_lc[b])
        local_dict[alias] = data
        invalid = (~valid) if invalid is None else (invalid | ~valid)
    with np.errstate(divide="ignore", invalid="ignore"):
        arr = ne.evaluate(expr, local_dict=local_dict)
    return emit(ds, np.asarray(arr, dtype="float64"), _NODATA, invalid, "float32")
