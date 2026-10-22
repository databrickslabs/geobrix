"""Selection planning: ``sel`` / ``isel`` to index ranges to chunk sub-boxes.

Reads, samples and tiles all start from the same question: which cells of the array does
the caller want, and which chunks hold them? ``resolve`` answers the first half by turning
label-based (``sel``) and position-based (``isel``) selections into per-axis half-open index
ranges (``AxisSel``). ``touched_chunks`` and ``chunk_slices`` answer the second half: the
chunk grid indices a selection touches, and for one chunk the sub-boxes to copy, as a pair
of slices (inside the chunk, inside the assembled output).

Coordinates are plain numpy arrays (``float``, ``int`` or ``datetime64``); this module uses
numpy only. Label slices are inclusive of both labels and follow the coordinate order, so a
descending latitude axis takes ``slice(60, 50)``. Longitudes on a 0..360 axis accept
negative bounds and east-going arcs (``slice(-10, 10)`` is ``350..360`` then ``0..10``), which
yields two ranges. Ranges are ascending and non-overlapping except for that wrapped pair,
whose first range holds the higher indices.
"""

from __future__ import annotations

import datetime as _dt
import itertools
from dataclasses import dataclass

import numpy as np

_LON_NAMES = frozenset({"lon", "longitude", "x"})
_FLOAT_TOL = 1e-6  # fraction of the axis step added to slice bounds on float axes


@dataclass(frozen=True)
class AxisSel:
    """The selected cells along one dimension."""

    dim: str
    ranges: tuple[tuple[int, int], ...]  # half-open index ranges; () = empty
    squeeze: bool  # True when selected by a scalar

    @property
    def size(self) -> int:
        return sum(e - s for s, e in self.ranges)


def resolve(
    dims: tuple[str, ...],
    shape: tuple[int, ...],
    coords: dict[str, np.ndarray],
    sel: dict | None,
    isel: dict | None,
    *,
    coord_attrs: dict[str, dict] | None = None,
) -> list[AxisSel]:
    """Resolve ``sel`` (labels) and ``isel`` (positions) into one ``AxisSel`` per dimension.

    Unselected dimensions cover the whole axis. ``isel`` values are an int (squeezes the
    axis), a ``slice`` with step ``None`` or ``1``, or a list of ints. ``sel`` values are a
    scalar (exact label, else the nearest label within half the axis step; squeezes), an
    inclusive ``slice(a, b)``, or a list of labels. Lists become sorted runs, so the output
    follows array order, not request order. A selection that matches nothing gives
    ``ranges == ()``; a scalar label that is not on the axis raises ``ValueError``.

    ``coord_attrs`` maps a dimension to its coordinate array's attributes; a dimension whose
    ``units`` is ``degrees_east`` is treated as a longitude, like ``lon``/``longitude``/``x``.
    """
    sel = dict(sel or {})
    isel = dict(isel or {})
    if len(dims) != len(shape):
        raise ValueError(
            f"dims {dims} and shape {shape} differ in length. Fix: pass one size per dimension."
        )
    unknown = [d for d in (*sel, *isel) if d not in dims]
    if unknown:
        raise ValueError(
            f"unknown dimension(s) {unknown}; the array has {list(dims)}. Fix: select only existing dimensions."
        )
    both = [d for d in sel if d in isel]
    if both:
        raise ValueError(
            f"dimension(s) {both} are in both sel and isel. Fix: select each dimension one way."
        )
    axes = []
    for dim, n in zip(dims, shape):
        if dim in isel:
            axes.append(_isel_axis(dim, n, isel[dim]))
        elif dim in sel:
            axes.append(
                _sel_axis(
                    dim,
                    n,
                    (coords or {}).get(dim),
                    sel[dim],
                    (coord_attrs or {}).get(dim),
                )
            )
        else:
            axes.append(AxisSel(dim, ((0, n),) if n > 0 else (), False))
    return axes


def touched_chunks(
    axes: list[AxisSel], chunks: tuple[int, ...]
) -> list[tuple[int, ...]]:
    """Chunk grid indices (sorted) that hold any selected cell; ``[]`` when nothing is selected."""
    if len(axes) != len(chunks):
        raise ValueError(
            f"{len(axes)} axes but {len(chunks)} chunk sizes. Fix: pass the array's chunk shape."
        )
    per_axis = []
    for ax, c in zip(axes, chunks):
        idx: set[int] = set()
        for s, e in ax.ranges:
            if e > s:
                idx.update(range(s // c, (e - 1) // c + 1))
        if not idx:
            return []
        per_axis.append(sorted(idx))
    return [tuple(t) for t in itertools.product(*per_axis)]


def chunk_slices(
    axes: list[AxisSel],
    chunk: tuple[int, ...],
    chunks: tuple[int, ...],
    shape: tuple[int, ...],
) -> list[tuple[tuple[slice, ...], tuple[slice, ...]]]:
    """Sub-boxes of one chunk to copy: ``(slices within the chunk, slices within the output)``.

    A range is intersected with the chunk's extent ``[c * chunks[i], min((c + 1) * chunks[i],
    shape[i]))``; one pair is emitted per combination of intersecting ranges across the axes.
    The output position of range ``k`` starts at the summed sizes of ranges ``0..k-1`` so the
    output keeps label order. Squeezed dimensions give a width-1 slice; the caller drops the
    axis when it assembles the result.
    """
    per_axis = []
    for ax, ci, c, n in zip(axes, chunk, chunks, shape):
        c0 = ci * c
        c1 = min(c0 + c, n)
        options = []
        offset = 0
        for s, e in ax.ranges:
            lo, hi = max(s, c0), min(e, c1)
            if lo < hi:
                options.append(
                    (slice(lo - c0, hi - c0), slice(offset + lo - s, offset + hi - s))
                )
            offset += e - s
        if not options:
            return []
        per_axis.append(options)
    return [
        (tuple(i for i, _ in combo), tuple(o for _, o in combo))
        for combo in itertools.product(*per_axis)
    ]


# ---------------------------------------------------------------- isel


def _is_int(v) -> bool:
    return isinstance(v, (int, np.integer)) and not isinstance(v, bool)


def _norm_index(dim: str, n: int, i: int) -> int:
    if i < -n or i >= n:
        raise ValueError(
            f"index {i} is out of range for {dim} (size {n}). Fix: use an index in [-{n}, {n - 1}]."
        )
    return i + n if i < 0 else i


def _runs(sorted_unique_ints) -> tuple[tuple[int, int], ...]:
    """Collapse ascending distinct ints into half-open runs: ``[1, 2, 3, 10]`` -> ``((1, 4), (10, 11))``."""
    runs: list[tuple[int, int]] = []
    start = prev = None
    for i in sorted_unique_ints:
        if start is None:
            start = prev = i
        elif i == prev + 1:
            prev = i
        else:
            runs.append((start, prev + 1))
            start = prev = i
    if start is not None:
        runs.append((start, prev + 1))
    return tuple(runs)


def _isel_axis(dim: str, n: int, v) -> AxisSel:
    if isinstance(v, slice):
        if v.step not in (None, 1):
            raise ValueError(
                f"isel slice for {dim} has step {v.step}; only step 1 is supported. Fix: drop the step or pass a list of indices."
            )
        try:
            start, stop, _ = v.indices(n)
        except TypeError as e:
            raise ValueError(
                f"isel slice bounds for {dim} must be integers: {v!r}. Fix: use integer positions."
            ) from e
        return AxisSel(dim, ((start, stop),) if start < stop else (), False)
    if _is_int(v):
        i = _norm_index(dim, n, int(v))
        return AxisSel(dim, ((i, i + 1),), True)
    if isinstance(v, (list, tuple, np.ndarray)):
        arr = np.asarray(v)
        if arr.ndim != 1:
            raise ValueError(
                f"isel list for {dim} must be one-dimensional. Fix: pass a flat list of indices."
            )
        if arr.size == 0:
            return AxisSel(dim, (), False)
        if arr.dtype.kind not in "iu":
            raise ValueError(
                f"isel list for {dim} must hold integers, got {arr.dtype}. Fix: use integer positions, or sel for labels."
            )
        idx = sorted({_norm_index(dim, n, int(i)) for i in arr})
        return AxisSel(dim, _runs(idx), False)
    raise ValueError(
        f"isel value for {dim} must be an int, a slice or a list of ints, got {type(v).__name__}. Fix: use one of those."
    )


# ---------------------------------------------------------------- sel


def _sel_axis(dim: str, n: int, coord, v, attrs: dict | None) -> AxisSel:
    if coord is None:
        raise ValueError(
            f"{dim} has no coordinate values to select labels from. Fix: use isel for position-based selection."
        )
    coord = np.asarray(coord)
    if coord.ndim != 1 or coord.shape[0] != n:
        raise ValueError(
            f"coordinate {dim} has shape {coord.shape} but the dimension has size {n}. Fix: pass a 1-D coordinate of matching length."
        )
    lon_like = _is_lon_like(dim, attrs)
    if isinstance(v, slice):
        if v.step is not None:
            raise ValueError(
                f"sel slice for {dim} has a step; label slices take only start and stop. Fix: drop the step."
            )
        return AxisSel(
            dim, _slice_ranges(coord, v.start, v.stop, lon_like, dim=dim), False
        )
    wrap = lon_like and _spans_0_360(_numeric(coord, dim))
    if isinstance(v, (list, tuple, np.ndarray)):
        if isinstance(v, np.ndarray) and v.ndim != 1:
            raise ValueError(
                f"sel list for {dim} must be one-dimensional. Fix: pass a flat list of labels."
            )
        idx = sorted(
            {_label_index(coord, _wrap_lon(item) if wrap else item, dim) for item in v}
        )
        return AxisSel(dim, _runs(idx), False)
    i = _label_index(coord, _wrap_lon(v) if wrap else v, dim)
    return AxisSel(dim, ((i, i + 1),), True)


def _is_lon_like(dim: str, attrs: dict | None) -> bool:
    if dim.lower() in _LON_NAMES:
        return True
    return bool(attrs) and str(attrs.get("units", "")).lower() == "degrees_east"


def _spans_0_360(num: np.ndarray) -> bool:
    """True for a global-looking longitude axis stored as [0, 360)."""
    return (
        num.dtype.kind in "fi"
        and num.size > 1
        and float(num.min()) >= 0.0
        and 180.0 < float(num.max()) < 360.0
    )


def _wrap_lon(value):
    """Map a numeric scalar outside [0, 360) onto it (``-75`` -> ``285``); anything else is unchanged."""
    if (
        isinstance(value, (int, float, np.integer, np.floating))
        and not isinstance(value, bool)
        and (value < 0 or value >= 360)
    ):
        return float(value) % 360.0
    return value


def _numeric(coord: np.ndarray, dim: str) -> np.ndarray:
    """The coordinate as ints (datetimes and timedeltas as ns) or float64, for exact comparisons."""
    kind = coord.dtype.kind
    if kind == "M":
        return coord.astype("datetime64[ns]").astype(np.int64)
    if kind == "m":
        return coord.astype("timedelta64[ns]").astype(np.int64)
    if kind in "iu":
        return coord.astype(np.int64, copy=False)
    if kind == "f":
        return coord.astype(np.float64, copy=False)
    raise ValueError(
        f"label selection needs numeric or datetime coordinates; {dim} is {coord.dtype}. Fix: use isel for position-based selection."
    )


def _to_datetime64(v, dim: str) -> np.datetime64:
    if isinstance(v, np.datetime64):
        out = v
    elif isinstance(v, str):
        s = v.strip()
        out = np.datetime64(s[:-1] if s.endswith("Z") else s)
    elif isinstance(v, (_dt.datetime, _dt.date)):
        out = np.datetime64(v)
    else:
        raise ValueError(
            f"{v!r} is not a time label for {dim}. Fix: pass an ISO string such as '2023-01-01T05:00' or a numpy datetime64."
        )
    if np.isnat(out):
        raise ValueError(
            f"{v!r} is not a valid time label for {dim}. Fix: pass a real timestamp."
        )
    return out


def _to_num(coord: np.ndarray, v, dim: str):
    """A label as a number comparable with ``_numeric(coord)``."""
    kind = coord.dtype.kind
    try:
        if kind == "M":
            return int(_to_datetime64(v, dim).astype("datetime64[ns]").astype(np.int64))
        if kind == "m":
            if not isinstance(v, np.timedelta64) or np.isnat(v):
                raise ValueError(
                    f"{v!r} is not a duration label for {dim}. Fix: pass a numpy timedelta64."
                )
            return int(v.astype("timedelta64[ns]").astype(np.int64))
        if (
            isinstance(v, (str, bytes, np.datetime64, np.timedelta64, _dt.date))
            or v is None
        ):
            raise ValueError(
                f"{v!r} is not a numeric label for {dim} ({coord.dtype}). Fix: pass a number."
            )
        if kind == "f":
            return float(
                np.asarray(v).astype(coord.dtype)
            )  # same rounding as the stored labels
        return v if _is_int(v) else float(v)
    except (TypeError, ValueError, OverflowError) as e:
        if isinstance(e, ValueError) and "Fix:" in str(e):
            raise
        raise ValueError(
            f"cannot use {v!r} as a label for {dim} ({coord.dtype}): {e}. Fix: pass a label of the coordinate's type."
        ) from e


def _step(num: np.ndarray) -> float:
    """Typical spacing of an axis: the median absolute difference (0 for fewer than two labels)."""
    if num.size < 2:
        return 0.0
    return float(np.median(np.abs(np.diff(num).astype(np.float64))))


def _label_index(coord: np.ndarray, value, dim: str) -> int:
    """Index of the label equal to ``value``, else of the nearest label within half the axis step."""
    num = _numeric(coord, dim)
    v = _to_num(coord, value, dim)
    if num.size:
        dist = np.abs(num - v).astype(np.float64)
        dist[np.isnan(dist)] = np.inf
        i = int(np.argmin(dist))
        if dist[i] == 0 or dist[i] <= 0.5 * _step(num):
            return i
        nearest = f"; the nearest label is {coord[i]}"
    else:
        nearest = ""
    raise ValueError(
        f"{value!r} not found in {dim}{nearest}. Fix: pick a label on the axis (within half a step), or use isel."
    )


def _direction(num: np.ndarray, dim: str) -> bool:
    """True when ``num`` is non-decreasing, False when non-increasing; ``ValueError`` otherwise."""
    d = np.diff(num)
    if bool((d >= 0).all()):
        return True
    if bool((d <= 0).all()):
        return False
    raise ValueError(
        f"coordinate {dim} is not monotonic, so a label slice is ambiguous. Fix: use isel, or a list of labels."
    )


def _bounded(
    num: np.ndarray, asc: bool, a, b, tol: float
) -> tuple[tuple[int, int], ...]:
    """Indices from label ``a`` to label ``b`` inclusive, in coordinate order; ``None`` leaves that end open."""
    n = num.size
    if asc:
        start = 0 if a is None else int(np.searchsorted(num, a - tol, side="left"))
        stop = n if b is None else int(np.searchsorted(num, b + tol, side="right"))
    else:
        neg = -num
        start = 0 if a is None else int(np.searchsorted(neg, -a - tol, side="left"))
        stop = n if b is None else int(np.searchsorted(neg, -b + tol, side="right"))
    return ((start, stop),) if start < stop else ()


def _slice_ranges(
    coord: np.ndarray, a, b, lon_like: bool, dim: str = ""
) -> tuple[tuple[int, int], ...]:
    """Index ranges for the inclusive label slice ``a..b`` along ``coord`` (ascending or descending).

    On a longitude axis stored as [0, 360), a request with a negative or >= 360 bound, or with
    ``a > b``, is an east-going arc from ``a`` to ``b`` and becomes up to two ranges: the labels
    from ``a`` to the end, then the labels from the start to ``b``. An arc of 360 degrees or more
    selects the whole axis.
    """
    num = _numeric(coord, dim)
    if num.size == 0:
        return ()
    asc = _direction(num, dim)
    tol = _FLOAT_TOL * _step(num) if coord.dtype.kind == "f" else 0.0
    lo = None if a is None else _to_num(coord, a, dim)
    hi = None if b is None else _to_num(coord, b, dim)
    if lon_like and asc and lo is not None and hi is not None and _spans_0_360(num):
        if lo > hi or lo < 0 or hi < 0 or lo >= 360 or hi >= 360:
            if lo <= hi and hi - lo >= 360:
                return ((0, num.size),)
            lo, hi = lo % 360.0, hi % 360.0
            if lo > hi:
                return _bounded(num, True, lo, None, tol) + _bounded(
                    num, True, None, hi, tol
                )
    return _bounded(num, asc, lo, hi, tol)
