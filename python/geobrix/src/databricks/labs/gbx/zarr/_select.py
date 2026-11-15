"""Selection planning: ``sel`` / ``isel`` to index ranges to chunk sub-boxes.

Reads, samples and tiles all start from the same question: which cells of the array does
the caller want, and which chunks hold them? ``resolve`` answers the first half by turning
label-based (``sel``) and position-based (``isel``) selections into per-axis half-open index
ranges (``AxisSel``). ``touched_chunks`` and ``chunk_slices`` answer the second half: the
chunk grid indices a selection touches, and for one chunk the sub-boxes to copy, as a pair
of slices (inside the chunk, inside the assembled output).

Coordinates are plain numpy arrays (``float``, ``int`` or ``datetime64``); this module uses
numpy only. Label slices are inclusive of both labels and follow the coordinate order, so a
descending latitude axis takes ``slice(60, 50)`` and a reversed slice on any ordinary axis is
empty. A global longitude axis is periodic, so a slice may cross its seam (``slice(-10, 10)``
on a 0..360 axis is ``350..360`` then ``0..10``), which yields two ranges.
"""

from __future__ import annotations

import datetime as _dt
import itertools
import math
import re
from dataclasses import dataclass
from functools import cached_property

import numpy as np

_LON_NAMES = frozenset({"lon", "longitude", "x"})
# CF spellings of the longitude unit, lower-cased
_LON_UNITS = frozenset(
    {"degrees_east", "degree_east", "degrees_e", "degree_e", "degreese", "degreee"}
)
_FLOAT_TOL = 1e-6  # fraction of the axis step added to slice bounds on float axes
_PERIOD_TOL = (
    0.01  # fraction of the axis step allowed when testing for a 360 degree span
)
_SNAP_STEP = 0.25  # the seam snap never exceeds this fraction of the axis step
_MAX_PERIODIC_REACH = (
    1e6  # degrees: further than this a 360 degree wrap loses the labels
)
_I64_MIN = (
    int(np.iinfo(np.int64).min) + 1
)  # one above NaT, so negating a bound cannot overflow
_I64_MAX = int(np.iinfo(np.int64).max)
# attoseconds per unit: exact integer arithmetic keeps out-of-range time bounds from overflowing
_UNIT_ATTO = {
    "as": 1,
    "fs": 10**3,
    "ps": 10**6,
    "ns": 10**9,
    "us": 10**12,
    "ms": 10**15,
    "s": 10**18,
    "m": 60 * 10**18,
    "h": 3600 * 10**18,
    "D": 86400 * 10**18,
    "W": 7 * 86400 * 10**18,
}
_ISO_WITH_OFFSET = re.compile(
    r"(?P<base>\d{4}-\d{2}-\d{2}[T ]\d{2}(?::\d{2}(?::\d{2}(?:\.\d+)?)?)?)"
    r"(?P<off>Z|[+-]\d{2}(?::?\d{2})?)"
)


@dataclass(frozen=True)
class AxisSel:
    """The selected cells along one dimension.

    ``ranges`` are half-open index ranges in output order: ascending and non-overlapping,
    except that a slice across the seam of a periodic longitude axis gives two ranges whose
    FIRST range holds the higher indices, e.g. ``((140, 144), (0, 5))`` for ``350..357.5``
    then ``0..10``. ``()`` means nothing is selected. ``size`` is the output length.
    """

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

    Label slices follow the coordinate order, so ``a`` and ``b`` reversed against it select
    nothing (``slice(50, 60)`` on a descending latitude is empty). Open ends use ``None``.

    Periodic longitude: a longitude-like dimension (named ``lon``/``longitude``/``x``, or whose
    ``coord_attrs`` has ``units`` ``degrees_east`` (or the CF spellings ``degree_east``,
    ``degrees_E``, ``degree_E``, ``degreesE``, ``degreeE``), ``standard_name == "longitude"`` or
    ``axis == "X"``) is periodic when its coordinates ascend, start between -360 and 360, and
    cover the full circle: ``span + step >= 360`` and ``span <= 360``. The origin and span bounds
    keep a projected ``x`` (metres, or cells counted from 500000) from wrapping, but the name
    rule stays: a bare ``x`` axis of about 360 units whose origin is in range is treated as a
    longitude. On a periodic axis both slice bounds are normalised into
    ``[axis min, axis min + 360)`` and the slice is the east-going arc from ``a`` to ``b``: one
    range, or two when it crosses the seam (see ``AxisSel``). ``slice(-10, 10)`` on 0..360 and
    ``slice(350, 10)`` on -180..180 are both the labels around 0. An axis stored with a repeated
    end (0..360 inclusive) never selects that label twice. A regional axis (``x`` 0..299, ``lon``
    100..250) never wraps, so a reversed slice there is empty. Scalars on a periodic axis
    are normalised the same way and matched by circular distance. A scalar exactly halfway
    between two labels goes to the lowest index.

    Time labels: an ISO string with a date or a partial time is a period, as in xarray. As a
    slice start it means the beginning of that period and as a slice end the end of it, so
    ``slice("2023-01-01", "2023-01-02")`` covers both whole days, and ``"2023-01"`` and
    ``"2023"`` are a month and a year. A ``numpy.datetime64`` or ``datetime`` is an exact
    instant, and a scalar date string selects the label at its start. Offsets such as ``+05:00``
    are converted to UTC. A bound beyond what the axis dtype can represent is clamped to the
    axis end, so ``slice("2023-01-01T20", "3000-01-01")`` is the tail.

    Numbers: ``nan`` and infinite bounds or labels raise ``ValueError`` (leave a slice end open with
    ``None``). A finite number too large for a float axis's dtype, and a Python int too large for a
    float, is past every label: a slice start there selects nothing, a slice end is open, and a
    scalar is not found; it never matches a label stored at the dtype maximum. Bounds are rounded
    to the axis dtype before they are compared, so ``0.3`` finds the label ``float32(0.3)``.

    Periodic axes add two limits. A bound within a few float rounding errors of the seam (at most a
    quarter of the label step) counts as on it, so ``slice(179.95, -179.95)`` keeps the label at
    -179.95 on a float32 axis. And a bound or scalar further than 1e6 degrees from the axis raises
    ``ValueError`` instead of wrapping, because at that size the wrap arithmetic has lost the
    labels; the exception is a slice that covers 360 degrees or more (``slice(-1e300, 10)``),
    which is the whole axis however far its ends are.

    ``coord_attrs`` maps a dimension to its coordinate array's attributes.
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
    if n == 0:
        raise ValueError(
            f"index {i} is out of range for {dim}, which is empty (size 0). Fix: select from a non-empty dimension."
        )
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


@dataclass(eq=False)
class _Axis:
    """One coordinate prepared once per ``sel`` dimension, so list selections do not redo it per label."""

    dim: str
    coord: np.ndarray  # the coordinate as given
    num: (
        np.ndarray
    )  # float64, or int64 ticks (datetimes and durations in the coordinate's own unit)
    kind: str  # "f" float, "i" integer, "M" datetime, "m" duration
    tick: int  # attoseconds per tick for "M"/"m", else 0
    step: float  # median |difference| between neighbouring labels, in ticks (0 for fewer than two)
    lon_like: bool

    @property
    def tol(self):
        """Slack added to slice bounds: a fraction of the step on float axes, exactly 0 otherwise."""
        return _FLOAT_TOL * self.step if self.kind == "f" else 0

    @property
    def snap(self) -> float:
        """How close to a seam counts as on it: a few float rounding errors over one turn, 0 for integers.

        Capped at a quarter of the label step, so a coarse dtype (float16) or a fine axis (float32 at
        1e-4 degrees) cannot snap a bound onto a neighbouring label.
        """
        if self.kind != "f":
            return 0.0
        noise = 4.0 * float(np.finfo(self.coord.dtype).eps) * 360.0
        return min(noise, _SNAP_STEP * self.step)

    @cached_property
    def direction(self) -> int:
        """1 when the labels never decrease, -1 when they never increase, 0 when neither."""
        d = np.diff(self.num)
        if bool((d >= 0).all()):
            return 1
        if bool((d <= 0).all()):
            return -1
        return 0

    @cached_property
    def period(self) -> tuple[float, bool] | None:
        """``(origin, repeats_origin)`` for a periodic longitude axis, else ``None``.

        Periodic means longitude-like, ascending and covering the full circle: ``span + step >=
        360``. The span cannot exceed 360 and the origin must lie in [-360, 360], which keeps a
        projected ``x`` (in metres, or counted from a large origin) from qualifying.
        ``repeats_origin`` is true when the last label is the first one again (an axis stored
        0..360 inclusive).
        """
        if (
            not self.lon_like
            or self.kind not in "fi"
            or self.num.size < 2
            or self.step <= 0
        ):
            return None
        if self.direction != 1:
            return None
        origin = float(self.num[0])
        if not -360.0 <= origin <= 360.0:
            return None
        span = float(self.num[-1]) - origin
        slack = _PERIOD_TOL * self.step
        if span + self.step < 360.0 - slack or span > 360.0 + slack:
            return None
        return origin, span >= 360.0 - slack


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
    ax = _axis_of(dim, coord, attrs)
    if isinstance(v, slice):
        if v.step is not None:
            raise ValueError(
                f"sel slice for {dim} has a step; label slices take only start and stop. Fix: drop the step."
            )
        return AxisSel(dim, _slice_ranges(ax, v.start, v.stop), False)
    if isinstance(v, (list, tuple, np.ndarray)):
        if isinstance(v, np.ndarray) and v.ndim != 1:
            raise ValueError(
                f"sel list for {dim} must be one-dimensional. Fix: pass a flat list of labels."
            )
        idx = sorted({_label_index(ax, item) for item in v})
        return AxisSel(dim, _runs(idx), False)
    i = _label_index(ax, v)
    return AxisSel(dim, ((i, i + 1),), True)


def _axis_of(dim: str, coord: np.ndarray, attrs: dict | None) -> _Axis:
    kind = coord.dtype.kind
    tick = 0
    if kind in "Mm":
        unit, _ = np.datetime_data(coord.dtype)
        if unit in (
            "Y",
            "M",
        ):  # calendar units have no fixed length: count days instead
            if kind == "m":
                raise ValueError(
                    f"label selection cannot compare {dim} durations in calendar units ({coord.dtype}). Fix: store them in days or a finer unit, or use isel."
                )
            coord_t = coord.astype("datetime64[D]")
        else:
            coord_t = coord
        num = coord_t.astype(np.int64)
        tick = _tick_atto(coord_t.dtype, dim)
    elif kind in "iu":
        kind, num = "i", coord.astype(np.int64, copy=False)
    elif kind == "f":
        num = coord.astype(np.float64, copy=False)
    else:
        raise ValueError(
            f"label selection needs numeric or datetime coordinates; {dim} is {coord.dtype}. Fix: use isel for position-based selection."
        )
    return _Axis(dim, coord, num, kind, tick, _step(num), _is_lon_like(dim, attrs))


def _is_lon_like(dim: str, attrs: dict | None) -> bool:
    if dim.lower() in _LON_NAMES:
        return True
    if not attrs:
        return False
    return (
        str(attrs.get("units", "")).lower() in _LON_UNITS
        or str(attrs.get("standard_name", "")).lower() == "longitude"
        or str(attrs.get("axis", "")).upper() == "X"
    )


def _step(num: np.ndarray) -> float:
    """Typical spacing of an axis: the median absolute difference (0 for fewer than two labels)."""
    if num.size < 2:
        return 0.0
    return float(np.median(np.abs(np.diff(num).astype(np.float64))))


def _clamp(i: int) -> int:
    return max(_I64_MIN, min(_I64_MAX, i))


def _wrap(x: float, origin: float) -> float:
    """``x`` mapped into ``[origin, origin + 360)``."""
    return origin + (x - origin) % 360.0


# ---- time labels


def _tick_atto(dtype: np.dtype, dim: str) -> int:
    unit, count = np.datetime_data(dtype)
    factor = _UNIT_ATTO.get(unit)
    if factor is None:
        raise ValueError(
            f"{dim} has an unsupported time unit '{unit}' ({dtype}). Fix: store the coordinate with a fixed-length unit such as ns, us, s or D."
        )
    return factor * count


def _atto(x) -> int:
    """A ``datetime64``/``timedelta64`` scalar as an exact count of attoseconds (a Python int)."""
    unit, count = np.datetime_data(x.dtype)
    if unit in ("Y", "M"):
        if x.dtype.kind != "M":
            raise ValueError("durations in years or months have no fixed length")
        x = x.astype("datetime64[D]")
        unit, count = "D", 1
    factor = _UNIT_ATTO.get(unit)
    if factor is None:
        raise ValueError(f"unsupported time unit '{unit}'")
    return int(x.astype(np.int64)) * count * factor


def _parse_iso(text: str) -> tuple[np.datetime64, np.datetime64]:
    """An ISO string as ``(start, end)`` of the period its precision names; ``end`` is exclusive.

    ``"2023"`` is a year, ``"2023-01-01"`` a day, ``"2023-01-01T05:00"`` a minute. An offset
    (``Z``, ``+05:00``) is applied here so both ends come back in UTC with no numpy timezone warning.
    """
    s = text.strip()
    shift = None
    m = _ISO_WITH_OFFSET.fullmatch(s)
    if m:
        s, off = m["base"].replace(" ", "T"), m["off"]
        if off != "Z":
            digits = off[1:].replace(":", "")
            hours, mins = int(digits[:2]), int(digits[2:] or 0)
            if hours > 23 or mins > 59:
                raise ValueError(
                    f"cannot parse the UTC offset {off}: it must be under 24 hours"
                )
            minutes = hours * 60 + mins
            shift = np.timedelta64(-minutes if off[0] == "-" else minutes, "m")
    start = np.datetime64(s)
    if np.isnat(start):
        raise ValueError("not a valid timestamp")
    end = start + np.timedelta64(1, np.datetime_data(start.dtype)[0])
    if shift is not None:
        start, end = start - shift, end - shift
    return start, end


def _time_label(ax: _Axis, v) -> tuple[int, int | None]:
    """``(start, end)`` of a time label in attoseconds; ``end`` is ``None`` for an exact instant."""
    try:
        if ax.kind == "m":
            if not isinstance(v, np.timedelta64) or np.isnat(v):
                raise ValueError("not a duration")
            return _atto(v), None
        if isinstance(v, str):
            start, end = _parse_iso(v)
            return _atto(start), _atto(end)
        if isinstance(v, np.datetime64):
            exact = v
        elif isinstance(v, _dt.datetime):
            if v.tzinfo is not None:
                v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
            exact = np.datetime64(v)
        elif isinstance(v, _dt.date):
            exact = np.datetime64(v)
        else:
            raise ValueError("not a time label")
        if np.isnat(exact):
            raise ValueError("not a valid timestamp")
        return _atto(exact), None
    except (TypeError, ValueError, OverflowError) as e:
        hint = (
            "a numpy timedelta64"
            if ax.kind == "m"
            else "an ISO string such as '2023-01-01T05:00' or a numpy datetime64"
        )
        raise ValueError(
            f"cannot use {v!r} as a label for {ax.dim} ({ax.coord.dtype}): {e}. Fix: pass {hint}."
        ) from e


def _time_bound(ax: _Axis, v, role: str) -> int:
    """A slice bound in the axis's own ticks, clamped to the representable range.

    A start rounds up and an exact end rounds down, so only labels inside the bound qualify. A
    date or partial-time string ends at the last tick before its period closes.
    """
    start, end = _time_label(ax, v)
    if role == "start":
        ticks = -((-start) // ax.tick)
    elif end is None:
        ticks = start // ax.tick
    else:
        ticks = -((-end) // ax.tick) - 1
    return _clamp(ticks)


# ---- label lookup


def _number(ax: _Axis, v, cast: bool = True, scalar: bool = False):
    """A numeric label as a number comparable with ``ax.num``.

    ``cast=False`` keeps a float as given (still checked finite), for callers that do arithmetic on it
    first and round to the axis dtype afterwards with ``_as_axis_dtype``. ``scalar`` only shapes the
    error text.
    """
    if (
        isinstance(v, (str, bytes, np.datetime64, np.timedelta64, _dt.date))
        or v is None
    ):
        raise ValueError(
            f"{v!r} is not a numeric label for {ax.dim} ({ax.coord.dtype}). Fix: pass a number."
        )
    try:
        x = int(v) if _is_int(v) else float(v)
    except (TypeError, ValueError, OverflowError) as e:
        raise ValueError(
            f"cannot use {v!r} as a label for {ax.dim} ({ax.coord.dtype}): {e}. Fix: pass a label of the coordinate's type."
        ) from e
    if isinstance(x, float) and not math.isfinite(x):
        fix = (
            "pass a label that is on the axis, or use isel"
            if scalar
            else "pass a finite number, or None to leave a slice end open"
        )
        raise ValueError(
            f"{v!r} is not a finite number, so it cannot select labels on {ax.dim}. Fix: {fix}."
        )
    if isinstance(x, int):
        # a float axis compares floats: an int past the float range is past every label (+-inf),
        # not the int64 maximum, which an axis label can exceed
        x = _int_as_float(x) if ax.kind == "f" else _clamp(x)
    if not cast:
        return x
    return _as_axis_dtype(ax, float(x)) if ax.kind == "f" else x


def _int_as_float(x: int) -> float:
    """``x`` as a float; an int too large for a float becomes +-inf."""
    try:
        return float(x)
    except OverflowError:
        return math.inf if x > 0 else -math.inf


def _as_axis_dtype(ax: _Axis, x):
    """``x`` rounded the way the stored labels are, so an equal label compares equal.

    A float too large for the dtype (it rounds past the largest finite value) becomes +-inf: past every
    label, so a start bound there selects nothing and an end bound is open, and it never equals the label
    stored at the dtype maximum. Integer axes keep ``x`` as it is.
    """
    if ax.kind != "f":
        return x
    with np.errstate(over="ignore"):
        return float(np.asarray(x).astype(ax.coord.dtype))


def _scalar(ax: _Axis, v):
    """A single label (not a slice bound) in ticks, rounded to the nearest tick; may be out of range."""
    if ax.kind in "Mm":
        start, _ = _time_label(ax, v)
        return (2 * start + ax.tick) // (2 * ax.tick)
    return _number(ax, v, scalar=True)


def _nearest(ax: _Axis, v, circular: bool) -> tuple[int, float]:
    """``(index, distance)`` of the label closest to ``v``."""
    num = ax.num
    if num.dtype.kind == "i" and isinstance(v, int) and not circular:
        lo, hi = int(num.min()), int(num.max())
        if (
            max(abs(v - lo), abs(v - hi)) <= _I64_MAX
        ):  # int64 differences cannot overflow: exact
            dist = np.abs(num - np.int64(v))
            i = int(np.argmin(dist))
            return i, float(dist[i])
        approx = np.abs(num.astype(np.float64) - float(v))
        i = int(np.argmin(approx))
        return i, float(abs(int(num[i]) - v))
    dist = np.abs(num.astype(np.float64) - float(v))
    dist[np.isnan(dist)] = np.inf
    i = int(np.argmin(dist))
    if (
        circular and dist[i] != 0
    ):  # an exact label beats a label on the other side of the seam
        dist = np.minimum(dist, 360.0 - dist)
        i = int(np.argmin(dist))
    return i, float(dist[i])


def _label_index(ax: _Axis, value) -> int:
    """Index of the label equal to ``value``, else of the nearest label within half the axis step."""
    v = _scalar(ax, value)
    if ax.kind in "Mm" and not _I64_MIN <= v <= _I64_MAX:
        raise ValueError(
            f"{value!r} not found in {ax.dim}: it is outside the times a {ax.coord.dtype} axis can represent. Fix: pick a label on the axis, or use isel."
        )
    period = ax.period
    if period is not None:
        origin, repeats = period
        top = origin + 360.0
        if not abs(v) <= _MAX_PERIODIC_REACH:  # also inf: a float cast can overflow
            raise ValueError(
                f"{value!r} is too far from {ax.dim} to place on a 360 degree circle. "
                f"Fix: pass a longitude within {_MAX_PERIODIC_REACH:g} degrees of the axis, or use isel."
            )
        if v < origin or v > top or (v == top and not repeats):
            v = _wrap(v, origin)
    nearest = ""
    if ax.num.size:
        i, dist = _nearest(ax, v, period is not None)
        if dist == 0 or dist <= 0.5 * ax.step:
            return i
        nearest = f"; the nearest label is {ax.coord[i]}"
    raise ValueError(
        f"{value!r} not found in {ax.dim}{nearest}. Fix: pick a label on the axis (within half a step), or use isel."
    )


# ---- slices


def _bounded(num: np.ndarray, asc: bool, a, b, tol) -> tuple[tuple[int, int], ...]:
    """Indices from label ``a`` to label ``b`` inclusive, in coordinate order; ``None`` leaves that end open.

    ``tol`` is an int 0 on integer and time axes so the comparisons stay exact int64.
    """
    n = num.size
    if asc:
        start = 0 if a is None else int(np.searchsorted(num, a - tol, side="left"))
        stop = n if b is None else int(np.searchsorted(num, b + tol, side="right"))
    else:
        neg = -num
        start = 0 if a is None else int(np.searchsorted(neg, -a - tol, side="left"))
        stop = n if b is None else int(np.searchsorted(neg, -b + tol, side="right"))
    return ((start, stop),) if start < stop else ()


def _join_arcs(
    east: tuple[tuple[int, int], ...],
    west: tuple[tuple[int, int], ...],
    n: int,
    repeats_origin: bool,
) -> tuple[tuple[int, int], ...]:
    """The two arcs of a seam-crossing slice as output ranges: ``east`` (up to the axis end) then ``west``.

    The last label of an axis stored 0..360 inclusive is the first one again, so ``east`` stops
    before it. The slice tolerance can make the arcs reach the same label; ``west`` is cut back so
    no index appears twice.
    """
    ranges: list[tuple[int, int]] = []
    if east:
        s, e = east[0]
        if repeats_origin:
            e = min(e, n - 1)
        if s < e:
            ranges.append((s, e))
    if west:
        s, e = west[0]
        if ranges:
            e = min(e, ranges[0][0])
        if s < e:
            ranges.append((s, e))
    return tuple(ranges)


def _slice_ranges(ax: _Axis, a, b) -> tuple[tuple[int, int], ...]:
    """Index ranges for the inclusive label slice ``a..b`` along the axis (ascending or descending).

    On a periodic longitude axis (see ``resolve``) a slice that leaves ``[origin, origin + 360]`` or
    is reversed is the east-going arc from ``a`` to ``b``: the labels from ``a`` to the end then
    from the start to ``b``, or one range when it does not cross the seam. An arc of 360 degrees or
    more selects the whole axis in array order. Every other slice follows the coordinate order, so
    a reversed one is empty.
    """
    num = ax.num
    n = num.size
    if n == 0:
        return ()
    if ax.direction == 0:
        raise ValueError(
            f"coordinate {ax.dim} is not monotonic, so a label slice is ambiguous. Fix: use isel, or a list of labels."
        )
    asc = ax.direction > 0
    if ax.kind in "Mm":
        lo = None if a is None else _time_bound(ax, a, "start")
        hi = None if b is None else _time_bound(ax, b, "stop")
    else:
        # as given: a periodic wrap does arithmetic on the bound, and rounding to float32 first would
        # carry that rounding error across the seam; the dtype cast follows the arithmetic
        lo = None if a is None else _number(ax, a, cast=False)
        hi = None if b is None else _number(ax, b, cast=False)
    period = ax.period
    if period is not None and lo is not None and hi is not None:
        origin, repeats = period
        top = origin + 360.0
        snap = (
            ax.snap
        )  # the bounds were not rounded to the axis dtype, so seams carry that much noise
        if not origin - snap <= lo <= hi <= top + snap:
            if lo <= hi and hi - lo >= 360.0 - snap:
                return ((0, n),)
            for bound, given in ((lo, a), (hi, b)):
                if not abs(bound) <= _MAX_PERIODIC_REACH:
                    raise ValueError(
                        f"{given!r} is too far from {ax.dim} to place on a 360 degree circle. "
                        f"Fix: pass a longitude within {_MAX_PERIODIC_REACH:g} degrees of the axis, "
                        "or None to leave a slice end open."
                    )

            def norm(v):
                r = _wrap(v, origin)
                if (
                    top - r <= snap
                ):  # just short of the seam is just before the origin, not a turn later
                    r -= 360.0
                return _as_axis_dtype(ax, r)

            a2, b2 = norm(lo), norm(hi)
            if (
                a2 <= b2 or a2 - b2 <= snap
            ):  # equal within noise: a zero-width arc, one label
                return _bounded(num, True, min(a2, b2), max(a2, b2), ax.tol)
            return _join_arcs(
                _bounded(num, True, a2, None, ax.tol),
                _bounded(num, True, None, b2, ax.tol),
                n,
                repeats,
            )
    if ax.kind not in "Mm":
        lo = None if lo is None else _as_axis_dtype(ax, lo)
        hi = None if hi is None else _as_axis_dtype(ax, hi)
    return _bounded(num, asc, lo, hi, ax.tol)
