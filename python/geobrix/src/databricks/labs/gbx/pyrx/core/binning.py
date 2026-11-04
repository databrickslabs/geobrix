"""Spark-free point->cell binning (the DSM engine).

Bins (x, y, z) arrays into a ``width_px x height_px`` grid over the extent
``[xmin, xmax] x [ymin, ymax]`` and reduces z values per cell by a statistic.
Empty cells (no points) are filled with NoData (-9999.0). Returns single-band
Float32 GTiff bytes.

This module is intentionally Spark-free: no pyspark import. The
SQL functions wrap ``bin_points`` in UDFs (``gbx_rst_binpoints`` / ``_agg``).
"""

from __future__ import annotations

import numpy as np
from rasterio.transform import from_bounds

from databricks.labs.gbx.core.crs import resolve_crs
from databricks.labs.gbx.pyrx.core.compression import write_gtiff

_NODATA: float = -9999.0


def _cell_indices(
    x, y, xmin: float, ymin: float, xmax: float, ymax: float, w: int, h: int
):
    """Map coordinate arrays to integer (row, col) cell indices.

    Raster convention: row 0 is the TOP (max y), so the row index flips y.

    Half-open interval: a point at exactly x==xmax (or y==ymin, the south edge:
    row 0 is the top) computes col==w (or row==h) and is DROPPED — not clamped
    into the last cell. This
    matches the brief's reference code and prevents cross-tile double-counting
    when DSMs are tiled on shared boundaries.

    Returns
    -------
    row : int64 ndarray (only in-bounds points)
    col : int64 ndarray (only in-bounds points)
    inb : bool ndarray  (same length as input x/y)
    """
    xa = np.asarray(x, dtype="float64")
    ya = np.asarray(y, dtype="float64")

    # Fractional position then multiply by grid dimensions.
    # Flip y: raster row 0 = top (max y). A NaN or infinite coordinate casts to a value far outside the grid.
    with np.errstate(invalid="ignore", over="ignore"):
        col = np.floor((xa - xmin) / (xmax - xmin) * w).astype("int64")
        row = np.floor((ymax - ya) / (ymax - ymin) * h).astype("int64")

    # Strict half-open interval: col in [0, w), row in [0, h).
    # A point exactly on xmax yields col==w and is excluded.
    # A point exactly on ymin (the south edge) yields row==h and is excluded.
    # A NaN or infinite coordinate is in no pixel. The explicit test matters: casting NaN to int64 is platform
    # dependent (x86 gives the minimum, arm64 gives 0, which is a real column).
    inb = (
        np.isfinite(xa)
        & np.isfinite(ya)
        & (col >= 0)
        & (col < w)
        & (row >= 0)
        & (row < h)
    )
    return row[inb], col[inb], inb


def pixel_key(x, y, xmin, ymin, xmax, ymax, w, h):
    """The ``(col, row)`` pixel of one point, or None when it is in no pixel (outside, on the east or south edge, or a
    NULL or NaN coordinate). The lightweight form of ``gbx_rst_binpoints_partial``: the same rule as
    :func:`_cell_indices`, which :func:`bin_points` bins by."""
    if x is None or y is None:
        return None
    row, col, inb = _cell_indices(
        [x], [y], float(xmin), float(ymin), float(xmax), float(ymax), int(w), int(h)
    )
    return (int(col[0]), int(row[0])) if len(col) else None


def check_lengths(x, y, z) -> None:
    """The three coordinate arrays are parallel: ValueError naming the lengths when they are not."""
    if not len(x) == len(y) == len(z):
        raise ValueError(
            "rst_binpoints: x_array, y_array and z_array must have the same length "
            f"(got {len(x)}, {len(y)}, {len(z)})"
        )


_STATISTICS = ("max", "min", "count", "mean", "median")


def _statistic_refusal(statistic) -> str:
    return (
        f"unknown statistic {statistic!r}; expected one of: "
        "max, min, mean, median, count, percentile:<p>"
    )


def _percentile_of(statistic) -> float:
    """The ``p`` of ``percentile:<p>``: a number from 0 to 100, else a ValueError."""
    text = str(statistic).split(":", 1)[1]
    try:
        p = float(text)
    except ValueError:
        p = float("nan")
    if not 0.0 <= p <= 100.0:
        raise ValueError(
            f"percentile statistic {statistic!r} needs a number from 0 to 100 after 'percentile:'"
        )
    return p


def check_statistic(statistic) -> None:
    """ValueError when ``statistic`` is not one :func:`bin_points` knows, or is a ``percentile:<p>`` whose ``p`` is not
    a number from 0 to 100."""
    stat = str(statistic).lower()
    if stat.startswith("percentile:"):
        _percentile_of(stat)
    elif stat not in _STATISTICS:
        raise ValueError(_statistic_refusal(statistic))


def working_bytes(width_px: int, height_px: int, n_points: int) -> int:
    """An upper estimate of the memory :func:`bin_points` holds at its peak: per output pixel the accumulators and
    the NaN-to-NoData copies (64 bytes), per point the coordinate, index and sort arrays (96 bytes).
    """
    return int(width_px) * int(height_px) * 64 + int(n_points) * 96


def bin_points(
    x,
    y,
    z,
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    width_px: int,
    height_px: int,
    srid: int,
    statistic: str = "max",
) -> bytes:
    """Bin (x, y, z) point clouds into a raster cell statistic.

    Parameters
    ----------
    x, y, z:
        Coordinate and value arrays (list or ndarray). All same length.
    xmin, ymin, xmax, ymax:
        Spatial extent of the output raster (in the CRS of ``srid``).
    width_px, height_px:
        Output raster dimensions in pixels.
    srid:
        EPSG code for the output CRS.
    statistic:
        One of ``"max"`` (default), ``"min"``, ``"mean"``, ``"median"``,
        ``"count"``, or ``"percentile:<p>"`` (e.g. ``"percentile:90"``).

    Returns
    -------
    bytes
        Single-band Float32 GTiff. Empty cells carry NoData ``-9999.0``.
    """
    w, h = int(width_px), int(height_px)
    z = np.asarray(z, dtype="float64")

    row, col, inb = _cell_indices(x, y, xmin, ymin, xmax, ymax, w, h)
    z = z[inb]
    flat = row * w + col  # flat cell index into a (h*w,) array

    stat = str(statistic).lower()

    if stat in ("max", "min"):
        # Seed with NaN so untouched cells remain NaN -> NoData.
        # np.fmax.at / np.fmin.at skip NaN operands — when the accumulator
        # starts at NaN and a real value arrives, the result is the real value.
        # When no value arrives the cell stays NaN. (numpy >=1.12; confirmed
        # on numpy 2.x used in geobrix light envs.)
        out = np.full(w * h, np.nan, dtype="float64")
        if stat == "max":
            np.fmax.at(out, flat, z)
        else:
            np.fmin.at(out, flat, z)

    elif stat == "count":
        out = np.zeros(w * h, dtype="float64")
        np.add.at(out, flat, 1.0)
        out[out == 0] = np.nan  # zero-count cells -> NoData

    elif stat == "mean":
        s = np.zeros(w * h, dtype="float64")
        c = np.zeros(w * h, dtype="float64")
        np.add.at(s, flat, z)
        np.add.at(c, flat, 1.0)
        with np.errstate(invalid="ignore"):
            out = np.where(c > 0, s / c, np.nan)

    elif stat == "median" or stat.startswith("percentile:"):
        p = 50.0 if stat == "median" else _percentile_of(stat)
        out = np.full(w * h, np.nan, dtype="float64")
        if len(flat):
            # Group by flat cell index using sorted order — avoids a Python
            # dict and is cache-friendly for large arrays.
            order = np.argsort(flat, kind="stable")
            fs = flat[order]
            zs = z[order]
            uniq, starts = np.unique(fs, return_index=True)
            ends = np.append(starts[1:], len(fs))
            for u, a, b in zip(uniq, starts, ends):
                out[int(u)] = np.percentile(zs[a:b], p)

    else:
        raise ValueError(_statistic_refusal(statistic))

    return _gtiff_of(out, xmin, ymin, xmax, ymax, w, h, srid)


def _gtiff_of(out, xmin, ymin, xmax, ymax, w: int, h: int, srid: int) -> bytes:
    """The Float32 GTiff of a flat ``(h*w,)`` float64 array of per-pixel values: NaN is NoData. The one place the tile is
    written, so :func:`bin_points` and :func:`bin_cells` write the same bytes for the same values.
    """
    # Replace NaN sentinels with the NoData value and reshape to (h, w).
    band = np.where(np.isnan(out), _NODATA, out).astype("float32").reshape(h, w)

    transform = from_bounds(xmin, ymin, xmax, ymax, w, h)
    profile = dict(
        driver="GTiff",
        height=h,
        width=w,
        count=1,
        dtype="float32",
        crs=resolve_crs(int(srid)),
        transform=transform,
        nodata=_NODATA,
    )

    return write_gtiff(profile, band[np.newaxis, ...])


def cells_working_bytes(width_px: int, height_px: int, n_cells: int) -> int:
    """An upper estimate of the memory :func:`bin_cells` holds at its peak: per output pixel the accumulator, the
    NoData copy and the Float32 band (24 bytes), per cell its two indices, flat index and value (32 bytes).
    """
    return int(width_px) * int(height_px) * 24 + int(n_cells) * 32


def cell_arrays(cells):
    """The ``(col, row, value)`` arrays of an array of structs ``(col, row, value)`` (dicts or Rows).

    A NULL element, and one with a NULL ``col`` or ``row``, is dropped; a NULL value stays as NaN, which is NoData.
    Returns ``(cols int64, rows int64, values float64)``.
    """
    cols, rows, vals = [], [], []
    for c in cells:
        if c is None or c["col"] is None or c["row"] is None:
            continue
        cols.append(c["col"])
        rows.append(c["row"])
        v = c["value"]
        vals.append(np.nan if v is None else v)
    return (
        np.asarray(cols, dtype="int64"),
        np.asarray(rows, dtype="int64"),
        np.asarray(vals, dtype="float64"),
    )


def bin_cells(
    cols,
    rows,
    values,
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    width_px: int,
    height_px: int,
    srid: int,
) -> bytes:
    """Write per-pixel values (already reduced, one per ``(col, row)``) as the tile :func:`bin_points` makes.

    ``col`` counts from the west and ``row`` from the north, as the pixel keys of ``gbx_rst_binpoints_partial`` do.
    A pixel given twice keeps its largest value; a NaN value, and a pixel in no cell, is NoData. A ``col`` or ``row``
    outside the canvas is a ValueError (the keys came from another canvas).
    """
    w, h = int(width_px), int(height_px)
    cols = np.asarray(cols, dtype="int64")
    rows = np.asarray(rows, dtype="int64")
    values = np.asarray(values, dtype="float64")
    if not len(cols) == len(rows) == len(values):
        raise ValueError(
            "rst_binpoints_finalize: col, row and value must have the same length "
            f"(got {len(cols)}, {len(rows)}, {len(values)})"
        )
    bad = (cols < 0) | (cols >= w) | (rows < 0) | (rows >= h)
    if bad.any():
        i = int(np.argmax(bad))
        raise ValueError(
            f"rst_binpoints_finalize: the cell (col {int(cols[i])}, row {int(rows[i])}) is outside the {w} x {h} canvas. "
            "Fix: group the pixel keys of gbx_rst_binpoints_partial called with this same extent, width_px and height_px"
        )
    out = np.full(w * h, np.nan, dtype="float64")
    # fmax skips NaN operands, so a NaN value leaves the pixel NoData and a repeated pixel keeps its largest value
    np.fmax.at(out, rows * w + cols, values)
    return _gtiff_of(out, xmin, ymin, xmax, ymax, w, h, srid)
