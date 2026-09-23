"""Imagery scalar utilities for photogrammetry pipelines.

Pure-Python / NumPy / PIL cores (no Spark session access) plus Arrow-based
pandas_udf wrappers for DataFrame use.

These helpers are imported directly by the ``exif_gbx`` ``qc`` mode and by the
orthomosaic Phase-1 example notebook — they must remain importable without a
SparkContext:

    from databricks.labs.gbx.pyrx.imagery import (
        gsd_from_telemetry,
        image_sharpness,
        image_brightness,
    )

Serverless-safe: no ``spark``, no ``sparkContext``, no ``_jvm``, no ``.rdd``.
"""

from __future__ import annotations

import io
import math

import numpy as np
import pandas as pd
from affine import Affine
from PIL import Image
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

# ---------------------------------------------------------------------------
# Pure-Python cores
# ---------------------------------------------------------------------------


def gsd_from_telemetry(
    alt_m: float,
    focal_mm: float,
    sensor_mm: float,
    width_px: int,
) -> float:
    """Compute ground sampling distance in **centimetres per pixel**.

    Uses the standard pinhole-camera formula:

        GSD (cm/px) = (sensor_mm * alt_m * 100) / (focal_mm * width_px)

    Parameters
    ----------
    alt_m:
        Flight altitude above ground (metres).
    focal_mm:
        Lens focal length (millimetres).  For a 35 mm-equivalent focal length
        stored in EXIF, divide by the crop factor first.
    sensor_mm:
        Sensor width (millimetres) — the physical dimension that corresponds
        to ``width_px``.
    width_px:
        Image width (pixels) matching ``sensor_mm``.

    Returns
    -------
    float
        GSD in cm/px.  Smaller values indicate higher spatial resolution.
    """
    return float((sensor_mm * alt_m * 100.0) / (focal_mm * width_px))


def image_sharpness(img_bytes: bytes) -> float:
    """Estimate image sharpness as the variance of the grayscale gradient magnitude.

    Applies ``numpy.gradient`` (first-order central differences) on both axes of
    the grayscale image to obtain the gradient components, then computes the
    variance of the resulting gradient magnitude.  A flat, blurry image returns a
    value near zero; a sharp image with edges returns a larger value.

    Parameters
    ----------
    img_bytes:
        Raw image bytes (any PIL-readable format: JPEG, PNG, TIFF, …).

    Returns
    -------
    float
        Variance of the gradient magnitude (non-negative).
    """
    img = Image.open(io.BytesIO(img_bytes)).convert("L")
    arr = np.asarray(img, dtype=np.float32)
    gy, gx = np.gradient(arr)
    magnitude = np.sqrt(gx**2 + gy**2)
    return float(np.var(magnitude))


def image_brightness(img_bytes: bytes) -> float:
    """Compute mean grayscale intensity (0–255).

    Parameters
    ----------
    img_bytes:
        Raw image bytes (any PIL-readable format).

    Returns
    -------
    float
        Mean pixel intensity of the grayscale image, in the range [0, 255].
    """
    img = Image.open(io.BytesIO(img_bytes)).convert("L")
    arr = np.asarray(img, dtype=np.float32)
    return float(np.mean(arr))


def zbuffer_ortho(
    x: np.ndarray,
    y: np.ndarray,
    z: np.ndarray,
    r: np.ndarray,
    g: np.ndarray,
    b: np.ndarray,
    *,
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    px: float,
) -> tuple[np.ndarray, np.ndarray, Affine]:
    """Top-surface z-buffer rasterization of a colored point cloud.

    Returns ``(rgb, dsm, transform)``:

    rgb:
        ``uint8`` ndarray ``(3, H, W)`` — per cell, the RGB of the max-z
        point; ``(0, 0, 0)`` where empty.
    dsm:
        ``float32`` ndarray ``(H, W)`` — per cell, the max z; ``NaN``
        where empty.
    transform:
        An :class:`affine.Affine` (north-up) for the grid so callers can
        georeference the output.

    Grid dimensions:
        ``W = ceil((xmax - xmin) / px)``,
        ``H = ceil((ymax - ymin) / px)``.
        Row 0 is the TOP row (corresponds to ``y`` near ``ymax``);
        standard north-up orientation (y decreases with row index).

    Points outside ``[xmin, xmax) × [ymin, ymax)`` are silently dropped.
    When multiple points fall in the same cell, the one with the highest
    ``z`` value contributes its RGB and z to that cell.  The algorithm is
    fully vectorised (no per-point Python loop): points are sorted by ``z``
    ascending and written via flat-index assignment so the last (highest-z)
    write wins each cell.

    Parameters
    ----------
    x, y, z:
        1-D float arrays of the same length (point coordinates).
    r, g, b:
        1-D ``uint8`` arrays of the same length (point colours).
    xmin, ymin, xmax, ymax:
        Spatial extent of the output grid.
    px:
        Cell size (same units as x/y).
    """
    W = math.ceil((xmax - xmin) / px)
    H = math.ceil((ymax - ymin) / px)

    x = np.asarray(x, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    z = np.asarray(z, dtype=np.float64)
    r = np.asarray(r, dtype=np.uint8)
    g = np.asarray(g, dtype=np.uint8)
    b = np.asarray(b, dtype=np.uint8)

    # Compute column and row indices (north-up: row 0 = top = max y).
    col = np.floor((x - xmin) / px).astype(np.intp)
    row = np.floor((ymax - y) / px).astype(np.intp)

    # Drop out-of-bounds points.
    mask = (col >= 0) & (col < W) & (row >= 0) & (row < H)
    col = col[mask]
    row = row[mask]
    z = z[mask]
    r = r[mask]
    g = g[mask]
    b = b[mask]

    # Sort ascending by z so the last (highest-z) flat-index write wins.
    order = np.argsort(z, kind="stable")
    col = col[order]
    row = row[order]
    z = z[order]
    r = r[order]
    g = g[order]
    b = b[order]

    flat_idx = row * W + col

    # Initialise outputs: DSM as NaN, RGB as 0.
    dsm = np.full(H * W, np.nan, dtype=np.float32)
    r_out = np.zeros(H * W, dtype=np.uint8)
    g_out = np.zeros(H * W, dtype=np.uint8)
    b_out = np.zeros(H * W, dtype=np.uint8)

    # Vectorised z-buffer write: ascending sort + direct assignment means
    # the last write per duplicate flat_idx is the highest-z point.
    dsm[flat_idx] = z.astype(np.float32)
    r_out[flat_idx] = r
    g_out[flat_idx] = g
    b_out[flat_idx] = b

    dsm = dsm.reshape(H, W)
    rgb = np.stack(
        [r_out.reshape(H, W), g_out.reshape(H, W), b_out.reshape(H, W)],
        axis=0,
    )

    # North-up affine: pixel (col, row) → (xmin + col*px, ymax - row*px).
    transform = Affine(px, 0.0, float(xmin), 0.0, -px, float(ymax))

    return rgb, dsm, transform


# ---------------------------------------------------------------------------
# GPS clustering for memory-bounded photogrammetry
# ---------------------------------------------------------------------------


def cluster_by_gps(
    df: pd.DataFrame,
    *,
    target_cluster_images: int = 55,
    overlap_frac: float = 0.30,
    min_cluster_images: int = 8,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
) -> pd.DataFrame:
    """Partition images into spatially-contiguous, RAM-bounded GPS clusters.

    Monolithic SfM (``pycolmap.incremental_mapping``) is single-node and its peak
    RAM scales with image count, so a large survey OOMs the driver. This function
    partitions images by GPS proximity into clusters small enough to reconstruct
    within a bounded memory budget, with an **overlap ring** of shared boundary
    images so adjacent clusters can be georeferenced into a common frame and
    blended into one orthomosaic without hard seams.

    The partition is built by recursive median bisection of the camera positions
    (each leaf cell has at most ``target_cluster_images`` "home" images), then an
    overlap ring adds boundary images from neighbouring cells (capped so a
    cluster never materially exceeds the target), then clusters smaller than
    ``min_cluster_images`` are merged into their nearest neighbour.

    Parameters
    ----------
    df:
        Per-image rows including ``lat_col`` and ``lon_col`` (decimal degrees) and
        any identifier columns (e.g. ``source``). Returned rows preserve all input
        columns.
    target_cluster_images:
        Target home-image count per cluster (the RAM knob). Lower it if a cluster
        still exceeds the driver's memory during reconstruction.
    overlap_frac:
        Fraction of a cell's extent used as the overlap ring on each side. ``0``
        yields a strict partition (no shared images); ``0.3`` shares ~30% boundary
        bands with neighbours.
    min_cluster_images:
        Clusters smaller than this are merged into the nearest cluster (too-small
        clusters fail to reconstruct).
    lat_col, lon_col:
        Column names for latitude / longitude.

    Returns
    -------
    pandas.DataFrame
        The input rows plus an integer ``_cluster`` column, with boundary images
        **duplicated** once per cluster they belong to (so ``len(result) >=
        len(df)``). Cluster ids are contiguous from 0. Every input image appears
        in at least one cluster.

    Notes
    -----
    Pure NumPy/pandas; no Spark session access (Serverless-safe). Intended to run
    driver-side on the (small) per-image telemetry table, then joined back to the
    image set for per-cluster reconstruction.
    """
    if target_cluster_images < 1:
        raise ValueError("target_cluster_images must be >= 1")

    work = df.reset_index(drop=True)
    n = len(work)
    lat = work[lat_col].to_numpy(dtype=float)
    lon = work[lon_col].to_numpy(dtype=float)

    finite = np.isfinite(lat) & np.isfinite(lon)
    has_spread = finite.any() and (np.ptp(lat[finite]) > 0 or np.ptp(lon[finite]) > 0)
    if n <= target_cluster_images or not has_spread:
        out = work.copy()
        out["_cluster"] = 0
        return out

    # Project lon/lat to a local metric plane (equirectangular about the centroid).
    lat0 = float(np.nanmean(lat))
    lon0 = float(np.nanmean(lon))
    x = (lon - lon0) * 111320.0 * math.cos(math.radians(lat0))
    y = (lat - lat0) * 110540.0
    x = np.where(np.isfinite(x), x, 0.0)
    y = np.where(np.isfinite(y), y, 0.0)

    # 1) Recursive median bisection: leaf cells with <= target home images.
    leaves = []  # list of (idx_array, (x0, x1, y0, y1))
    stack = [np.arange(n)]
    while stack:
        idx = stack.pop()
        if len(idx) <= target_cluster_images:
            leaves.append(
                (idx, (x[idx].min(), x[idx].max(), y[idx].min(), y[idx].max()))
            )
            continue
        coord = x[idx] if np.ptp(x[idx]) >= np.ptp(y[idx]) else y[idx]
        med = np.median(coord)
        left, right = idx[coord <= med], idx[coord > med]
        if len(left) == 0 or len(right) == 0:  # degenerate median (ties)
            order = idx[np.argsort(coord, kind="stable")]
            half = len(order) // 2
            left, right = order[:half], order[half:]
        stack.extend((left, right))

    # 2) Home cluster per point.
    home = np.empty(n, dtype=int)
    cell_bounds = []
    for cid, (idx, bounds) in enumerate(leaves):
        home[idx] = cid
        cell_bounds.append(bounds)
    members = {
        cid: set(np.where(home == cid)[0].tolist()) for cid in range(len(leaves))
    }

    # 3) Overlap ring (capped so a cluster never materially exceeds the target).
    if overlap_frac > 0:
        cap = math.ceil(target_cluster_images * (1 + 2 * overlap_frac))
        for cid, (x0, x1, y0, y1) in enumerate(cell_bounds):
            rx = overlap_frac * max(x1 - x0, 1e-9)
            ry = overlap_frac * max(y1 - y0, 1e-9)
            cand = np.where(
                (x >= x0 - rx) & (x <= x1 + rx) & (y >= y0 - ry) & (y <= y1 + ry)
            )[0]
            members[cid].update(int(p) for p in cand)
            if len(members[cid]) > cap:
                cx, cy = 0.5 * (x0 + x1), 0.5 * (y0 + y1)
                home_pts = set(np.where(home == cid)[0].tolist())
                ring = sorted(
                    (p for p in members[cid] if p not in home_pts),
                    key=lambda p: (x[p] - cx) ** 2 + (y[p] - cy) ** 2,
                )
                members[cid] = home_pts | set(ring[: max(0, cap - len(home_pts))])

    # 4) Merge clusters smaller than min into their nearest neighbour.
    def _centroid(cid):
        pts = np.fromiter(members[cid], dtype=int)
        return x[pts].mean(), y[pts].mean()

    while True:
        active = [cid for cid in members if members[cid]]
        if len(active) <= 1:
            break
        smalls = [cid for cid in active if len(members[cid]) < min_cluster_images]
        if not smalls:
            break
        tiny = min(smalls, key=lambda c: len(members[c]))
        tcx, tcy = _centroid(tiny)
        nearest = min(
            (c for c in active if c != tiny),
            key=lambda c: (lambda cx, cy: (cx - tcx) ** 2 + (cy - tcy) ** 2)(
                *_centroid(c)
            ),
        )
        members[nearest] |= members[tiny]
        members[tiny] = set()

    # 5) Relabel contiguous from 0 and emit exploded rows.
    active = [cid for cid in sorted(members) if members[cid]]
    relabel = {cid: k for k, cid in enumerate(active)}
    frames = []
    for cid in active:
        sub = work.iloc[sorted(members[cid])].copy()
        sub["_cluster"] = relabel[cid]
        frames.append(sub)
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Point-cloud I/O (pure Python / NumPy / laspy — no Spark)
# ---------------------------------------------------------------------------

# Mapping from PLY scalar type names to NumPy dtype strings.
_PLY_TYPE_MAP: dict[str, str] = {
    "char": "i1",
    "int8": "i1",
    "uchar": "u1",
    "uint8": "u1",
    "short": "i2",
    "int16": "i2",
    "ushort": "u2",
    "uint16": "u2",
    "int": "i4",
    "int32": "i4",
    "uint": "u4",
    "uint32": "u4",
    "float": "f4",
    "float32": "f4",
    "double": "f8",
    "float64": "f8",
    "long": "i8",
    "int64": "i8",
    "ulong": "u8",
    "uint64": "u8",
}


def read_fused_ply(path: str) -> dict[str, np.ndarray]:
    """Read a COLMAP binary PLY point cloud into arrays.

    Returns a dict: ``{"x","y","z": float64 (N,), "r","g","b": uint8 (N,)}``.

    Handles PLY ``format binary_little_endian 1.0``, vertex element with
    ``x,y,z`` (float or double) + ``red,green,blue`` (uchar), tolerating extra
    properties (``nx,ny,nz``, etc.) by using each property's declared type and
    order.  Raises :class:`ValueError` for ASCII or big-endian PLY.

    Parameters
    ----------
    path:
        Filesystem path to the ``.ply`` file (binary little-endian).

    Returns
    -------
    dict
        Six NumPy arrays keyed ``"x"``, ``"y"``, ``"z"`` (float64) and
        ``"r"``, ``"g"``, ``"b"`` (uint8), each of length N (the vertex count).
    """
    with open(path, "rb") as fh:
        # --- Parse ASCII header -----------------------------------------------
        header: list[str] = []
        while True:
            raw = fh.readline()
            line = raw.decode("ascii", errors="replace").rstrip("\r\n")
            header.append(line)
            if line.strip() == "end_header":
                break

        if not header or header[0].strip() != "ply":
            raise ValueError("Not a PLY file (missing 'ply' magic line)")

        # Locate format declaration.
        fmt_line = next((ln for ln in header if ln.startswith("format ")), None)
        if fmt_line is None:
            raise ValueError("PLY file is missing a 'format' declaration line")
        fmt = fmt_line.split()[1]
        if fmt != "binary_little_endian":
            raise ValueError(
                f"Only binary_little_endian PLY is supported; got '{fmt}'. "
                "Re-export the point cloud in binary little-endian format."
            )

        # Collect vertex element properties in declaration order.
        n_verts: int | None = None
        in_vertex = False
        props: list[tuple[str, str]] = []

        for ln in header:
            if ln.startswith("element vertex "):
                n_verts = int(ln.split()[2])
                in_vertex = True
            elif ln.startswith("element "):
                in_vertex = False
            elif ln.startswith("property ") and in_vertex:
                parts = ln.split()
                ply_type, prop_name = parts[1], parts[2]
                np_type = _PLY_TYPE_MAP.get(ply_type)
                if np_type is None:
                    raise ValueError(
                        f"Unsupported PLY scalar type '{ply_type}' for property '{prop_name}'. "
                        "List-properties are not supported."
                    )
                props.append((prop_name, np_type))

        if n_verts is None:
            raise ValueError("PLY file missing 'element vertex N' declaration")

        # --- Read binary body --------------------------------------------------
        dtype = np.dtype([(name, dt) for name, dt in props])
        raw_body = fh.read(n_verts * dtype.itemsize)
        data = np.frombuffer(raw_body, dtype=dtype)

    return {
        "x": data["x"].astype(np.float64),
        "y": data["y"].astype(np.float64),
        "z": data["z"].astype(np.float64),
        "r": data["red"].astype(np.uint8),
        "g": data["green"].astype(np.uint8),
        "b": data["blue"].astype(np.uint8),
    }


def write_xyzrgb_laz(
    path: str,
    x: np.ndarray,
    y: np.ndarray,
    z: np.ndarray,
    r: np.ndarray,
    g: np.ndarray,
    b: np.ndarray,
    *,
    crs: int | str | None = None,
) -> str:
    """Write a colored point cloud to LAS/LAZ via laspy (RGB point format).

    Uses LAS point format 2 (XYZ + RGB, no GPS time).  LAS RGB channels are
    16-bit; uint8 values are scaled to 16-bit via ``v << 8`` so that 255
    maps to 0xFF00 and the round-trip recovers the original value with
    ``(v16 >> 8).astype(np.uint8)``.

    Header scales and offsets are derived from the data bounds to give
    sub-millimetre precision across the cloud extent.

    Parameters
    ----------
    path:
        Output file path.  Writes ``.laz`` when the path ends in ``.laz``
        (requires a lazrs or laszip backend); otherwise writes ``.las``.
        If the LAZ backend is absent, falls back to ``.las`` with a warning
        and returns the fallback path.
    x, y, z:
        1-D float arrays of the same length (point coordinates).
    r, g, b:
        1-D uint8 arrays of the same length (0-255 per channel).
    crs:
        Optional EPSG integer or WKT string to tag the file header.
        Requires ``pyproj``; silently skipped with a warning if unavailable
        or if the CRS cannot be resolved.

    Returns
    -------
    str
        The path actually written (may differ from *path* on LAZ fallback).
    """
    import laspy

    x = np.asarray(x, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    z = np.asarray(z, dtype=np.float64)
    r = np.asarray(r, dtype=np.uint8)
    g = np.asarray(g, dtype=np.uint8)
    b = np.asarray(b, dtype=np.uint8)

    hdr = laspy.LasHeader(point_format=2)

    if len(x) > 0:
        hdr.offsets = np.array([float(x.min()), float(y.min()), float(z.min())])
        # Scale so that the full range fits in ~1e7 integer steps (sub-mm).
        ranges = np.array([np.ptp(x), np.ptp(y), np.ptp(z)])
        hdr.scales = np.maximum(ranges / 1e7, 1e-6)
    else:
        hdr.offsets = np.zeros(3)
        hdr.scales = np.full(3, 1e-6)

    if crs is not None:
        try:
            from pyproj import CRS as ProjCRS

            proj_crs = (
                ProjCRS.from_epsg(crs) if isinstance(crs, int) else ProjCRS.from_wkt(str(crs))
            )
            hdr.add_crs(proj_crs)
        except Exception as _crs_err:
            import warnings

            warnings.warn(
                f"Could not tag CRS on LAS header ({_crs_err!r}); writing without CRS.",
                stacklevel=2,
            )

    las = laspy.LasData(header=hdr)
    las.x = x
    las.y = y
    las.z = z
    # Scale uint8 [0..255] to 16-bit [0..0xFF00] so 255 → 0xFF00.
    las.red = r.astype(np.uint16) << 8
    las.green = g.astype(np.uint16) << 8
    las.blue = b.astype(np.uint16) << 8

    out_path = str(path)
    try:
        las.write(out_path)
    except Exception as laz_err:
        if out_path.lower().endswith(".laz"):
            las_path = out_path[:-4] + ".las"
            import warnings

            warnings.warn(
                f"LAZ write failed ({laz_err!r}); falling back to {las_path}.",
                stacklevel=2,
            )
            las.write(las_path)
            return las_path
        raise
    return out_path


# ---------------------------------------------------------------------------
# pandas_udf wrappers for DataFrame / SQL use
# ---------------------------------------------------------------------------


@pandas_udf(DoubleType())
def gsd_udf(
    alt_m: pd.Series,
    focal_mm: pd.Series,
    sensor_mm: pd.Series,
    width_px: pd.Series,
) -> pd.Series:
    """Arrow-vectorised wrapper for :func:`gsd_from_telemetry`.

    All four series must have matching length (one row per image).
    """
    return (sensor_mm * alt_m * 100.0) / (focal_mm * width_px)


@pandas_udf(DoubleType())
def sharpness_udf(img: pd.Series) -> pd.Series:
    """Arrow-vectorised wrapper for :func:`image_sharpness`.

    Parameters
    ----------
    img:
        Series of ``bytes`` — raw image file contents.
    """
    return img.apply(image_sharpness)


@pandas_udf(DoubleType())
def brightness_udf(img: pd.Series) -> pd.Series:
    """Arrow-vectorised wrapper for :func:`image_brightness`.

    Parameters
    ----------
    img:
        Series of ``bytes`` — raw image file contents.
    """
    return img.apply(image_brightness)
