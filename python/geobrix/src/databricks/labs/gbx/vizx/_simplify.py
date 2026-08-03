"""Tile simplification specification schema and validation.

Public API: normalize_spec, simplify_tiles_from_source.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Union

log = logging.getLogger(__name__)

# 1 MB in bytes
_MB = 1024 * 1024


def normalize_spec(spec: dict | None) -> dict:
    """
    Apply defaults to a simplify_tiles spec and validate.

    Defaults: budget_mb=64, min_z=0, max_z=10, tolerance="auto",
    drop_densest=True, cluster_distance=None, keep_attrs=None,
    raster_max_px=1024, effort="fast".

    Validates:
    - min_z <= max_z (raises ValueError if not)
    - budget_mb > 0 (raises ValueError if not)
    - effort in {"fast", "full"} (raises ValueError if not)

    Args:
        spec: Optional dict with overrides. None returns all defaults.

    Returns:
        Merged and validated dict.

    Raises:
        ValueError: On validation failure (min_z > max_z, budget_mb <= 0, invalid effort).
    """
    defaults = {
        "budget_mb": 64,  # per-tile byte cap (tippecanoe --maximum-tile-bytes); NOT a total archive ceiling
        "min_z": 0,
        "max_z": 10,
        "tolerance": "auto",
        "drop_densest": True,
        "cluster_distance": None,
        "keep_attrs": None,
        "raster_max_px": 1024,
        "effort": "fast",
    }

    if spec is None:
        result = defaults.copy()
    else:
        result = defaults.copy()
        result.update(spec)

    # Validate
    if result["min_z"] > result["max_z"]:
        raise ValueError(
            f"min_z ({result['min_z']}) must be <= max_z ({result['max_z']})"
        )

    if result["budget_mb"] <= 0:
        raise ValueError(f"budget_mb must be > 0, got {result['budget_mb']}")

    if result["effort"] not in {"fast", "full"}:
        raise ValueError(f"effort must be 'fast' or 'full', got {result['effort']!r}")

    return result


# ---------------------------------------------------------------------------
# Source-type detection helpers
# ---------------------------------------------------------------------------


def _is_geodataframe(source) -> bool:
    """True if source is a geopandas GeoDataFrame (lazy import)."""
    try:
        import geopandas as gpd

        return isinstance(source, gpd.GeoDataFrame)
    except ImportError:
        return False


def _is_spark_dataframe(source) -> bool:
    """True if source is a PySpark DataFrame (lazy import)."""
    try:
        from pyspark.sql import DataFrame

        return isinstance(source, DataFrame)
    except ImportError:
        return False


def _is_vector_path(source) -> bool:
    """True for a file path that looks like a vector source."""
    if not isinstance(source, (str, Path)):
        return False
    suffix = Path(source).suffix.lower()
    return suffix in {".geojson", ".json", ".gpkg", ".shp", ".fgb"}


def _is_raster_source(source) -> bool:
    """True for ndarray or a file path that looks like a raster."""
    try:
        import numpy as np

        if isinstance(source, np.ndarray):
            return True
    except ImportError:
        pass
    if isinstance(source, (str, Path)):
        suffix = Path(source).suffix.lower()
        return suffix in {".tif", ".tiff", ".img", ".vrt", ".nc", ".hdf", ".jp2"}
    # dict-like tile struct
    if isinstance(source, dict) and ("raster" in source or "content" in source):
        return True
    return False


# ---------------------------------------------------------------------------
# Vector path via tippecanoe
# ---------------------------------------------------------------------------

# Candidate tippecanoe binary names to try in preference order.
# The PyPI `tippecanoe` wheel bundles the actual C binary in its BIN_DIR; on
# Linux the wrapper script that lands on $PATH raises SystemExit with the child
# exit-code, which confuses subprocess.run when capture_output=True. We resolve
# the real binary by:
#   1. Looking inside the PyPI package's BIN_DIR (most reliable).
#   2. Falling back to PATH lookup (works on macOS brew install or system pkg).
_TIPPECANOE_BIN: str | None = None  # cached after first resolution


def _resolve_tippecanoe_bin() -> str | None:
    """Return the path to the real tippecanoe C binary, or None if unavailable."""
    global _TIPPECANOE_BIN
    if _TIPPECANOE_BIN is not None:
        return _TIPPECANOE_BIN

    # 1. PyPI-wheel BIN_DIR (Linux manylinux wheel path)
    try:
        import tippecanoe as _tc_pkg

        candidate = str(Path(_tc_pkg.BIN_DIR) / "tippecanoe")
        if Path(candidate).is_file() and os.access(candidate, os.X_OK):
            # Quick sanity: run with --version; the real binary exits 0 on success.
            try:
                r = subprocess.run(
                    [candidate, "--version"],
                    capture_output=True,
                    timeout=10,
                )
                if r.returncode == 0:
                    _TIPPECANOE_BIN = candidate
                    return _TIPPECANOE_BIN
            except Exception:
                pass
    except ImportError:
        pass

    # 2. PATH lookup — works on macOS (brew) and system packages.
    path_bin = shutil.which("tippecanoe")
    if path_bin:
        # If the found binary is itself a Python script (PyPI wrapper), try to
        # extract BIN_DIR from it.
        try:
            with open(path_bin, "rb") as f:
                header = f.read(256)
            if b"python" in header.lower() or b"tippecanoe" in header[50:]:
                # Probably the wrapper; we already tried BIN_DIR above.
                pass
            else:
                _TIPPECANOE_BIN = path_bin
                return _TIPPECANOE_BIN
        except Exception:
            pass

        # Try running the PATH binary directly — works on macOS/system installs.
        try:
            r = subprocess.run(
                [path_bin, "--version"],
                capture_output=True,
                timeout=10,
            )
            if r.returncode == 0:
                _TIPPECANOE_BIN = path_bin
                return _TIPPECANOE_BIN
        except Exception:
            pass

    return None


def _ensure_tippecanoe() -> str:
    """Return the real tippecanoe binary path; raise ImportError if not available."""
    bin_path = _resolve_tippecanoe_bin()
    if bin_path is None:
        raise ImportError(
            "tippecanoe binary not found or not executable. "
            "Install it via: pip install geobrix[vizx]  "
            "(which pulls in the tippecanoe manylinux wheel), or "
            "brew install tippecanoe (macOS), or build from source."
        )
    return bin_path


def _gdf_to_geojson(gdf, tmp_dir: str) -> str:
    """Write geopandas GeoDataFrame to a temp GeoJSON file; return its path."""
    path = str(Path(tmp_dir) / "input.geojson")
    gdf.to_file(path, driver="GeoJSON")
    return path


def _vector_path_to_geojson(source_path, tmp_dir: str) -> str:
    """Convert a vector file to GeoJSON in tmp_dir if needed, else return as-is."""
    src = Path(source_path)
    if src.suffix.lower() in {".geojson", ".json"}:
        return str(src)
    # Other formats: read via geopandas and re-export
    import geopandas as gpd

    gdf = gpd.read_file(str(src))
    return _gdf_to_geojson(gdf, tmp_dir)


def _build_tippecanoe_argv(
    bin_path: str, in_geojson: str, out_pmtiles: str, spec: dict
) -> list:
    """Build the tippecanoe command-line from a normalized spec dict."""
    argv = [
        bin_path,
        f"-z{spec['max_z']}",
        f"-Z{spec['min_z']}",
        f"--maximum-tile-bytes={int(spec['budget_mb'] * _MB)}",
        "--force",
        "-o",
        out_pmtiles,
        in_geojson,
    ]
    if spec.get("drop_densest"):
        argv.insert(-1, "--drop-densest-as-needed")
    cd = spec.get("cluster_distance")
    if cd is not None:
        argv.insert(-1, f"--cluster-distance={int(cd)}")
    return argv


def _run_tippecanoe(argv: list) -> None:
    """Execute tippecanoe; surface stderr on failure."""
    log.debug("tippecanoe argv: %s", argv)
    result = subprocess.run(argv, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"tippecanoe failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )
    log.debug("tippecanoe stdout: %s", result.stdout)


def _spark_df_to_gdf(df, geom_col: str):
    """Collect a Spark DataFrame with a geometry column into a GeoDataFrame (EPSG:4326).

    Unlike :func:`vizx.as_gdf` (WKT-only, row-capped at 10k for driver-side display),
    this collects ALL rows -- tiling needs every feature, and a silent cap would drop
    most of them -- and accepts the geometry column as WKB bytes, WKT strings, or a
    Spark GEOMETRY/GEOGRAPHY type (normalized to WKB via ST_AsBinary first). The
    collected frame must fit in driver memory.
    """
    import geopandas as gpd

    if geom_col not in df.columns:
        raise ValueError(
            f"simplify_tiles_from_source: geom_col {geom_col!r} not in DataFrame "
            f"columns {df.columns}. Pass geom_col=<your geometry column>."
        )
    # A Spark GEOMETRY/GEOGRAPHY type does not round-trip through pandas as bytes; ask
    # Spark for WKB up front. Binary (WKB) and string (WKT) columns pass through as-is.
    type_name = df.schema[geom_col].dataType.typeName().lower()
    if type_name not in ("binary", "string"):
        from pyspark.sql import functions as F

        df = df.withColumn(geom_col, F.expr(f"ST_AsBinary({geom_col})"))

    pdf = df.toPandas()  # ALL rows -- no cap (tiling needs every feature)
    col = pdf[geom_col]
    sample = next((v for v in col if v is not None), None)
    if isinstance(sample, (bytes, bytearray)):
        # Spark BinaryType collects as bytearray; shapely.from_wkb wants bytes.
        wkb = [bytes(v) if v is not None else None for v in col]
        geom = gpd.GeoSeries.from_wkb(wkb, crs=4326)
    elif isinstance(sample, str):
        geom = gpd.GeoSeries.from_wkt(col, crs=4326)
    elif sample is None:
        raise ValueError(
            f"simplify_tiles_from_source: geometry column {geom_col!r} is all-null."
        )
    else:
        raise TypeError(
            f"simplify_tiles_from_source: geometry column {geom_col!r} collected as "
            f"{type(sample).__name__}, expected WKB bytes or WKT text. Convert it with "
            "ST_AsBinary(...) or ST_AsText(...) in Spark first."
        )
    attrs = pdf.drop(columns=[geom_col])
    return gpd.GeoDataFrame(attrs, geometry=geom.values, crs=4326)


def _simplify_vector(
    source,
    spec: dict,
    out_path: str | None,
    bbox: tuple | None = None,
    geom_col: str = "geometry",
) -> Union[bytes, str]:
    """Vector branch: GeoDataFrame / path / Spark DF → PMTiles bytes or path."""
    bin_path = _ensure_tippecanoe()

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Resolve source → GeoDataFrame or GeoJSON path
        if _is_spark_dataframe(source):
            gdf = _spark_df_to_gdf(source, geom_col)
        elif _is_geodataframe(source):
            gdf = source
        elif _is_vector_path(source):
            import geopandas as gpd

            gdf = gpd.read_file(str(source))
        else:
            raise TypeError(
                f"Cannot interpret source as a vector: {type(source).__name__}. "
                "Pass a GeoDataFrame, Spark DataFrame, or path to a vector file."
            )

        # Apply bbox clip if requested.
        if bbox is not None:
            import shapely.geometry

            clip_box = shapely.geometry.box(*bbox)
            gdf = gdf[gdf.intersects(clip_box)]

        in_geojson = _gdf_to_geojson(gdf, tmp_dir)

        out_pmtiles = str(Path(tmp_dir) / "out.pmtiles")
        argv = _build_tippecanoe_argv(bin_path, in_geojson, out_pmtiles, spec)
        _run_tippecanoe(argv)

        if out_path is not None:
            shutil.copy2(out_pmtiles, out_path)
            log.info("engine=tippecanoe → %s", out_path)
            return out_path
        else:
            data = Path(out_pmtiles).read_bytes()
            log.info("engine=tippecanoe → %d bytes in memory", len(data))
            return data


# ---------------------------------------------------------------------------
# Raster path via rasterio (overview downsample → COG)
# ---------------------------------------------------------------------------


def _simplify_raster(source, spec: dict, out_path: str | None) -> Union[bytes, str]:
    """Raster branch: path / ndarray → decimated COG bytes or path.

    Downsamples the raster to <= raster_max_px on the longest axis using
    rasterio's bilinear resampling, writes a cloud-optimised GeoTIFF (COG).
    Full raster-PMTiles encoding (mvt/png tiles + PMTiles archive) is deferred
    to the distributed engine (engine='distributed', not yet implemented);
    this driver-side path produces a ready-to-serve COG instead, which is
    honest about the limitation.
    """
    import numpy as np

    try:
        import rasterio
        from rasterio.enums import Resampling
        from rasterio.transform import from_bounds
    except ImportError as exc:
        raise ImportError(
            "rasterio is required for the raster path. "
            "Install it via: pip install geobrix[vizx]"
        ) from exc

    raster_max_px = spec["raster_max_px"]

    with tempfile.TemporaryDirectory() as tmp_dir:
        out_cog = str(Path(tmp_dir) / "out.tif")

        if isinstance(source, np.ndarray):
            # ndarray: must be (bands, height, width) or (height, width)
            arr = source
            if arr.ndim == 2:
                arr = arr[np.newaxis, ...]
            bands, h, w = arr.shape
            scale = max(h, w) / raster_max_px
            if scale > 1:
                new_h = max(1, int(h / scale))
                new_w = max(1, int(w / scale))
                # PIL for resizing avoids the rasterio-dataset requirement
                try:
                    from PIL import Image

                    resized = np.stack(
                        [
                            np.array(
                                Image.fromarray(arr[b].astype(np.float32)).resize(
                                    (new_w, new_h)
                                )
                            )
                            for b in range(bands)
                        ]
                    )
                except ImportError:
                    # Fallback: nearest-neighbour via index slicing
                    row_idx = np.round(np.linspace(0, h - 1, new_h)).astype(int)
                    col_idx = np.round(np.linspace(0, w - 1, new_w)).astype(int)
                    resized = arr[:, row_idx][:, :, col_idx]
            else:
                resized = arr
                new_h, new_w = h, w

            transform = from_bounds(0, 0, new_w, new_h, new_w, new_h)
            with rasterio.open(
                out_cog,
                "w",
                driver="GTiff",
                height=new_h,
                width=new_w,
                count=bands,
                dtype=str(resized.dtype),
                transform=transform,
            ) as dst:
                dst.write(resized.astype(resized.dtype))

        else:
            # File path source
            src_path = str(source)
            with rasterio.open(src_path) as src:
                scale = max(src.width, src.height) / raster_max_px
                if scale > 1:
                    new_h = max(1, int(src.height / scale))
                    new_w = max(1, int(src.width / scale))
                else:
                    new_h, new_w = src.height, src.width

                out_shape = (src.count, new_h, new_w)
                data = src.read(out_shape=out_shape, resampling=Resampling.bilinear)
                transform = src.transform * src.transform.scale(
                    src.width / new_w, src.height / new_h
                )
                profile = src.profile.copy()

            from databricks.labs.gbx.pyrx.core import compression as _comp

            # Route through the compression authority with compress="lzw" (kept for
            # viz-only output compat; the authority adds the correct dtype predictor).
            _comp_opts = _comp.creation_opts(
                str(data.dtype), decoded_bytes=data.nbytes, compress="lzw"
            )
            profile.update(
                driver="GTiff",
                height=new_h,
                width=new_w,
                transform=transform,
                tiled=True,
                blockxsize=256,
                blockysize=256,
            )
            profile.update(_comp_opts)
            with rasterio.open(out_cog, "w", **profile) as dst:
                dst.write(data)

        log.info(
            "engine=rasterio (COG overview) → raster_max_px=%d; "
            "note: full raster-PMTiles (mvt/png tile pyramid) requires engine='distributed' (not yet implemented).",
            raster_max_px,
        )

        if out_path is not None:
            shutil.copy2(out_cog, out_path)
            return out_path
        else:
            return Path(out_cog).read_bytes()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def simplify_tiles_from_source(
    source,
    *,
    spec: dict | None = None,
    out_path: str | None = None,
    bbox: tuple | None = None,
    geom_col: str = "geometry",
) -> Union[bytes, str]:
    """Re-tile a vector or raster SOURCE into a budget-bounded PMTiles overview.

    Args:
        source: One of:
            - geopandas.GeoDataFrame  (vector)
            - PySpark DataFrame with a geometry/WKT column (vector)
            - str/Path to a GeoJSON, GeoPackage, Shapefile, or FlatGeobuf (vector)
            - str/Path to a GeoTIFF or other raster (raster)
            - numpy ndarray (raster; shape (bands, H, W) or (H, W))
        spec: Optional dict of simplify options (see normalize_spec for keys).
            Key ``engine`` is special:
              - ``"distributed"`` → raises NotImplementedError (future tier).
              - Default (absent / None): driver-side tippecanoe (vector) or
                rasterio (raster).
        out_path: If given, the output is written to this path and the path
            string is returned. If None, the raw bytes are returned.
        bbox: Optional ``(min_lon, min_lat, max_lon, max_lat)`` tuple in
            WGS-84 degrees.  When given, the source is spatially clipped to
            this bounding box before tiling; features wholly outside the box
            are dropped.  Only applied to GeoDataFrame / vector sources;
            ignored for raster sources.  ``None`` → no clipping (default).
        geom_col: Name of the geometry column when ``source`` is a Spark
            DataFrame (default ``"geometry"``). The column may hold WKB bytes,
            WKT strings, or a Spark GEOMETRY/GEOGRAPHY type. ALL rows are
            collected to the driver (no row cap) -- tiling needs every feature.
            Ignored for GeoDataFrame / path / raster sources.

    Returns:
        bytes: the PMTiles (vector) or COG (raster) archive, when out_path=None.
        str:   out_path, when out_path was specified.

    Raises:
        NotImplementedError: if spec contains engine='distributed'.
        ImportError: if tippecanoe binary is missing (vector path).
        TypeError: if source cannot be identified as vector or raster.
        ValueError: if ``geom_col`` is absent from a Spark DataFrame source.
    """
    merged = normalize_spec(spec)

    if merged.get("engine") == "distributed":
        raise NotImplementedError(
            "distributed engine: future — the distributed GeoBrix-tiling "
            "branch is not yet implemented for simplify_tiles_from_source."
        )

    # Route by source type
    if (
        _is_geodataframe(source)
        or _is_spark_dataframe(source)
        or _is_vector_path(source)
    ):
        return _simplify_vector(source, merged, out_path, bbox=bbox, geom_col=geom_col)
    elif _is_raster_source(source):
        return _simplify_raster(source, merged, out_path)
    else:
        raise TypeError(
            f"Cannot determine source type for {type(source).__name__!r}. "
            "Pass a GeoDataFrame, Spark DataFrame, vector path, raster path, or numpy array."
        )
