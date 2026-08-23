"""Executable doc examples for VizX raster rendering (Docker).

Contains doc-test examples for plot_mosaic.  Runs in Docker with the full
environment (rasterio, matplotlib, h3, pyspark).  No /Volumes sample data needed —
examples synthesize small rasters.
"""

import os
import tempfile

import matplotlib

matplotlib.use("Agg")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_small_raster(path: str) -> None:
    """Write a small EPSG:32632 GeoTIFF (200×120 px, 1-band uint16).

    Deterministic: pixel value = (row*200 + col + 1) % 65535.
    All nonzero so no tile is all-nodata and pruneEmpty keeps them all.
    """
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    os.makedirs(os.path.dirname(path), exist_ok=True)
    profile = dict(
        driver="GTiff",
        width=200,
        height=120,
        count=1,
        dtype="uint16",
        crs="EPSG:32632",
        transform=from_origin(400000.0, 5000000.0, 10.0, 10.0),
    )
    data = (np.arange(200 * 120, dtype="uint32").reshape(1, 120, 200) + 1).astype(
        "uint16"
    )
    data = data % 65535
    data[data == 0] = 1
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)


def _build_small_h3_mosaic(base_dir: str) -> str:
    """Build a small h3 mini-COG mosaic from a synthetic raster.

    Mirrors the test helper in python/geobrix/test/ds/test_mosaic_write.py
    (``_write_mosaic_for_test``), but also calls ``commit([msg])`` so that
    ``mosaic.vrt`` is produced — required by ``plot_mosaic``.

    Returns the mosaic directory path (which contains ``mosaic.vrt``).
    """
    from pyspark.sql.types import StringType, StructField, StructType

    from databricks.labs.gbx.ds.cog_writer import CogGbxWriter, parse_mosaic_options

    src_path = os.path.join(base_dir, "src", "input.tif")
    mosaic_dir = os.path.join(base_dir, "mosaic_h3")

    _write_small_raster(src_path)

    schema = StructType([StructField("path", StringType(), False)])
    opts = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "h3", "gridResolution": "5"}
    )
    os.makedirs(mosaic_dir, exist_ok=True)
    writer = CogGbxWriter(
        mosaic_dir,
        schema,
        overwrite=True,
        cog_blocksize=256,
        mosaic_opts=opts,
    )
    msg = writer.write(iter([{"path": src_path}]))
    writer.commit([msg])
    return mosaic_dir


# ---------------------------------------------------------------------------
# Doc-test functions
# ---------------------------------------------------------------------------


def plot_mosaic_example():
    """Build a small h3 mini-COG mosaic and render it memory-safely.

    Covers: directory-based VRT resolution, h3 cell-outline overlay, and the
    memory-safe read ceiling (max_pixels).
    """
    import matplotlib.pyplot as plt

    from databricks.labs.gbx.vizx import plot_mosaic

    with tempfile.TemporaryDirectory() as tmp:
        vrt_dir = _build_small_h3_mosaic(tmp)

        plt.close("all")
        plot_mosaic(vrt_dir, show_cells=True, max_pixels=512, debug_mode=0)

        n_figs = len(plt.get_fignums())
        assert n_figs == 1, (
            f"expected 1 open figure after plot_mosaic, got {n_figs}"
        )
        plt.close("all")

    return True
