"""What a raster_gbx read plans: one partition per clip polygon, window or tile, decided on the driver.

Rows go through the public path: the reader built from its options, then ``partitions()``. Only the
raster header is opened (no pixels), so even the 10 GB raster of the tileSize guard is a sparse file.
"""

from test._volume_safe import open_raster

import numpy as np
import pytest
import rasterio
import shapely.wkb
from rasterio.transform import from_origin
from shapely import set_srid
from shapely.geometry import box

from databricks.labs.gbx.ds.raster import RasterGbxReader


def raster(tmp_path, width=1000, height=1000, tiled=False, name="r.tif"):
    """A float32 GTiff at 10E 50N, 0.001 degree pixels. `tiled` keeps it header-sized: a sparse file with one
    block written (sparse_ok also spares GDAL's check that the disk could hold the full raster).
    """
    p = str(tmp_path / name)
    prof = dict(
        driver="GTiff", width=width, height=height, count=1, dtype="float32", crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.001, 0.001), nodata=-9999.0,
    )  # fmt: skip
    if tiled:
        prof.update(tiled=True, blockxsize=512, blockysize=512, sparse_ok=True)
    with open_raster(p, "w", **prof) as ds:
        if tiled:
            ds.write(
                np.zeros((16, 16), "float32"),
                1,
                window=rasterio.windows.Window(0, 0, 16, 16),
            )
        else:
            ds.write(np.zeros((height, width), "float32"), 1)
    return p


def pixel_box(c0, r0, c1, r1):
    return shapely.wkb.dumps(
        box(10.0 + c0 * 0.001, 50.0 - r1 * 0.001, 10.0 + c1 * 0.001, 50.0 - r0 * 0.001)
    )


def plan(path, **options):
    return RasterGbxReader(
        {"path": path, "virtualTiles": "false", **options}
    ).partitions()


def windows(parts):
    return [p.window for p in parts]


def test_a_clip_polygon_is_one_partition_with_its_window_and_the_clip(tmp_path):
    p = raster(tmp_path)
    parts = plan(
        p,
        clipPolygons=[pixel_box(100, 50, 300, 250), pixel_box(400, 400, 500, 500)],
        clipCrs="EPSG:4326",
    )
    assert len(parts) == 2 and parts[0].window == (100, 50, 200, 200)
    assert parts[0].clip_polygon is not None and parts[0].clip_crs == "EPSG:4326"


def test_a_polygon_that_misses_the_raster_is_skipped(tmp_path):
    assert (
        plan(
            raster(tmp_path),
            clipPolygons=[shapely.wkb.dumps(box(100.0, 10.0, 101.0, 11.0))],
            clipCrs="EPSG:4326",
        )
        == []
    )


def test_an_srid_embedded_in_the_polygon_beats_clip_crs(tmp_path):
    wkb = shapely.wkb.dumps(
        set_srid(box(10.1, 49.8, 10.2, 49.9), 4326), include_srid=True
    )
    (part,) = plan(raster(tmp_path), clipPolygons=[wkb], clipCrs="EPSG:3857")
    assert part.clip_crs == "EPSG:4326"


# (raster size, options) -> the windows planned
WINDOWS = {
    "window_overhanging_the_raster_is_clipped_to_it": ((200, 200), {"windows": [(0, 0, 256, 256), (50, 50, 100, 100)]}, [(0, 0, 200, 200), (50, 50, 100, 100)]),
    "window_outside_the_raster_is_skipped": ((200, 200), {"windows": [(500, 500, 10, 10)]}, []),
    "tile_grid": ((512, 512), {"tileSize": "256"}, [(0, 0, 256, 256), (256, 0, 256, 256), (0, 256, 256, 256), (256, 256, 256, 256)]),
    "tile_overlap_makes_a_3x3_grid_of_step_192": ((512, 512), {"tileSize": "256", "overlapPercent": "25"}, [(c, r, min(256, 512 - c), min(256, 512 - r)) for r in (0, 192, 384) for c in (0, 192, 384)]),
    "tile_larger_than_the_raster_is_the_raster": ((300, 200), {"tileSize": "512"}, [(0, 0, 300, 200)]),
}  # fmt: skip


@pytest.mark.parametrize("size,opts,want", WINDOWS.values(), ids=WINDOWS.keys())
def test_windows_and_tiles_are_planned_inside_the_raster(tmp_path, size, opts, want):
    parts = plan(raster(tmp_path, *size), **opts)
    assert all(pt.clip_polygon is None for pt in parts)
    assert sorted(windows(parts)) == sorted(want)


def test_a_materialized_tile_over_the_memory_guard_is_refused_but_a_virtual_one_is_planned(
    tmp_path,
):
    p = raster(
        tmp_path, 50000, 50000, tiled=True
    )  # ~10 GB decoded, ~6.4 GB per 40000 x 40000 tile
    with pytest.raises(ValueError, match="tileSize"):
        plan(p, tileSize="40000")
    assert len(plan(p, tileSize="40000", virtualTiles="true")) >= 1


# (virtualTiles stays on) raster size, sizeInMB -> the partitions
def test_a_budget_splits_a_virtual_raster_into_virtual_windows(tmp_path):
    parts = plan(
        raster(tmp_path, 1000, 1000), virtualTiles="true", sizeInMB="1"
    )  # ~4 MB decoded
    assert len(parts) > 1 and all(
        pt.emit_virtual and pt.window is not None and not pt.is_whole for pt in parts
    )
    assert all(pt.clip_polygon is None for pt in parts)


@pytest.mark.parametrize(
    "size,mib",
    [((1000, 1000), None), ((100, 100), "1")],
    ids=["no_budget", "fits_the_budget"],
)
def test_a_virtual_raster_without_a_split_is_one_lazy_whole_file_partition(
    tmp_path, size, mib
):
    opts = {"sizeInMB": mib} if mib else {}
    (part,) = plan(raster(tmp_path, *size), virtualTiles="true", **opts)
    assert part.emit_virtual and part.is_whole and part.window is None
