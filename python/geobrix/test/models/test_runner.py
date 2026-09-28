import numpy as np

from databricks.labs.gbx.models import runner

from .conftest import _straddling_object, _two_color_objects


def _fake_two_boxes(image):
    m = np.zeros(image.shape[:2], np.int32)
    m[2:6, 2:6] = 1  # object A
    m[2:6, 10:14] = 2  # object B
    return m


def test_segment_raster_returns_polygons(synthetic_rgb_tile):
    df = runner.segment_raster(
        synthetic_rgb_tile,
        segmenter=_fake_two_boxes,
        gpus=1,
        tile_px=64,
        overlap=0,
        min_area=1.0,
    )
    assert set(df.columns) >= {"label", "geom", "score"}
    assert len(df) == 2  # two objects vectorized


def test_all_nodata_tile_yields_no_polygons(nodata_rgb_tile):
    df = runner.segment_raster(
        nodata_rgb_tile,
        segmenter=lambda im: np.zeros(im.shape[:2], np.int32),
        gpus=1,
        tile_px=64,
        overlap=0,
    )
    assert len(df) == 0  # Review Focus: empty tile, no spurious polygon


def test_seam_overlap_merges_split_object(wide_rgb_tile):
    # object straddles the tile seam; overlap merge must yield ONE polygon, not two
    df = runner.segment_raster(
        wide_rgb_tile,
        segmenter=_straddling_object,
        gpus=1,
        tile_px=32,
        overlap=16,
        min_area=1.0,
    )
    assert len(df) == 1  # Review Focus: seam double-count


def test_single_and_distributed_agree(multi_chip_rgb_tile):
    # multi_chip_rgb_tile + tile_px=32/overlap=0 plans THREE chips (see conftest), with
    # objects in chip 0 and chip 2 and nothing in chip 1 -- this is the multi-chip,
    # multi-GPU aggregation property the Review Focus calls out: gpus=1 (all chips on
    # device 0) and gpus=4 (chips fanned across up to 4 device slots) must recombine
    # every chip's result into the SAME set of polygons, not just the same count.
    a = runner.segment_raster(
        multi_chip_rgb_tile, segmenter=_two_color_objects, gpus=1, tile_px=32, overlap=0
    )
    b = runner.segment_raster(
        multi_chip_rgb_tile, segmenter=_two_color_objects, gpus=4, tile_px=32, overlap=0
    )
    assert len(a) == len(b) == 2  # both objects present, from two different chips
    # Review Focus: scheduling != different results -- compare the actual resulting
    # geometries (sorted, since row order is not a documented guarantee), not just a
    # count that a dropped-and-duplicated pair of chips could coincidentally match.
    assert sorted(a["geom"].tolist()) == sorted(b["geom"].tolist())
