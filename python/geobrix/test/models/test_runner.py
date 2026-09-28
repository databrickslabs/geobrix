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


def test_default_segmenter_binds_per_device(monkeypatch, multi_chip_rgb_tile):
    """FIX: the default (segmenter=None) path must bind each chip's inference to its
    OWN assigned gpu_id -- a per-device handle, cached (not rebuilt per chip), never
    one handle shared across every device. Patches the module-level
    `_default_geosam_factory` seam (no torch/GPU needed) and runs the multi_chip
    fixture (3 chips) at gpus=2 -- fewer devices than chips, so at least one device
    must be reused across more than one chip, which is what exercises caching.
    """
    builds = []  # gpu_id at each factory (handle-build) call
    usages = []  # gpu_id at each per-chip inference call

    def fake_factory(gpu_id):
        builds.append(gpu_id)

        def _segment(image):
            usages.append(gpu_id)
            return _two_color_objects(image)

        return _segment

    monkeypatch.setattr(runner, "_default_geosam_factory", fake_factory)

    df = runner.segment_raster(multi_chip_rgb_tile, gpus=2, tile_px=32, overlap=0)

    assert len(usages) == 3  # one inference call per chip
    # (a) the factory is invoked once per DISTINCT gpu_id, never twice for the same one
    assert len(builds) == len(set(builds))
    # (b) _run_one threaded the REAL per-chip gpu_id through -- not a constant device
    assert len(set(builds)) > 1
    # caching: fewer builds than inference calls means a device handle was reused
    # across chips instead of being rebuilt every time
    assert len(builds) < len(usages)
    # every inference call's gpu_id is one the factory actually built a handle for
    assert set(usages) <= set(builds)
    # (c) per-device fan-out still recombines into the same two real-world objects
    assert len(df) == 2
