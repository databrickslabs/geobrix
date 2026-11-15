import numpy as np
import pytest
from shapely.geometry import box

from databricks.labs.gbx.models import runner

from .conftest import GREEN, RED, _straddling_object, _two_color_objects


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


def test_area_m2_geographic_is_metric_not_degrees(geographic_rgb_tile):
    """_area_m2 on a geographic (EPSG:4326) polygon returns SQUARE METERS (geodesic),
    NOT the ~1e-7 square-DEGREE value geom.area gives -- the bug that silently dropped
    every polygon from a 4326 orthomosaic under a metric min_area."""
    crs = geographic_rgb_tile.crs  # EPSG:4326 (fixture CRS -- no bare CRS.from_*)
    poly = box(-77.968, 43.233, -77.968 + 3e-4, 43.233 + 3e-4)  # ~30px @ 1e-5 deg
    assert poly.area < 1e-6  # raw degrees: astronomically under any metric threshold
    assert 500 < runner._area_m2(poly, crs) < 1500  # metric: hundreds of m^2 at lat 43


def test_area_m2_projected_matches_planar(synthetic_rgb_tile):
    """Projected CRS (meter-based EPSG:32633) -> planar .area unchanged, so the
    projected fixtures/tests keep their exact meaning under the metric-area fix."""
    crs = synthetic_rgb_tile.crs  # EPSG:32633
    assert runner._area_m2(box(0, 0, 10, 10), crs) == 100.0


def test_min_area_metric_keeps_geographic_object(geographic_rgb_tile):
    """Integration regression: a several-hundred-m^2 object in a 4326 tile survives
    min_area=10.0 (m^2). Under the old CRS-unit filter its ~1e-7 sq-degree area was
    < 10 and it was dropped -- the 0-objects bug on the real orthomosaic COG."""
    df = runner.segment_raster(
        geographic_rgb_tile,
        segmenter=_straddling_object,
        gpus=1,
        tile_px=64,
        overlap=0,
        min_area=10.0,
    )
    assert len(df) == 1


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


# --- weights= : a checkpoint staged on a Volume (no CDN download) ---------------------


@pytest.fixture
def fake_geosam(monkeypatch):
    """Fake the model load (no torch / SamGeo / GPU in CI). Records every
    ``load_geosam`` call's kwargs; ``segment`` draws two boxes."""
    import sys
    import types

    from databricks.labs.gbx.models import geosam

    calls = []

    def fake_load(**kwargs):
        calls.append(kwargs)
        return object()

    fake_torch = types.SimpleNamespace(
        cuda=types.SimpleNamespace(set_device=lambda i: None)
    )
    monkeypatch.setitem(sys.modules, "torch", fake_torch)
    monkeypatch.setattr(geosam, "load_geosam", fake_load)
    monkeypatch.setattr(geosam, "segment", lambda handle, image: _fake_two_boxes(image))
    return calls


@pytest.fixture
def checkpoint(tmp_path):
    p = tmp_path / "sam_vit_h_4b8939.pth"
    p.write_bytes(b"weights")
    return str(p)


def test_weights_default_none_keeps_samgeo_download(fake_geosam, synthetic_rgb_tile):
    runner.segment_raster(synthetic_rgb_tile, gpus=1, tile_px=64, overlap=0)
    assert fake_geosam and all(c["weights"] is None for c in fake_geosam)


def test_weights_passed_through_to_load_geosam(
    fake_geosam, synthetic_rgb_tile, checkpoint
):
    df = runner.segment_raster(
        synthetic_rgb_tile,
        weights=checkpoint,
        gpus=1,
        tile_px=64,
        overlap=0,
        min_area=1.0,
    )
    assert len(df) == 2
    assert [c["weights"] for c in fake_geosam] == [checkpoint]
    assert fake_geosam[0]["device"] == "cuda:0"


def test_weights_dbfs_uri_is_normalised(fake_geosam, synthetic_rgb_tile, checkpoint):
    runner.segment_raster(
        synthetic_rgb_tile, weights=f"dbfs:{checkpoint}", gpus=1, tile_px=64, overlap=0
    )
    assert [c["weights"] for c in fake_geosam] == [checkpoint]


def test_weights_with_segmenter_is_refused(synthetic_rgb_tile, checkpoint):
    with pytest.raises(ValueError, match=r"weights.*segmenter") as e:
        runner.segment_raster(
            synthetic_rgb_tile,
            weights=checkpoint,
            segmenter=_two_color_objects,
            gpus=1,
            tile_px=64,
        )
    assert "Fix:" in str(e.value)


def test_missing_weights_file_is_refused(fake_geosam, synthetic_rgb_tile, tmp_path):
    missing = str(tmp_path / "nope.pth")
    with pytest.raises(FileNotFoundError, match="nope.pth") as e:
        runner.segment_raster(synthetic_rgb_tile, weights=missing, gpus=1, tile_px=64)
    assert "Fix:" in str(e.value)
    assert fake_geosam == []  # refused before any model load


# --- seam merge: only pieces of ONE object cut by a chip seam are merged ----------------


def _seg(ds, **kw):
    kw.setdefault("segmenter", _two_color_objects)
    kw.setdefault("gpus", 1)
    kw.setdefault("min_area", 1.0)
    return runner.segment_raster(ds, **kw)


def test_touching_objects_in_one_chip_stay_separate(painted_tile):
    # (a) two side-by-side objects sharing an edge inside ONE chip are two objects:
    # touching is not a seam, and one chip's fragments are never merged together.
    ds = painted_tile(
        100,
        100,
        [
            (slice(20, 80), slice(10, 50), RED),
            (slice(20, 80), slice(50, 90), GREEN),
        ],
    )
    assert len(_seg(ds, tile_px=100, overlap=0)) == 2


def test_object_split_by_abutting_chips_merges(painted_tile):
    # (b) one object cut by the seam between two abutting (overlap=0) chips: the two
    # fragments share a boundary segment along the seam line -> one object.
    ds = painted_tile(64, 32, [(slice(8, 24), slice(20, 44), RED)])
    df = _seg(ds, tile_px=32, overlap=0)
    assert len(df) == 1
    import shapely.wkb

    assert shapely.wkb.loads(bytes(df["geom"][0])).area == 16 * 24


def test_objects_touching_at_a_seam_corner_stay_separate(painted_tile):
    # (b') two objects in abutting chips that meet only at a point on the seam are
    # not one object cut by the seam.
    ds = painted_tile(
        64,
        32,
        [
            (slice(8, 16), slice(24, 32), RED),
            (slice(16, 24), slice(32, 40), GREEN),
        ],
    )
    assert len(_seg(ds, tile_px=32, overlap=0)) == 2


def test_object_split_by_overlapping_chips_merges(painted_tile):
    # (c) tile_px=32, overlap=16% on a 48-wide raster -> chips at cols 0..32 and
    # 26..48 (a 6px overlap zone). An object across it is seen by both chips; the
    # fragments overlap with a positive area in the zone -> one object.
    ds = painted_tile(48, 20, [(slice(5, 15), slice(20, 40), RED)])
    df = _seg(ds, tile_px=32, overlap=16)
    assert len(df) == 1
    import shapely.wkb

    assert shapely.wkb.loads(bytes(df["geom"][0])).area == 10 * 20


def test_neighbours_in_an_overlap_zone_stay_separate(painted_tile):
    # (c') two touching objects whose shared edge (x=29) lies inside the overlap zone:
    # each is merged with its own copy from the other chip, never with its neighbour.
    ds = painted_tile(
        48,
        20,
        [
            (slice(5, 15), slice(20, 29), RED),
            (slice(5, 15), slice(29, 40), GREEN),
        ],
    )
    assert len(_seg(ds, tile_px=32, overlap=16)) == 2


def _black_and_red(image):
    """Fake segmenter that also segments the black (all-zero) frame as an object, as
    SAM's automatic masks do on a NoData border."""
    m = np.zeros(image.shape[:2], np.int32)
    m[np.all(image[..., :3] == 0, axis=-1)] = 1
    m[np.all(image[..., :3] == [255, 0, 0], axis=-1)] = 2
    return m


def _border(n=64, w=8):
    inner = np.zeros((n, n), bool)
    inner[w:-w, w:-w] = True
    return inner


@pytest.mark.parametrize("nodata", [None, 0])
def test_nodata_border_yields_no_object(painted_tile, nodata):
    # (d) a black border (RGB all 0) is NoData -- declared (nodata=0) or not (all
    # bands 0, no mask): a segmenter mask over it is not an object.
    ds = painted_tile(
        64,
        64,
        [
            (slice(0, 64), slice(0, 8), (0, 0, 0)),
            (slice(0, 64), slice(56, 64), (0, 0, 0)),
            (slice(0, 8), slice(0, 64), (0, 0, 0)),
            (slice(56, 64), slice(0, 64), (0, 0, 0)),
            (slice(20, 30), slice(20, 30), RED),
        ],
        nodata=nodata,
    )
    df = _seg(ds, segmenter=_black_and_red, tile_px=64, overlap=0)
    assert len(df) == 1
    import shapely.wkb

    assert shapely.wkb.loads(bytes(df["geom"][0])).area == 100


def test_transparent_border_yields_no_object(painted_tile):
    # (d') an RGBA raster: alpha 0 marks the border NoData even though its RGB is
    # not black; the segmenter sees the three color bands only.
    seen = []

    def _seg_frame(image):
        seen.append(image.shape[-1])
        m = np.zeros(image.shape[:2], np.int32)
        m[np.all(image == [7, 7, 7], axis=-1)] = 1  # the (transparent) frame
        m[np.all(image == [255, 0, 0], axis=-1)] = 2
        return m

    frame = ~_border()
    # the frame's RGB is a non-black marker, so only alpha can mask it
    ds = painted_tile(
        64,
        64,
        [
            (slice(0, 64), slice(0, 8), (7, 7, 7)),
            (slice(0, 64), slice(56, 64), (7, 7, 7)),
            (slice(0, 8), slice(0, 64), (7, 7, 7)),
            (slice(56, 64), slice(0, 64), (7, 7, 7)),
            (slice(20, 30), slice(20, 30), RED),
        ],
        alpha=np.where(frame, 0, 255).astype("uint8"),
    )
    assert ds.colorinterp[-1].name == "alpha"
    df = _seg(ds, segmenter=_seg_frame, tile_px=64, overlap=0)
    assert seen == [3]
    assert len(df) == 1


def test_min_area_filters_after_the_seam_merge(painted_tile):
    # (e) min_area is judged on the MERGED object: each half of the split red object
    # (8x16 = 128 m^2) is under 200 m^2, the whole (256 m^2) is not; the 4 m^2 green
    # object is dropped.
    ds = painted_tile(
        64,
        32,
        [
            (slice(8, 24), slice(24, 40), RED),
            (slice(2, 4), slice(2, 4), GREEN),
        ],
    )
    df = _seg(ds, tile_px=32, overlap=0, min_area=200.0)
    assert len(df) == 1
    import shapely.wkb

    assert shapely.wkb.loads(bytes(df["geom"][0])).area == 256
