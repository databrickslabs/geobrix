"""Unit tests for the bench GEOMETRY corpus.

Geometry-input raster functions (rst_clip / rst_rasterize / rst_dtmfromgeoms /
the geometry aggregators) need a deterministic, CRS-correct geometry corpus that
BOTH benchmark engines (pyrx + heavy) read identically. The bench previously
generated only raster TILES; this corpus derives geometry from a tile's bounds +
CRS so every geometry is in-extent for its source tile.

These tests assert the generator's contract via shapely on the WKB outputs:
  * deterministic     -> same seed produces byte-identical WKB
  * in-extent         -> every box/point lies within the source tile bounds
  * exact counts      -> n_boxes / n_points respected
  * WKB round-trips    -> shapely.wkb.loads parses every geometry
  * z-points carry Z  -> 3D points with a finite Z
and that the manifest persists + round-trips byte-identically (so a second
process -- the heavy tier -- reads the SAME geometry bytes).
"""

import numpy as np
import rasterio
import shapely.wkb

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench import manifest as m
from databricks.labs.gbx.bench import runner as rn
from databricks.labs.gbx.bench.spec import FnSpec

# A tile's bounds in CRS units (UTM-like metres) used across the tests.
_BOUNDS = (583000.0, 4507000.0, 583500.0, 4507500.0)  # (left, bottom, right, top)
_SRID = 32618


def _gen(seed=7, n_boxes=16, n_points=64, bounds=_BOUNDS, srid=_SRID):
    return dg.generate_geometry_corpus(
        bounds, srid, seed, n_boxes=n_boxes, n_points=n_points
    )


def test_geometry_corpus_is_deterministic_for_seed():
    a = _gen(seed=42)
    b = _gen(seed=42)
    assert [wkb for wkb, _ in a.boxes] == [wkb for wkb, _ in b.boxes]
    assert [wkb for wkb, _ in a.points] == [wkb for wkb, _ in b.points]
    assert a.zpoints == b.zpoints


def test_geometry_corpus_counts_are_as_requested():
    g = _gen(n_boxes=16, n_points=64)
    assert len(g.boxes) == 16
    assert len(g.points) == 64
    assert len(g.zpoints) == 64


def test_boxes_and_points_lie_within_tile_bounds():
    left, bottom, right, top = _BOUNDS
    g = _gen()
    for wkb, _ in g.boxes:
        geom = shapely.wkb.loads(wkb)
        gl, gb, gr, gt = geom.bounds
        assert gl >= left and gb >= bottom and gr <= right and gt <= top
    for wkb, _ in g.points:
        p = shapely.wkb.loads(wkb)
        assert left <= p.x <= right and bottom <= p.y <= top


def test_wkb_round_trips_via_shapely():
    g = _gen()
    assert all(shapely.wkb.loads(wkb).geom_type == "Polygon" for wkb, _ in g.boxes)
    assert all(shapely.wkb.loads(wkb).geom_type == "Point" for wkb, _ in g.points)
    assert all(shapely.wkb.loads(wkb).geom_type == "Point" for wkb in g.zpoints)


def test_zpoints_carry_a_finite_z():
    g = _gen()
    for wkb in g.zpoints:
        p = shapely.wkb.loads(wkb)
        assert p.has_z
        assert np.isfinite(p.z)


def test_each_box_and_point_has_a_deterministic_burn_value():
    a = _gen(seed=9)
    b = _gen(seed=9)
    assert [v for _, v in a.boxes] == [v for _, v in b.boxes]
    assert [v for _, v in a.points] == [v for _, v in b.points]
    assert all(np.isfinite(v) for _, v in a.boxes)
    assert all(np.isfinite(v) for _, v in a.points)


def test_zpoints_z_samples_the_source_tile_when_given_a_tile(tmp_path):
    # When fed an open tile (not just bounds), z-points sample the actual raster,
    # so their Z falls inside the tile's value range -- realistic elevation.
    b = dg.make_tile_bytes(
        tile_px=64, bands=1, dtype="float32", srid=_SRID, nodata_frac=0.0, seed=3
    )
    src = tmp_path / "src.tif"
    src.write_bytes(b)
    with rasterio.open(src) as ds:
        bounds = tuple(ds.bounds)
        srid = ds.crs.to_epsg()
        arr = ds.read(1)
        lo, hi = float(arr.min()), float(arr.max())
        g = dg.generate_geometry_corpus(ds, srid, seed=5)
    zs = [shapely.wkb.loads(wkb).z for wkb in g.zpoints]
    assert all(lo - 1e-6 <= z <= hi + 1e-6 for z in zs)
    # geometry is in the tile CRS / extent
    left, bottom, right, top = bounds
    for wkb, _ in g.points:
        p = shapely.wkb.loads(wkb)
        assert left <= p.x <= right and bottom <= p.y <= top


def test_manifest_round_trips_byte_identically(tmp_path):
    g = _gen(seed=11)
    gc = m.GeometryCorpus(
        seed=11,
        srid=_SRID,
        source_tile="size/t0.tif",
        sets={"t0": g},
    )
    path = tmp_path / "geometry.json"
    gc.write(path)
    back = m.GeometryCorpus.read(path)
    assert back.seed == 11
    assert back.srid == _SRID
    assert back.source_tile == "size/t0.tif"
    rg = back.sets["t0"]
    assert [wkb for wkb, _ in rg.boxes] == [wkb for wkb, _ in g.boxes]
    assert [v for _, v in rg.boxes] == [v for _, v in g.boxes]
    assert [wkb for wkb, _ in rg.points] == [wkb for wkb, _ in g.points]
    assert rg.zpoints == g.zpoints


def test_generate_corpus_persists_a_geometry_manifest(tmp_path):
    # Wiring: the corpus generation flow must produce geometry alongside tiles.
    dg.generate_corpus(
        out_dir=tmp_path,
        seed=11,
        tile_px=[32, 64],
        bands=[1],
        dtypes=["float32"],
        srids=[4326, 3857],
        nodata_fracs=[0.0],
        row_rows=3,
        row_tile_px=64,
        row_bands=1,
        row_dtype="float32",
    )
    geom_path = tmp_path / "geometry.json"
    assert geom_path.exists(), "geometry corpus persisted next to corpus.json"
    gc = m.GeometryCorpus.read(geom_path)
    assert gc.sets, "geometry sets recorded"
    # each set's source tile exists and its geometry is in that tile's extent
    for key, gset in gc.sets.items():
        src = tmp_path / gset.source_tile
        assert src.exists(), f"source tile for {key} exists"
        with rasterio.open(src) as ds:
            left, bottom, right, top = tuple(ds.bounds)
        for wkb, _ in gset.points:
            p = shapely.wkb.loads(wkb)
            assert left <= p.x <= right and bottom <= p.y <= top


def test_runner_feeds_geometry_set_to_geometry_input_kind(tmp_path):
    # The runner's input_kind == "geometry" branch loads the geometry corpus and
    # passes the tile's GeometrySet to core_fn(ds, args, geom). This smoke FnSpec
    # asserts it receives in-extent geometry whose count matches the corpus.
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=7,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=1,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )

    def _core(ds, args, geom):
        # geom is a GeometrySet whose points are in the tile extent.
        assert isinstance(geom, m.GeometrySet)
        left, bottom, right, top = tuple(ds.bounds)
        cnt = 0
        for wkb, _ in geom.points:
            p = shapely.wkb.loads(wkb)
            assert left <= p.x <= right and bottom <= p.y <= top
            cnt += 1
        return float(cnt)

    smoke = FnSpec(
        name="rst_geomsmoke",
        sql_name="gbx_rst_geomsmoke",
        category="geometry",
        modes=("pure-core",),
        args={},
        core_fn=_core,
        col_fn=lambda t, a: t,
        input_kind="geometry",
    )
    rows = rn.run_pure_core(
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=[smoke],
        run_id="t",
        warmup=1,
        measured=1,
        where="venv",
    )
    assert rows and all(r.status == "ok" for r in rows)
    assert all(r.output_fingerprint for r in rows)
    # Non-BNG fns skip the GB-only tile (role "bng_gb").
    _sweep = [t for t in corpus.size_sweep if t.role != "bng_gb"]
    assert len(rows) == len(_sweep)
