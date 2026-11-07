"""The value-level fingerprints of the levels leg: the same values in another encoding hash alike, other values do not."""

import numpy as np
import pytest

from databricks.labs.gbx.bench import levels_family as fam
from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import value_fp as vfp

rasterio = pytest.importorskip("rasterio")


def _tif(arr, **profile):
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    prof = dict(
        driver="GTiff", width=arr.shape[-1], height=arr.shape[-2], count=arr.shape[0],
        dtype=str(arr.dtype), crs="EPSG:4326", transform=from_origin(0, 64, 1, 1), nodata=-1.0,
    )  # fmt: skip
    prof.update(profile)
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(arr)
        return mf.read()


def _field():
    rng = np.random.default_rng(3)
    a = rng.random((1, 64, 64)).astype("float32")
    a[0, :4, :4] = -1.0  # nodata
    return a


def test_a_raster_hashes_by_its_pixels_not_its_codec_tiling_or_overviews():
    a = _field()
    plain = _tif(a)
    zstd_tiled = _tif(a, compress="zstd", tiled=True, blockxsize=16, blockysize=16)
    deflate = _tif(a, compress="deflate", predictor=3)
    assert (
        len(plain) != len(zstd_tiled) != len(deflate)
    )  # the length fingerprint tells them apart
    assert vfp.raster_fp(plain) == vfp.raster_fp(zstd_tiled) == vfp.raster_fp(deflate)


def test_a_raster_with_another_pixel_nodata_cell_or_grid_hashes_apart():
    a = _field()
    base = vfp.raster_fp(_tif(a))
    b = a.copy()
    b[0, 30, 30] += 0.01
    assert vfp.raster_fp(_tif(b)) != base  # a pixel
    c = a.copy()
    c[0, 10, 10] = -1.0
    assert vfp.raster_fp(_tif(c)) != base  # a nodata cell
    from rasterio.transform import from_origin

    assert (
        vfp.raster_fp(_tif(a, transform=from_origin(1, 64, 1, 1))) != base
    )  # the grid


def test_a_raster_with_sub_precision_noise_hashes_alike_and_a_real_gap_does_not():
    a = _field().astype("float64")
    noisy = a + 1e-13
    assert vfp.raster_fp(_tif(a)) == vfp.raster_fp(
        _tif(noisy)
    )  # kernels differ by rounding noise
    approx = a * 1.05  # GDAL's approximate proximity against the exact one: up to ~6%
    assert vfp.raster_fp(_tif(a)) != vfp.raster_fp(_tif(approx))


def test_a_polygon_hashes_alike_from_any_vertex_and_orientation():
    from shapely import wkb
    from shapely.geometry import Polygon

    ring = [(0, 0), (0, 4), (6, 4), (6, 0), (0, 0)]
    a = Polygon(ring)
    b = Polygon(ring[2:-1] + ring[:2] + [ring[2]])  # the same ring from another vertex
    c = Polygon(list(reversed(ring)))  # and the other way round
    assert wkb.dumps(a) != wkb.dumps(b)
    assert (
        vfp.wkb_fp(wkb.dumps(a)) == vfp.wkb_fp(wkb.dumps(b)) == vfp.wkb_fp(wkb.dumps(c))
    )
    assert vfp.wkb_fp(wkb.dumps(a)) != vfp.wkb_fp(
        wkb.dumps(Polygon([(0, 0), (0, 4), (6, 5), (6, 0)]))
    )
    bow = Polygon(
        [(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)]
    )  # invalid: the bench's repair input
    assert not bow.is_valid and vfp.wkb_fp(wkb.dumps(bow)) == vfp.wkb_fp(wkb.dumps(bow))


def test_doubles_hash_alike_to_the_last_bits_and_apart_beyond_them():
    x = [0.4875086306277067, 0.4228135583360952]
    assert vfp.doubles_fp(x) == vfp.doubles_fp([x[0] - 1.5e-14, x[1] + 1.2e-14])
    assert vfp.doubles_fp(x) != vfp.doubles_fp([x[0] + 1e-3, x[1]])


def test_a_vector_tile_hashes_by_its_extent_and_features():
    mvt = pytest.importorskip("mapbox_vector_tile")
    feats = [{"geometry": f"POINT({i} {i})", "properties": {"id": i}} for i in range(5)]
    full = mvt.encode(
        [{"name": "l", "features": feats}], default_options={"extents": 4096}
    )
    again = mvt.encode(
        [{"name": "l", "features": feats[::-1]}], default_options={"extents": 4096}
    )
    fewer = mvt.encode(
        [{"name": "l", "features": feats[:4]}], default_options={"extents": 4096}
    )
    coarse = mvt.encode(
        [{"name": "l", "features": feats}], default_options={"extents": 128}
    )
    assert vfp.mvt_fp(full) == vfp.mvt_fp(again)
    assert (
        len({vfp.mvt_fp(b) for b in (full, fewer, coarse)}) == 3
    )  # a dropped feature or a coarser extent shows


def test_every_decoded_representative_has_a_value_query_for_each_of_its_levels():
    """The families whose length fingerprint differed between levels (G8) carry a `vsql` on every L1/L3 form, never on a floor."""
    want = {r.fn for r in fam.REPS if r.vfp}
    assert want >= {
        "gbx_rst_boundingbox", "gbx_rst_avg", "gbx_rst_proximity", "gbx_rst_cog_convert", "gbx_rst_binpoints",
        "gbx_rst_align_to", "gbx_st_asmvt", "gbx_rst_bng_rasterize_agg",
    }  # fmt: skip
    for family, forms in lf.FORMS.items():
        for f in forms:
            if f.fn in want and f.level in ("L1", "L3") and "@" in f.variant:
                if f.variant.startswith("floor"):
                    assert not f.vsql, (family, f.variant)
                else:
                    assert f.vsql and "gbx_bench_vfp_" in f.vsql, (
                        family,
                        f.fn,
                        f.variant,
                    )
    t = [
        f
        for f in lf.FORMS["S"]
        if f.fn == "gbx_rst_transformcrs" and f.variant != "floor"
    ]
    assert t and all("gbx_bench_vfp_raster" in f.vsql for f in t)
