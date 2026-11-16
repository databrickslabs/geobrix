"""stack_scene: real small rasters in, one master COG out."""

import numpy as np
import pytest

rasterio = pytest.importorskip("rasterio")
from rasterio.enums import MaskFlags  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402

from land.scenes import S2_BANDS, stack_scene  # noqa: E402

CRS = "EPSG:32614"
SIZE_10M = 1100  # large enough that a 512 block grid and 4 overview levels are real
SIZE_20M = SIZE_10M // 2


def _write(path, band, size, res, dtype="uint16"):
    rng = np.random.default_rng(sum(map(ord, band)))
    if band == "SCL":
        data = rng.choice([4, 5, 8, 9], size=(size, size)).astype(dtype)
    else:
        data = rng.integers(1, 4000, size=(size, size)).astype(dtype)
    data[:5, :5] = 0  # a nodata corner
    profile = dict(
        driver="GTiff",
        width=size,
        height=size,
        count=1,
        dtype=dtype,
        crs=CRS,
        transform=from_origin(500000.0, 4000000.0, res, res),
        nodata=0,
    )
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)
    return str(path)


@pytest.fixture
def band_paths(tmp_path):
    paths = {}
    for b in ("B02", "B03", "B04", "B08"):  # 10 m
        paths[b] = _write(tmp_path / f"{b}.tif", b, SIZE_10M, 10.0)
    for b in ("B11", "B12", "SCL"):  # 20 m, half the size
        paths[b] = _write(
            tmp_path / f"{b}.tif",
            b,
            SIZE_20M,
            20.0,
            "uint16" if b != "SCL" else "uint8",
        )
    return paths


def test_output_is_a_cog_with_blocks_and_overviews(band_paths, tmp_path):
    out = stack_scene(band_paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        assert ds.overviews(1) == [2, 4, 8, 16]
        assert ds.block_shapes[0] == (512, 512)
        assert ds.profile["compress"].lower() == "zstd"
        assert ds.nodata == 0
        assert ds.tags(ns="IMAGE_STRUCTURE").get("LAYOUT") == "COG"


def test_no_internal_mask_band(band_paths, tmp_path):
    out = stack_scene(band_paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        for flags in ds.mask_flag_enums:
            assert MaskFlags.per_dataset not in flags
            assert MaskFlags.alpha not in flags


def test_seven_bands_with_descriptions(band_paths, tmp_path):
    out = stack_scene(band_paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        assert ds.count == 7
        assert tuple(ds.descriptions) == S2_BANDS
        assert ds.descriptions == ("B02", "B03", "B04", "B08", "B11", "B12", "SCL")


def test_20m_bands_resampled_to_the_10m_grid(band_paths, tmp_path):
    out = stack_scene(band_paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        assert (ds.height, ds.width) == (SIZE_10M, SIZE_10M)
        assert ds.res == (10.0, 10.0)
        b11 = ds.read(5)
        scl = ds.read(7)
    with rasterio.open(band_paths["B11"]) as src:
        # a 20 m pixel is a 2x2 block at 10 m: bilinear stays inside the source range
        lo, hi = src.read(1)[src.read(1) > 0].min(), src.read(1).max()
    assert b11[b11 > 0].min() >= lo and b11.max() <= hi
    # nearest keeps SCL a set of source class ids, no blended values
    assert set(np.unique(scl)) <= {0, 4, 5, 8, 9}


def test_10m_bands_are_copied_unchanged(band_paths, tmp_path):
    out = stack_scene(band_paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        for idx, name in ((1, "B02"), (2, "B03"), (3, "B04"), (4, "B08")):
            with rasterio.open(band_paths[name]) as src:
                assert (ds.read(idx) == src.read(1)).all(), name


def test_nodata_corner_stays_nodata(band_paths, tmp_path):
    out = stack_scene(band_paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        assert (ds.read(3)[:4, :4] == 0).all()  # reference band's own nodata corner


def test_missing_band_is_refused_with_a_fix(band_paths, tmp_path):
    del band_paths["B12"]
    with pytest.raises(ValueError, match="B12.*Fix"):
        stack_scene(band_paths, str(tmp_path / "scene.tif"))


def test_boa_offset_is_removed_from_reflectance_not_scl(band_paths, tmp_path):
    out = stack_scene(band_paths, str(tmp_path / "off.tif"), boa_offset=1000)
    plain = stack_scene(band_paths, str(tmp_path / "plain.tif"))
    with rasterio.open(out) as a, rasterio.open(plain) as b:
        ra, rb = a.read(3).astype("int32"), b.read(3).astype("int32")
        valid = rb > 1000
        assert (ra[valid] == rb[valid] - 1000).all()
        assert (ra[rb > 0] >= 1).all()  # a valid pixel never becomes nodata
        assert (a.read(7) == b.read(7)).all()  # SCL untouched
