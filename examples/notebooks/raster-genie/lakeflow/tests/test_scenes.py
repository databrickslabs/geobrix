"""stack_scene: real small rasters in, one master COG out."""

import numpy as np
import pytest

rasterio = pytest.importorskip("rasterio")
from land.scenes import (  # noqa: E402
    S2_BANDS,
    STACK_VERSION,
    choose_scene_pairs,
    is_current,
    stack_scene,
    stack_version,
)
from rasterio.enums import MaskFlags  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402

CRS = "EPSG:32614"
SIZE_10M = 1100  # large enough that a 512 block grid and 4 overview levels are real
SIZE_20M = SIZE_10M // 2


def _write(path, band, size, res, dtype="uint16"):
    rng = np.random.default_rng(sum(map(ord, band)))
    if band == "SCL":
        data = rng.choice([4, 5, 6, 7], size=(size, size)).astype(
            dtype
        )  # classes that are NOT masked
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
        assert ds.tags(ns="IMAGE_STRUCTURE").get("COMPRESSION", "").lower() == "zstd"
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
    # bilinear blends neighbouring 20 m pixels: most output pixels differ from a nearest-neighbour 2x replicate
    with rasterio.open(band_paths["B11"]) as src:
        replicate = np.repeat(np.repeat(src.read(1), 2, axis=0), 2, axis=1)
    assert (b11 != replicate).mean() > 0.5
    # SCL is written with nearest neighbour at full resolution: only source class ids, no blended values
    assert set(np.unique(scl)) <= {0, 4, 5, 6, 7}


def test_10m_bands_are_copied_unchanged(band_paths, tmp_path):
    out = stack_scene(band_paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        for idx, name in ((1, "B02"), (2, "B03"), (3, "B04"), (4, "B08")):
            with rasterio.open(band_paths[name]) as src:
                # outside the SCL nodata corner (10 x 10 px at 10 m), where the scene is masked
                assert (ds.read(idx)[10:, 10:] == src.read(1)[10:, 10:]).all(), name


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


def _scene_with_scl(tmp_path, scl_cells):
    """Band files whose 20 m SCL is class 4 everywhere but at `scl_cells` ({(row, col): class} at 20 m)."""
    paths = {}
    for b in ("B02", "B03", "B04", "B08"):
        paths[b] = _write(tmp_path / f"{b}.tif", b, SIZE_10M, 10.0)
    for b in ("B11", "B12"):
        paths[b] = _write(tmp_path / f"{b}.tif", b, SIZE_20M, 20.0)
    scl = np.full((SIZE_20M, SIZE_20M), 4, dtype="uint8")
    for (r, c), cls in scl_cells.items():
        scl[r, c] = cls
    profile = dict(
        driver="GTiff",
        width=SIZE_20M,
        height=SIZE_20M,
        count=1,
        dtype="uint8",
        crs=CRS,
        transform=from_origin(500000.0, 4000000.0, 20.0, 20.0),
        nodata=0,
    )
    with rasterio.open(tmp_path / "SCL.tif", "w", **profile) as dst:
        dst.write(scl, 1)
    paths["SCL"] = str(tmp_path / "SCL.tif")
    return paths


@pytest.mark.parametrize("cls", [0, 1, 3, 8, 9, 10, 11])
def test_masked_scl_classes_become_nodata_in_every_reflectance_band(tmp_path, cls):
    paths = _scene_with_scl(tmp_path, {(100, 100): cls})
    out = stack_scene(paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        for idx in (1, 2, 3, 4, 5, 6):
            assert (ds.read(idx)[200:202, 200:202] == 0).all(), (
                idx,
                cls,
            )  # the 2 x 2 of the 20 m pixel
            assert (ds.read(idx)[210:212, 210:212] > 0).all(), (
                idx,
                cls,
            )  # a neighbour is untouched
        assert (ds.read(7)[200:202, 200:202] == cls).all()  # SCL is written as it is


@pytest.mark.parametrize("cls", [2, 4, 5, 6, 7])
def test_clear_scl_classes_stay_valid(tmp_path, cls):
    paths = _scene_with_scl(tmp_path, {(100, 100): cls})
    out = stack_scene(paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        assert (ds.read(3)[200:202, 200:202] > 0).all()


def test_mask_scl_false_leaves_the_reflectance_alone(tmp_path):
    paths = _scene_with_scl(tmp_path, {(100, 100): 9})
    out = stack_scene(paths, str(tmp_path / "scene.tif"), mask_scl=False)
    with rasterio.open(out) as ds:
        assert (ds.read(3)[200:202, 200:202] > 0).all()


def test_reflectance_overview_is_the_mean_of_its_valid_pixels(tmp_path):
    # one 20 m pixel (2 x 2 at 10 m) masked inside a 4 x 4 block of the 4x overview: 12 valid, 4 nodata
    paths = _scene_with_scl(tmp_path, {(100, 100): 8})
    out = stack_scene(paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        assert ds.overviews(1) == [2, 4, 8, 16]
        full = ds.read(3).astype("float64")
        block = full[200:204, 200:204]
        valid = block[block > 0]
        assert len(valid) == 12
        ovr = ds.read(
            3, out_shape=(ds.height // 4, ds.width // 4)
        )  # served by the 4x overview
        assert (
            abs(float(ovr[50, 50]) - valid.mean()) <= 1.0
        )  # the mean of the valid pixels, not of all 16 (25% lower)
        assert ovr[50, 50] > 0.9 * valid.mean()


def test_an_overview_pixel_is_nodata_only_where_every_pixel_it_covers_is(tmp_path):
    cells = {
        (100 + i, 100 + j): 9 for i in range(2) for j in range(2)
    }  # a 4 x 4 block at 10 m, all masked
    paths = _scene_with_scl(tmp_path, cells)
    out = stack_scene(paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        ovr = ds.read(3, out_shape=(ds.height // 4, ds.width // 4))
        assert ovr[50, 50] == 0 and ovr[51, 51] > 0


def test_stats_count_the_masked_share_of_the_data_pixels(tmp_path):
    cells = {
        (10, 10): 9,
        (10, 11): 3,
        (20, 20): 0,
    }  # two masked 20 m pixels and one nodata one
    paths = _scene_with_scl(tmp_path, cells)
    stats = {}
    stack_scene(paths, str(tmp_path / "scene.tif"), stats=stats)
    assert (
        stats["masked_px"] == 2 * 4
        and stats["data_px"] == (SIZE_20M * SIZE_20M - 1) * 4
    )
    assert stats["masked_pct"] == round(
        100.0 * stats["masked_px"] / stats["data_px"], 2
    )


def _row(tile, date, orbit, cloud, nodata=0.0, item=None):
    return dict(
        item_id=item or f"{tile}_{date}_R{orbit}",
        mgrs_tile=tile,
        date=date,
        orbit=orbit,
        cloud_pct=cloud,
        nodata_pct=nodata,
    )


def test_pre_scenes_share_the_date_that_serves_most_tiles():
    pre = [
        _row("A", "2024-02-19", "055", 1.0),
        _row("B", "2024-02-19", "055", 2.0),
        _row("A", "2024-02-24", "055", 0.0),
        _row("B", "2024-02-14", "055", 0.0),  # cleaner, but on other days
    ]
    post = [_row("A", "2024-03-10", "055", 0.0), _row("B", "2024-03-10", "055", 0.0)]
    got = choose_scene_pairs(pre, post, max_cloud=20, max_nodata=5)
    assert {t: r["date"] for t, r in got["pre"].items()} == {
        "A": "2024-02-19",
        "B": "2024-02-19",
    }


def test_a_tie_in_dates_goes_to_the_day_closest_to_the_fire():
    pre = [_row("A", "2024-02-10", "055", 0.0), _row("A", "2024-02-20", "055", 0.0)]
    post = [_row("A", "2024-03-05", "055", 0.0), _row("A", "2024-03-15", "055", 0.0)]
    got = choose_scene_pairs(pre, post, 20, 5)
    assert (
        got["pre"]["A"]["date"] == "2024-02-20"
        and got["post"]["A"]["date"] == "2024-03-05"
    )


def test_post_scene_is_on_the_pre_scenes_relative_orbit_when_there_is_one():
    pre = [_row("A", "2024-02-19", "012", 0.0)]
    post = [_row("A", "2024-03-05", "055", 0.0), _row("A", "2024-03-12", "012", 5.0)]
    got = choose_scene_pairs(pre, post, 20, 5)
    assert got["post"]["A"]["orbit"] == "012" and got["orbit_mismatch"] == []


def test_a_post_orbit_that_differs_is_used_and_reported_when_the_pre_orbit_has_none():
    pre = [_row("A", "2024-02-19", "012", 0.0)]
    post = [_row("A", "2024-03-05", "055", 0.0)]
    got = choose_scene_pairs(pre, post, 20, 5)
    assert got["post"]["A"]["orbit"] == "055" and got["orbit_mismatch"] == ["A"]


def test_cloudy_and_cut_scenes_are_out_but_an_unknown_nodata_figure_is_kept():
    pre = [
        _row("A", "2024-02-19", "055", 30.0),
        _row("B", "2024-02-19", "055", 1.0, nodata=54.0),
        _row("C", "2024-02-19", "055", 1.0, nodata=None),
    ]
    post = [_row(t, "2024-03-10", "055", 0.0) for t in "ABC"]
    got = choose_scene_pairs(pre, post, 20, 5)
    assert sorted(got["pre"]) == ["C"] and got["dropped"] == ["A", "B"]


def test_least_cloud_ties_by_whole_percent_then_exact_cloud_then_item_id():
    rs = [
        _row("A", "2024-02-19", "055", 0.4, item="b"),
        _row("A", "2024-02-19", "055", 0.2, item="a"),
    ]
    got = choose_scene_pairs(rs, [_row("A", "2024-03-10", "055", 0.0)], 20, 5)
    assert got["pre"]["A"]["item_id"] == "a"


def test_master_cog_carries_the_stack_version_tag(band_paths, tmp_path):
    out = stack_scene(band_paths, str(tmp_path / "scene.tif"))
    with rasterio.open(out) as ds:
        assert ds.tags()["GBX_STACK_VERSION"] == str(STACK_VERSION)
        assert ds.tags(ns="IMAGE_STRUCTURE").get("LAYOUT") == "COG"  # still a COG
    assert stack_version(out) == STACK_VERSION
    assert is_current(out)


def test_a_master_without_the_tag_or_with_a_lower_one_is_not_current(
    band_paths, tmp_path
):
    out = stack_scene(band_paths, str(tmp_path / "scene.tif"))
    old = tmp_path / "old.tif"
    with rasterio.open(out) as src:  # a pre-fix master: same pixels, no tag
        profile = src.profile
        data = src.read()
    with rasterio.open(old, "w", **profile) as dst:
        dst.write(data)
    assert stack_version(str(old)) == 0
    assert not is_current(str(old))
    with rasterio.open(old, "r+") as dst:
        dst.update_tags(GBX_STACK_VERSION=str(STACK_VERSION - 1))
    assert stack_version(str(old)) == STACK_VERSION - 1
    assert not is_current(str(old))
    with rasterio.open(old, "r+") as dst:
        dst.update_tags(GBX_STACK_VERSION="garbage")
    assert stack_version(str(old)) == 0


def test_a_missing_file_is_not_current(tmp_path):
    assert not is_current(str(tmp_path / "nope.tif"))


@pytest.fixture(scope="module")
def local_spark(tmp_path_factory):
    pyspark_sql = pytest.importorskip("pyspark.sql")
    spark = (
        pyspark_sql.SparkSession.builder.master("local[1]")
        .appName("table-stack-version")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("wh")))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_table_stack_version_reads_the_property_value(local_spark):
    from land.scenes import STACK_VERSION, table_stack_version

    local_spark.sql(
        f"CREATE TABLE tsv_set (a INT) USING parquet TBLPROPERTIES ('gbx.stack_version' = '{STACK_VERSION}')"
    )
    local_spark.sql("CREATE TABLE tsv_unset (a INT) USING parquet")
    assert table_stack_version(local_spark, "tsv_set") == str(STACK_VERSION)
    assert table_stack_version(local_spark, "tsv_unset") is None
    local_spark.sql("ALTER TABLE tsv_unset SET TBLPROPERTIES ('gbx.stack_version' = '1')")
    assert table_stack_version(local_spark, "tsv_unset") == "1"
