"""A representative rst_* per family on a VIRTUAL input tile: the same answer as on the tile's
own bytes, and the force-output params (``virtualize_dir``, ``materialize``) shape the result.

How a window is read by path, through a FILE reference and as bytes is the table in
``test_file_ref_equivalence.py``; this file is the UDF layer over it. The UDF closures are
called directly (``.func``) because the force-output variants take literal params, not Columns;
two Spark tests confirm the Column API on a v2-schema DataFrame.
"""

import os

import numpy as np
import pytest
import rasterio
import rasterio.io
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile
from databricks.labs.gbx.rasterx import functions as rx

from .conftest import make_geotiff_bytes


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _write_ramp_tif(path, *, side=8, ulx=0.0, uly=8.0, px=1.0, epsg=32633):
    """Write a single-band float32 ramp DEM (values 0..side*side-1)."""
    prof = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(ulx, uly, px, px),
        nodata=-9999.0,
    )
    data = np.arange(side * side, dtype="float32").reshape(side, side)
    with rasterio.open(path, "w", **prof) as ds:
        ds.write(data, 1)
    return data


def _virtual_row(path, *, side=8):
    """A virtual (bytes-free) tile dict over the whole extent of ``path``."""
    return {
        "cellid": 5,
        "raster": None,
        "path": str(path),
        "window": {"col_off": 0, "row_off": 0, "width": side, "height": side},
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }


def _virtual_tile(tmp_path, name="dem.tif", **kw):
    p = str(tmp_path / name)
    side = kw.get("side", 8)
    _write_ramp_tif(p, **kw)
    return _virtual_row(p, side=side)


def _materialized(tile):
    """The same tile as its own bytes (what a pre-virtual reader would have held)."""
    with open(tile["path"], "rb") as fh:
        return {"cellid": 5, "raster": fh.read(), "metadata": {}}


def _point(x, y):
    import shapely.wkb
    from shapely.geometry import Point

    return shapely.wkb.dumps(Point(x, y))


# an 8x8 ramp, origin (0, 8), px 1, EPSG:32633: pixel (col 2, row 3) is 26, centred at (2.5, 4.5)
_ACCESSORS = {
    "width": (lambda t: prx._width_header_udf.func(t), 8),
    "avg": (lambda t: prx._avg_pixel_udf.func(t), [pytest.approx(31.5)]),
    "sample": (
        lambda t: prx._sample_udf.func(t, _point(2.5, 4.5)),
        [pytest.approx(26.0)],
    ),
    "r2w_x": (lambda t: prx._u_r2w_x.func(t, 3, 2), pytest.approx(3.5)),
    "r2w_y": (lambda t: prx._u_r2w_y.func(t, 3, 2), pytest.approx(5.5)),
    "w2r_x": (lambda t: prx._u_w2r_x.func(t, 3.5, 5.5), 3),
    "w2r_y": (lambda t: prx._u_w2r_y.func(t, 3.5, 5.5), 2),
}


@pytest.mark.parametrize("name", sorted(_ACCESSORS), ids=str)
def test_accessor_on_a_virtual_tile_equals_its_bytes(tmp_path, name):
    """A virtual tile answers (never None: the old raster-only builders short-circuited it) as its bytes do."""
    call, want = _ACCESSORS[name]
    tile = _virtual_tile(tmp_path)
    got = call(tile)
    assert got is not None and got == want
    assert call(_materialized(tile)) == got


def test_slope_shapes_follow_the_force_output_params(tmp_path):
    from databricks.labs.gbx.engine import l3 as engine_l3
    from databricks.labs.gbx.engine import run as engine_run

    if engine_run.unavailable_reason():
        pytest.skip(engine_run.unavailable_reason())
    tile = _virtual_tile(tmp_path)
    forced = engine_l3.forced_c0("gbx_rst_slope").func
    auto = forced(tile, "degrees", None, None, None, None, None)
    with _serde.open_tile(bytes(auto["raster"])) as ds:
        assert ds.count == 1 and ds.dtypes[0] == "float32"
        direct = ds.read(1)

    # virtualize_dir: a bytes-free row over a provenance-named file that reads back as the auto result
    out_dir = str(tmp_path / "vout")
    row = forced(tile, "degrees", None, None, out_dir, "run1", None)
    assert row["raster"] is None and row["path"].endswith(".tif")
    assert os.path.basename(row["path"]).startswith("run1_") and os.path.exists(
        row["path"]
    )
    with ot._open(row) as ds:
        assert np.array_equal(ds.read(1), direct)

    # materialize=True forces bytes
    row = forced(tile, "degrees", None, None, None, None, True)
    assert row["raster"] is not None and row["path"] is None


def test_slope_refuses_virtualize_dir_with_materialize():
    with pytest.raises(ValueError, match="mutually exclusive"):
        rx.rst_slope("tile", virtualize_dir="/x", materialize=True)


def test_clip_of_a_virtual_tile_equals_the_clip_of_its_bytes(tmp_path):
    import shapely.wkb
    from shapely.geometry import box

    tile = _virtual_tile(tmp_path)
    geom = shapely.wkb.dumps(box(2.0, 2.0, 6.0, 6.0))  # the interior 4x4
    out = prx._clip_udf.func(tile, geom, False)
    mat_out = prx._clip_udf.func(_materialized(tile), geom, False)
    with (
        _serde.open_tile(bytes(out["raster"])) as a,
        _serde.open_tile(bytes(mat_out["raster"])) as b,
    ):
        assert (a.width, a.height) == (4, 4)
        assert np.array_equal(a.read(1), b.read(1))


def test_merge_of_virtual_tiles_is_one_mosaic_whichever_shape(tmp_path):
    left = _virtual_tile(tmp_path, name="left.tif", ulx=0.0, uly=8.0)
    right = _virtual_tile(tmp_path, name="right.tif", ulx=8.0, uly=8.0)
    out = prx._merge_udf.func([left, right])
    with _serde.open_tile(bytes(out["raster"])) as ds:
        # two adjacent 8-wide tiles at px=1 -> union width 16
        assert ds.width == 16 and ds.bounds.left == 0.0 and ds.bounds.right == 16.0
        want = (ds.crs.to_epsg(), ds.width, ds.height)
    # the virtualize_dir form opens to the same grid (the row's crs is None; the file embeds it)
    row = prx._merge_v2_udf.func([left, right], str(tmp_path / "mout"), None, None)
    assert row["raster"] is None and row["path"] is not None
    with ot._open(row) as ds:
        assert (ds.crs.to_epsg(), ds.width, ds.height) == want


def test_merge_of_materialized_tiles_keeps_the_raw_bytes_overlap_winner():
    """Materialized inputs must NOT be re-encoded before merge: ``merge_tiles`` sorts on the RAW
    GTiff bytes to pick a deterministic last-wins overlap winner; a re-encode flips it.
    """
    from databricks.labs.gbx.pyrx.core import agg as agg_core

    a_bytes = make_geotiff_bytes(width=4, height=4)
    b_bytes = _shift_pixels(make_geotiff_bytes(width=4, height=4, count=1), +50.0)
    a_tile = {"cellid": 0, "raster": a_bytes, "metadata": {}}
    b_tile = {"cellid": 0, "raster": b_bytes, "metadata": {}}
    got = prx._merge_udf.func([a_tile, b_tile])
    assert bytes(got["raster"]) == agg_core.merge_tiles([a_bytes, b_bytes])


def _shift_pixels(raster_bytes, delta):
    """GTiff bytes with every band shifted by ``delta`` (same georef)."""
    from rasterio.io import MemoryFile

    with MemoryFile(raster_bytes) as mf, mf.open() as src:
        data = src.read() + delta
        profile = src.profile.copy()
    with MemoryFile() as out:
        with out.open(**profile) as dst:
            dst.write(data)
        return out.read()


def _engine():
    from databricks.labs.gbx.engine import l3 as engine_l3
    from databricks.labs.gbx.engine import run as engine_run

    if engine_run.unavailable_reason():  # transform runs on the GDAL spec engine alone
        pytest.skip(engine_run.unavailable_reason())
    return engine_l3


@pytest.mark.parametrize("target", [32633, 4326], ids=["identity", "reproject"])
@pytest.mark.parametrize("virtualize", [False, True], ids=["bytes", "virtualize_dir"])
def test_transform_of_a_virtual_tile_opens_in_the_target_crs(
    tmp_path, target, virtualize
):
    """The result is self-consistent in the target CRS (the emitted row's crs is None), whether the
    transform is the identity (a passthrough) or a reprojection (new pixels)."""
    engine_l3 = _engine()
    tile = _virtual_tile(tmp_path, epsg=32633)
    if virtualize:
        row = engine_l3.forced_c0("gbx_rst_transform").func(
            tile, target, str(tmp_path / "tout"), None, None
        )
        assert row["raster"] is None and row["path"] is not None
        opened = ot._open(row)
    else:
        out = engine_l3.c0("gbx_rst_transform").func(tile, target)
        assert out["raster"] is not None
        opened = _serde.open_tile(bytes(out["raster"]))
    with opened as ds:
        assert ds.crs.to_epsg() == target
        if target == 32633:
            assert (ds.width, ds.height) == (8, 8)


def test_identity_transform_returns_the_original_bytes_and_keeps_the_merge_winner():
    """A materialized identity transform returns the ORIGINAL bytes (no re-encode): the raw-bytes
    sort key of ``merge_tiles`` decides the overlap winner, so the merge matches merging raw.
    """
    from databricks.labs.gbx.pyrx.core import agg as agg_core

    transform = _engine().c0("gbx_rst_transform").func
    a = make_geotiff_bytes(width=4, height=4, epsg=4326)
    b = _shift_pixels(make_geotiff_bytes(width=4, height=4, epsg=4326), +50.0)
    ta = transform({"cellid": 0, "raster": a, "metadata": {}}, 4326)
    assert bytes(ta["raster"]) == a
    tb = transform({"cellid": 0, "raster": b, "metadata": {}}, 4326)
    got = prx._merge_udf.func([ta, tb])
    assert bytes(got["raster"]) == agg_core.merge_tiles([a, b])


def test_reproject_identity_returns_the_source_and_a_codeless_crs_still_warps(tmp_path):
    """Identity (target == source EPSG) produces no new pixels; a CRS with no EPSG code is never
    mistaken for it, since the check compares EPSG codes."""
    import rasterio.crs

    from databricks.labs.gbx.pyrx.core import warp

    p = str(tmp_path / "ident.tif")
    src_px = _write_ramp_tif(p, side=8, epsg=32633)
    with rasterio.open(p) as ds:
        out = warp.reproject_to_srid(ds, 32633)
    with rasterio.io.MemoryFile(out) as mf, mf.open() as ods:
        assert ods.crs.to_epsg() == 32633
        np.testing.assert_array_equal(ods.read(1), src_px)

    q = str(tmp_path / "esri.tif")
    prof = dict(
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="float32",
        crs=rasterio.crs.CRS.from_string(
            "ESRI:54008"
        ),  # World Sinusoidal: no EPSG code
        transform=from_origin(0.0, 8.0, 1000.0, 1000.0),
        nodata=-9999.0,
    )
    with rasterio.open(q, "w", **prof) as ds:
        ds.write(np.arange(64, dtype="float32").reshape(8, 8), 1)
    with rasterio.open(q) as ds:
        assert ds.crs.to_epsg() is None
        out = warp.reproject_to_srid(ds, 4326)
    with rasterio.io.MemoryFile(out) as mf, mf.open() as ods:
        assert ods.crs.to_epsg() == 4326


# ---------------------------------------------------------------------------
# Spark Column API on a v2-schema DataFrame
# ---------------------------------------------------------------------------
def _slope_array_from_row(row):
    """Open a (v1 or v2) tile row and return band-1 slope array."""
    with ot._open(row) as ds:
        return ds.read(1)


def _v2_df(spark, tmp_path):
    from pyspark.sql.types import StructField, StructType

    row = VirtualTile.from_row(_virtual_tile(tmp_path)).to_row()
    schema = StructType([StructField("tile", V2_TILE_SCHEMA, nullable=True)])
    return spark.createDataFrame([(row,)], schema)


def test_column_api_on_a_virtual_df(spark, tmp_path):
    from databricks.labs.gbx.engine import run as engine_run

    if engine_run.unavailable_reason():  # slope runs on the GDAL spec engine alone
        pytest.skip(engine_run.unavailable_reason())
    df = _v2_df(spark, tmp_path)
    assert df.select(rx.rst_width("tile").alias("w")).first()["w"] == 8
    out_dir = str(tmp_path / "col_vout")
    row = df.select(rx.rst_slope("tile", virtualize_dir=out_dir).alias("t")).first()[
        "t"
    ]
    assert row["raster"] is None and row["path"].endswith(".tif")
    with ot._open(row.asDict()) as ds:
        assert ds.count == 1
