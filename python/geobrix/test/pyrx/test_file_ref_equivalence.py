"""One equivalence table for how a tile is read.

A tile is the same tile however it reaches the operation: a window of a file read by
path (the fallback), the same window read through a FILE reference (its path is bogus,
so the reference is the only way in), or the window's own bytes (a materialized tile).
Each row is one operation on the open dataset, and every way of reading must give the
answer of the fallback. Windows: the whole 4x3 file, and a part of it.

The routing rows pin what the FILE reference may NOT change: a clip or a warp on the
tile still applies, a failing reference falls back to the file, a materialized tile
ignores the reference, and the FUSE-direct switch does not move the routing.
"""

import io

import numpy as np
import pytest
import rasterio.io

from databricks.labs.gbx._geom import parse_geom
from databricks.labs.gbx.pyrx.core import accessors as acc
from databricks.labs.gbx.pyrx.core import agg as agg_core
from databricks.labs.gbx.pyrx.core import coords, edit
from databricks.labs.gbx.pyrx.core import mapalgebra as mapalgebra_core
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import resample, terrain, warp
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from databricks.labs.gbx.pyrx.functions import _dataset_to_gtiff_bytes

from .conftest import make_geotiff_bytes

RAMP = np.arange(12, dtype="float32").reshape(3, 4)  # what gtiff_bytes holds
WINDOWS = {"whole": (0, 0, 4, 3), "part": (1, 1, 3, 2)}


class _Ref:
    """A FileRef over bytes. ``local`` is what ``as_local_file`` answers (a header read asks for it);
    a pixel read must never ask."""

    def __init__(self, data, local=None):
        self.data = data
        self.local = local
        self.opened = False

    def open(self):
        self.opened = True
        return io.BytesIO(self.data)

    def as_local_file(self):
        if self.local is None:
            raise AssertionError("a pixel read must stay on the FILE branch")
        return self.local


class _FailingRef(_Ref):
    """A reference whose stream fails: the read degrades to the local file."""

    def open(self):
        raise IOError("FileRef stream failed")


def _write(tmp_path, data, name="src.tif"):
    p = tmp_path / name
    p.write_bytes(data)
    return str(p)


def _ways(tmp_path, data, window):
    """The three ways to read the same window: ``{way: (tile row, file_ref)}``."""
    path = _write(tmp_path, data)
    fallback = VirtualTile(cellid=0, path=path, window=window)
    via_ref = VirtualTile(cellid=0, path="/nonexistent/src.tif", window=window)
    mat = ot.materialize_to_bytes(fallback)
    return {
        "fallback": (fallback.to_row(), None),
        "file_ref": (via_ref.to_row(), _Ref(data, local=path)),
        "materialized": (VirtualTile(cellid=0, raster=mat.raster).to_row(), None),
    }


def _bytes_of(ds):
    return _dataset_to_gtiff_bytes(ds)


# --- the operations: op(ds) -> result; "kind" says how two results are equal --------------------


def _wkb_box(x0, y0, x1, y1):
    from shapely import geometry, wkb

    return parse_geom(wkb.dumps(geometry.box(x0, y0, x1, y1)))


def _mapalgebra(ds):
    b = _bytes_of(ds)
    return mapalgebra_core.mapalgebra([b], "A * 2")


def _frombands(ds):
    b = _bytes_of(ds)
    return agg_core.frombands_tiles([(0, b), (1, b)])


def _merge(ds):
    b = _bytes_of(ds)
    return agg_core.merge_tiles([b, b])


# (id, header-only, op, kind, atol) - kind: "eq" python values, "tile" tile bytes pixel-equal
PIXEL_OPS = [
    ("avg", False, acc.avg, "eq", 0),
    ("min", False, acc.minimum, "eq", 0),
    ("max", False, acc.maximum, "eq", 0),
    ("median", False, acc.median, "eq", 0),
    ("pixelcount", False, acc.pixelcount, "eq", 0),
    ("histogram", False, lambda ds: acc.histogram(ds, 16, None, None, False), "eq", 0),
    ("initnodata", False, edit.init_nodata, "tile", 0),
    (
        "clip",
        False,
        lambda ds: edit.clip_to_geom(
            ds, _wkb_box(10.0, 49.0, 11.0, 50.0), all_touched=False, geom_crs=None
        ),
        "tile",
        0,
    ),
    (
        "resample",
        False,
        lambda ds: resample.resample_by_factor(ds, 2.0, "bilinear"),
        "tile",
        1e-5,
    ),
    (
        "resample_to_size",
        False,
        lambda ds: resample.resample_to_size(ds, 8, 6, "bilinear"),
        "tile",
        1e-5,
    ),
    (
        "resample_to_res",
        False,
        lambda ds: resample.resample_to_res(ds, 0.25, 0.25, "bilinear"),
        "tile",
        1e-5,
    ),
    ("updatetype", False, lambda ds: edit.update_type(ds, "Float64"), "tile", 0),
    ("threshold", False, lambda ds: edit.threshold(ds, ">", 5.0), "tile", 0),
    ("slope", False, lambda ds: terrain.slope(ds, unit="degrees"), "tile", 1e-5),
    (
        "aspect",
        False,
        lambda ds: terrain.aspect(ds, trigonometric=False, zero_for_flat=False),
        "tile",
        1e-5,
    ),
    (
        "hillshade",
        False,
        lambda ds: terrain.hillshade(ds, azimuth=315.0, altitude=45.0, z_factor=1.0),
        "tile",
        0,
    ),
    ("transform", False, lambda ds: warp.reproject_to_srid(ds, 32632), "tile", 1e-5),
    (
        "to_webmercator",
        False,
        lambda ds: warp.reproject_to_srid(ds, 3857),
        "tile",
        1e-5,
    ),
    (
        "transformcrs",
        False,
        lambda ds: warp.reproject_to_crs(ds, "EPSG:3857"),
        "tile",
        1e-5,
    ),
    ("setsrid", False, lambda ds: edit.set_srid(ds, 32632), "tile", 0),
    ("setcrs", False, lambda ds: edit.set_crs(ds, "EPSG:32632"), "tile", 0),
    ("frombands", False, _frombands, "tile", 0),
    ("merge", False, _merge, "tile", 0),
    ("mapalgebra", False, _mapalgebra, "tile", 1e-5),
]

# Header operations read no pixel; a window reports its own size and origin.
HEADER_OPS = [
    ("width", acc.width),
    ("height", acc.height),
    ("numbands", acc.numbands),
    ("srid", acc.srid),
    ("type", acc.type),
    ("getnodata", acc.getnodata),
    ("pixelwidth", acc.pixelwidth),
    ("upperleftx", acc.upperleftx),
    ("upperlefty", acc.upperlefty),
    ("boundingbox", acc.boundingbox),
    ("r2w_x", lambda ds: coords.raster_to_world_x(ds, 1, 1)),
    ("r2w_y", lambda ds: coords.raster_to_world_y(ds, 1, 1)),
    ("w2r_x", lambda ds: coords.world_to_raster_x(ds, 10.75, 49.25)),
    ("w2r_y", lambda ds: coords.world_to_raster_y(ds, 10.75, 49.25)),
]


def _assert_tiles_equal(got, want, atol, label):
    assert got is not None and want is not None, label
    with rasterio.io.MemoryFile(want) as a, rasterio.io.MemoryFile(got) as b:
        with a.open() as ea, b.open() as eb:
            assert eb.count == ea.count and eb.dtypes == ea.dtypes, label
            assert (eb.width, eb.height) == (ea.width, ea.height), label
            assert eb.crs == ea.crs, label
            if atol:
                np.testing.assert_allclose(
                    eb.read(), ea.read(), atol=atol, err_msg=label
                )
            else:
                np.testing.assert_array_equal(eb.read(), ea.read(), err_msg=label)


@pytest.mark.parametrize("window", sorted(WINDOWS), ids=str)
@pytest.mark.parametrize("op", PIXEL_OPS, ids=[o[0] for o in PIXEL_OPS])
def test_pixel_op_is_the_same_whichever_way_the_tile_is_read(
    tmp_path, gtiff_bytes, op, window
):
    name, _header, fn, kind, atol = op
    ways = _ways(tmp_path, gtiff_bytes, WINDOWS[window])
    results = {}
    for way, (row, ref) in ways.items():
        with ot._open(row, file_ref=ref) as ds:
            results[way] = fn(ds)
        if way == "file_ref":
            assert ref.opened, f"{name}: the FILE branch was not taken"
    for way in ("file_ref", "materialized"):
        label = f"{name}/{window}/{way}"
        if kind == "tile":
            _assert_tiles_equal(results[way], results["fallback"], atol, label)
        else:
            assert results[way] == results["fallback"], label


@pytest.mark.parametrize("window", sorted(WINDOWS), ids=str)
@pytest.mark.parametrize("op", HEADER_OPS, ids=[o[0] for o in HEADER_OPS])
def test_header_op_is_the_same_whichever_way_and_reads_no_pixel(
    tmp_path, gtiff_bytes, op, window, monkeypatch
):
    name, fn = op
    ways = _ways(tmp_path, gtiff_bytes, WINDOWS[window])
    results = {}
    for way, (row, ref) in ways.items():
        if way == "materialized":
            with ot.open_header(row, file_ref=ref) as ds:
                results[way] = fn(ds)
            continue
        with monkeypatch.context() as m:
            m.setattr(
                rasterio.io.DatasetReader,
                "read",
                lambda *a, **k: (_ for _ in ()).throw(
                    AssertionError(f"{name} must not read pixels")
                ),
            )
            with ot.open_header(row, file_ref=ref) as ds:
                results[way] = fn(ds)
    assert results["file_ref"] == results["fallback"], name
    assert results["materialized"] == results["fallback"], name


def test_header_values_are_the_windows_own():
    """The anchors a row cannot judge against another way: size, origin and coordinates of a part."""
    data = make_geotiff_bytes()
    row = VirtualTile(cellid=0, raster=data).to_row()
    with ot.open_header(row) as ds:
        assert (acc.width(ds), acc.height(ds), acc.numbands(ds)) == (4, 3, 1)
        assert acc.srid(ds) == 4326 and acc.type(ds) == ["Float32"]
        assert acc.getnodata(ds) == [-9999.0]
        assert (acc.upperleftx(ds), acc.upperlefty(ds)) == (10.0, 50.0)
        assert coords.raster_to_world_x(ds, 1, 1) == pytest.approx(10.75)
        assert coords.raster_to_world_y(ds, 1, 1) == pytest.approx(49.25)
        assert coords.world_to_raster_x(ds, 10.75, 49.25) == 1
        assert coords.world_to_raster_y(ds, 10.75, 49.25) == 1


def test_a_part_reports_its_own_header(tmp_path, gtiff_bytes):
    ways = _ways(tmp_path, gtiff_bytes, WINDOWS["part"])
    for way, (row, ref) in ways.items():
        with ot.open_header(row, file_ref=ref) as ds:
            assert (acc.width(ds), acc.height(ds)) == (3, 2), way
            assert acc.upperleftx(ds) == pytest.approx(10.5), way
            assert acc.upperlefty(ds) == pytest.approx(49.5), way


@pytest.mark.parametrize("window", sorted(WINDOWS), ids=str)
def test_the_pixels_are_the_windows_slice_whichever_way(tmp_path, gtiff_bytes, window):
    c, r, w, h = WINDOWS[window]
    for way, (row, ref) in _ways(tmp_path, gtiff_bytes, WINDOWS[window]).items():
        with ot._open(row, file_ref=ref) as ds:
            np.testing.assert_array_equal(ds.read(1), RAMP[r : r + h, c : c + w], way)


def test_the_file_aware_frombands_stacks_what_the_plain_one_stacks(
    tmp_path, gtiff_bytes
):
    import pandas as pd

    from databricks.labs.gbx.pyrx.functions import _frombands_agg_file_udf

    path = _write(tmp_path, gtiff_bytes)
    row = VirtualTile(cellid=0, path=path, window=WINDOWS["whole"]).to_row()
    with ot._open(row) as ds:
        b = _bytes_of(ds)
    want = agg_core.frombands_tiles([(0, b), (1, b)])
    ref = _Ref(gtiff_bytes, local=path)
    got = _frombands_agg_file_udf.func(
        pd.Series([row, row]), pd.Series([ref, ref]), pd.Series([0, 1])
    )
    _assert_tiles_equal(bytes(got), want, 0, "frombands_agg")
    with rasterio.io.MemoryFile(bytes(got)) as mf, mf.open() as ds:
        assert ds.count == 2


# --- routing: what a FILE reference does not change ---------------------------------------------


def test_a_tile_without_a_reference_reads_its_path_whole_or_windowed(
    tmp_path, gtiff_bytes
):
    path = _write(tmp_path, gtiff_bytes)
    whole = VirtualTile(cellid=0, path=path, window=None)
    with ot.open_tile(whole) as ds:
        np.testing.assert_array_equal(ds.read(1), RAMP)
    with ot.open_tile(whole, file_ref=None) as ds:
        np.testing.assert_array_equal(ds.read(1), RAMP)


def test_a_failing_reference_falls_back_to_the_file(tmp_path, gtiff_bytes):
    path = _write(tmp_path, gtiff_bytes)
    tile = VirtualTile(cellid=0, path=path, window=WINDOWS["whole"])
    ref = _FailingRef(gtiff_bytes, local=_write(tmp_path, gtiff_bytes, "local.tif"))
    with ot.open_tile(tile, file_ref=ref) as ds:
        np.testing.assert_array_equal(ds.read(1), RAMP)


def test_a_clip_still_applies_through_a_reference(tmp_path, gtiff_bytes):
    path = _write(tmp_path, gtiff_bytes)
    clip = VirtualTile(
        cellid=0,
        path=path,
        window=WINDOWS["whole"],
        clip_polygon=_wkb_bytes(10.0, 48.0, 11.0, 50.5),
        clip_crs="EPSG:4326",
    )
    with ot.open_tile(clip, file_ref=_Ref(gtiff_bytes, local=path)) as a:
        via_ref = a.read(1)
    with ot.open_tile(clip, file_ref=None) as b:
        plain = b.read(1)
    np.testing.assert_array_equal(via_ref, plain)
    assert via_ref.shape[1] < 4, "the clip was bypassed by the FILE branch"


def test_a_warp_still_applies_through_a_reference(tmp_path, gtiff_bytes):
    path = _write(tmp_path, gtiff_bytes)
    tile = VirtualTile(cellid=0, path=path, window=WINDOWS["whole"], crs="EPSG:3857")
    with ot.open_tile(tile, file_ref=_Ref(gtiff_bytes, local=path)) as a:
        via_ref, crs_a = a.read(1), a.crs
    with ot.open_tile(tile, file_ref=None) as b:
        plain, crs_b = b.read(1), b.crs
    assert crs_a.to_epsg() == 3857 and crs_b.to_epsg() == 3857
    np.testing.assert_array_equal(via_ref, plain)


@pytest.mark.parametrize("switch", [None, "1"], ids=["switch-unset", "switch-on"])
def test_routing_depends_only_on_the_presence_of_a_reference(
    tmp_path, gtiff_bytes, monkeypatch, switch
):
    """A reference present is the stream, none is the path; the FUSE-direct switch moves neither,
    and a materialized tile never consults either."""
    if switch is None:
        monkeypatch.delenv("GBX_DISABLE_FUSE_DIRECT", raising=False)
    else:
        monkeypatch.setenv("GBX_DISABLE_FUSE_DIRECT", switch)
    path = _write(tmp_path, gtiff_bytes)
    direct = VirtualTile(cellid=0, path=path, window=WINDOWS["whole"])
    with ot.open_tile(direct, file_ref=None) as ds:
        np.testing.assert_array_equal(ds.read(1), RAMP)
    stream = _Ref(gtiff_bytes)
    s3 = VirtualTile(
        cellid=0, path="s3://bucket/prefix/tile.tif", window=WINDOWS["whole"]
    )
    with ot.open_tile(s3, file_ref=stream) as ds:
        np.testing.assert_array_equal(ds.read(1), RAMP)
    assert stream.opened
    mat = VirtualTile(cellid=0, raster=gtiff_bytes, path="/nonexistent/x.tif")
    with ot.open_tile(mat, file_ref=_Ref(b"not a tile")) as ds:
        np.testing.assert_array_equal(ds.read(1), RAMP)


def _wkb_bytes(x0, y0, x1, y1):
    from shapely import geometry, wkb

    return wkb.dumps(geometry.box(x0, y0, x1, y1))
