"""GeoBrix tiles from Zarr: ``gbx_zarr_read(output="tiles")`` and the ``open_tile`` branch.

Most tests run without Spark: ``_read._plan_tile_set`` is the driver half, ``tile_rows`` makes
the tiles (the part a Spark task runs), and ``open_tile`` / ``open_header`` read them as any
pyrx function does. The Spark tests keep one end-to-end run per behaviour: the read itself,
a persisted table of tiles, and the light ``rst_*`` functions on Zarr tiles.
"""

import dataclasses
import json
import uuid

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")

from test.zarr import _helpers as H  # noqa: E402

from databricks.labs.gbx.pyrx.core import open_tile as ot  # noqa: E402
from databricks.labs.gbx.pyrx.core.virtual_tile import (  # noqa: E402
    V2_TILE_SCHEMA,
    VirtualTile,
)
from databricks.labs.gbx.zarr import _read, _tiles  # noqa: E402
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    make_era5_like,
    plain_store_rows,
    rewrite_one_inner_chunk,
)
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrAccessUnavailable,
    ZarrError,
    ZarrUnsupported,
)

SHAPE = (48, 73, 144)
LAYOUTS = ["era5_v3_sharded", "era5_v3_unsharded", "era5_v2"]


# --- helpers -----------------------------------------------------------------------------


def tile_set(source, variables=("t2m",), **kw):
    """The ``TilePlan`` of each variable of a tiles read of ``source`` (a path or a source),
    as ``gbx_zarr_read`` plans them, without Spark."""
    src = (
        source
        if isinstance(source, _read._Source)
        else _read._open_source(None, str(source), "path", None, None)
    )
    variables = list(variables) if variables else None
    names = _read._choose_variables(variables, src)
    return _read._plan_tile_set(
        None,
        src,
        names,
        variables is not None,
        kw.get("sel"),
        kw.get("isel"),
        kw.get("decode", True),
        kw.get("flag"),
        kw.get("band_dim"),
        kw.get("tile_size"),
    )


def one_plan(source, **kw):
    (plan,) = tile_set(source, **kw)
    return plan


def virtual(plan, ids=None):
    rows = _tiles.tile_rows(plan, range(plan.n_tiles) if ids is None else ids)
    return [VirtualTile.from_row(r) for r in rows]


def read(tile):
    """``(array, transform, colorinterp, nodata)`` of an open tile."""
    with ot.open_tile(tile) as ds:
        return ds.read(), ds.transform, ds.colorinterp, ds.nodata


def same(a, b):
    return np.array_equal(np.asarray(a), np.asarray(b), equal_nan=True)


def cube(path, *, dtype="float32", nt=4, attrs=None, lat=None, lon=None, **extra):
    """A small v3 store ``v(time, y, x)`` with coordinate arrays, written by zarr itself.

    ``lat`` and ``lon`` are the label arrays (default a regular 0.5 degree grid, 8 x 12);
    ``attrs`` the attributes of ``v``; the fill value and data are ``range`` of the cells.
    """
    lat = np.linspace(10, 7.5, 6) if lat is None else np.asarray(lat, dtype="float64")
    lon = np.linspace(20, 22.5, 6) if lon is None else np.asarray(lon, dtype="float64")
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    for name, values in (
        ("time", np.arange(nt, dtype="int64")),
        ("y", lat),
        ("x", lon),
    ):
        arr = g.create_array(
            name,
            shape=values.shape,
            chunks=values.shape,
            dtype=values.dtype,
            dimension_names=(name,),
            attributes=(
                {"units": "hours since 2000-01-01"}
                if name == "time"
                else {"units": "degrees_north" if name == "y" else "degrees_east"}
            ),
        )
        arr[:] = values
    shape = (nt, lat.size, lon.size)
    v = g.create_array(
        "v",
        shape=shape,
        chunks=shape,
        dtype=dtype,
        dimension_names=("time", "y", "x"),
        attributes=attrs or {},
        **extra,
    )
    v[:] = (np.arange(np.prod(shape)) % 200).reshape(shape).astype(dtype)
    return path


# --- the grid ----------------------------------------------------------------------------


def test_affine_of_is_north_up_whichever_way_the_axes_run():
    lat, lon = np.linspace(90, -90, 73), np.arange(144) * 2.5
    gt = (-1.25, 2.5, 0.0, 91.25, 0.0, -2.5)
    assert _tiles.affine_of(lat, lon) == gt
    assert (
        _tiles.affine_of(lat[::-1], lon) == gt
    )  # ascending latitude: same north-up grid
    assert _tiles.affine_of(lat, lon[::-1]) == gt


def test_affine_of_refuses_what_is_not_a_regular_grid():
    lon = np.arange(10) * 1.0
    assert _tiles.affine_of(np.array([0.0, 1.0, 3.0, 4.0]), lon) is None
    assert _tiles.affine_of(np.array([5.0, 5.0, 5.0]), lon) is None
    assert _tiles.affine_of(np.array([1.0]), lon) is None  # no cell size
    assert _tiles.affine_of(np.array([0.0, np.nan, 2.0]), lon) is None
    near = np.arange(10) * 0.5
    near[4] += 1e-3  # 0.2 % of a step: not regular at the default tolerance
    assert _tiles.affine_of(near, lon) is None
    assert _tiles.affine_of(near, lon, rtol=1e-2) is not None


def test_affine_of_takes_the_rounding_of_float32_labels():
    lat = np.linspace(10, 0, 101).astype("float32")  # 0.1 degree steps, off by ~1e-6
    assert _tiles.affine_of(lat, np.arange(5.0))[5] == pytest.approx(-0.1, abs=1e-6)


# --- the tiles ---------------------------------------------------------------------------


def test_tiles_are_virtual_and_chunk_aligned(era5_v3_sharded):
    plan = one_plan(era5_v3_sharded)
    rows = _tiles.tile_rows(plan, range(plan.n_tiles))
    assert plan.n_tiles == len(rows) == 4 * 48  # (2 x 2 chunks) per time step
    assert set(rows[0]) == set(V2_TILE_SCHEMA.fieldNames())  # a v2 tile struct
    cells = set()
    for r in rows:
        assert r["raster"] is None and r["path"] == str(era5_v3_sharded)
        win = r["window"]
        # one inner chunk (37 x 72 cells) of the plane, cut at chunk boundaries
        assert win["col_off"] % 72 == 0 and win["row_off"] % 37 == 0
        assert win["width"] <= 72 and win["height"] <= 37
        assert (
            win["col_off"] + win["width"] <= 144
            and win["row_off"] + win["height"] <= 73
        )
        md = r["metadata"]
        assert (md["format"], md["driver"], md["zarr_array"]) == ("zarr", "Zarr", "t2m")
        assert (md["width"], md["height"], md["count"]) == ("144", "73", "1")
        assert (
            md["zarr_gt"] == "-1.25,2.5,0.0,91.25,0.0,-2.5"
            and md["zarr_crs"] == "EPSG:4326"
        )
        assert md["zarr_table"] == "" and md["zarr_version"] == ""
        assert md["sliceCoord_time"].startswith("2023-01-0")
        cells.add((*win.values(), md["sliceDims"]))
    assert len(cells) == plan.n_tiles  # every (window, time step) once
    assert {r["metadata"]["sliceDims"] for r in rows} == {
        f"time={i}" for i in range(48)
    }
    vt = VirtualTile.from_row(rows[0])
    assert _tiles.is_zarr_tile(vt) and vt.is_virtual()
    assert _tiles.is_zarr_tile(rows[0]) and not _tiles.is_zarr_tile(
        VirtualTile(cellid=0, raster=b"x", metadata={"format": "zarr"})
    )
    assert not _tiles.is_zarr_tile(VirtualTile(cellid=0, path="/x.tif"))
    assert _tiles.source_of(rows[1]).endswith('.zarr":t2m[time=1]')


def test_tile_size_overrides_the_chunk_and_windows_never_overlap(era5_v3_sharded):
    plan = one_plan(era5_v3_sharded, isel={"time": 0}, tile_size=(50, 20))
    seen = np.zeros((73, 144), dtype=int)
    for t in virtual(plan):
        c, r, w, h = t.window
        assert w <= 50 and h <= 20
        seen[r : r + h, c : c + w] += 1
    assert (seen == 1).all()  # the plane, covered exactly once
    assert plan.n_tiles == 3 * 4  # 144 / 50, 73 / 20 rounded up


def test_tiles_north_up_descending_lat(era5_v3_sharded):
    plan = one_plan(era5_v3_sharded, isel={"time": 5})
    top_left = next(t for t in virtual(plan) if t.window[:2] == (0, 0))
    data, transform, _, _ = read(top_left)
    t2m = zarr.open_array(str(era5_v3_sharded / "t2m"), mode="r")
    assert data[0, 0, 0] == t2m[5, 0, 0]  # the value at lat 90, lon 0
    assert same(data[0], t2m[5, :37, :72])
    gdal = transform.to_gdal()
    assert (
        gdal[5] < 0 and gdal[3] == 91.25 and gdal[0] == -1.25
    )  # row 0 is the north edge


@pytest.fixture(scope="module")
def era5_ascending(tmp_path_factory):
    """The cube with latitude stored south to north (-90 .. 90)."""
    return make_era5_like(
        tmp_path_factory.mktemp("asc") / "asc.zarr",
        zarr_format=3,
        sharded=False,
        latitude=np.linspace(-90, 90, 73),
    )


def test_tiles_ascending_lat_flipped(era5_ascending):
    plan = one_plan(era5_ascending, isel={"time": 3})
    t2m = zarr.open_array(str(era5_ascending / "t2m"), mode="r")[3]
    whole = np.full((73, 144), np.nan, dtype="float32")
    for t in virtual(plan):
        c, r, w, h = t.window
        data, transform, _, _ = read(t)
        gdal = transform.to_gdal()
        assert gdal[5] < 0  # north-up, whichever way the array runs
        assert gdal[3] == pytest.approx(91.25 - 2.5 * r)  # this window's north edge
        whole[r : r + h, c : c + w] = data[0]
    assert same(whole, t2m[::-1])  # row 0 is lat +90: the last row of the array
    assert not same(whole, t2m)


def test_tiles_across_lon_seam_split(era5_v3_unsharded):
    sel = {"longitude": slice(-10, 10), "latitude": slice(60, 50)}
    plan = one_plan(era5_v3_unsharded, sel=sel, isel={"time": 0})
    tiles = virtual(plan)
    # 350..357.5 (columns 140..143) and 0..10 (columns 0..4): two windows, none across
    assert sorted(t.window[0::2] for t in tiles) == [(0, 5), (140, 4)]
    t2m = zarr.open_array(str(era5_v3_unsharded / "t2m"), mode="r")[0]
    for t in tiles:
        c, r, w, h = t.window
        data, transform, _, _ = read(t)
        assert data.shape == (1, h, w)
        assert same(data[0], t2m[r : r + h, c : c + w])  # north-up, descending: no flip
        assert transform.c == pytest.approx(-1.25 + 2.5 * c)  # each its own origin
    assert not any(t.window[0] < 144 <= t.window[0] + t.window[2] - 1 for t in tiles)


@pytest.mark.parametrize("layout", LAYOUTS)
def test_materialize_matches_blocks(request, layout):
    store = request.getfixturevalue(layout)
    isel = {"time": slice(2, 4)}
    plan = one_plan(store, isel=isel)
    blocks = H.read_blocks(store, variables=["t2m"], isel=isel)
    want, seen = H.assemble(blocks, "t2m", SHAPE)
    assert seen[2:4].all() and not seen[:2].any()
    for t in virtual(plan):
        mat = ot.materialize_to_bytes(t)
        assert mat.raster is not None and not _tiles.is_zarr_tile(mat)
        assert mat.metadata["driver"] == mat.metadata["format"] == "GTiff"
        assert (
            mat.metadata["zarr_array"] == "t2m" and mat.window == t.window
        )  # provenance
        assert int(mat.metadata["tile_byte_size"]) == len(mat.raster)
        c, r, w, h = t.window
        k = int(t.metadata["sliceDims"].partition("=")[2])
        data, _, _, _ = read(mat)
        assert same(data[0], want[k, r : r + h, c : c + w])  # what the blocks hold


def test_a_registered_store_tile_reads_the_override_rows_of_its_shard(
    era5_v3_sharded, monkeypatch, tmp_path
):
    _, rows, arrays = plain_store_rows(era5_v3_sharded, origin="external")
    rows, expected = rewrite_one_inner_chunk(
        rows, arrays["t2m"], tmp_path, chunk=(1, 0, 1), value=7.0
    )
    src = H.registered_source(monkeypatch, era5_v3_sharded, overlay=tmp_path, rows=rows)
    plan = one_plan(src, isel={"time": slice(12, 24)})  # chunk (1, *, *) of the shard
    refs = [json.loads(t.metadata["zarr_refs"])[0] for t in virtual(plan)]
    flat = [ref for slice_refs in refs for ref in slice_refs]
    # the override is a chunk row of its own (written by GeoBrix, in the overlay) naming the
    # inner chunk it replaces; the shard it overrides is the store's own file
    assert any(not r["shard"] and r["inner"] and "origin" not in r for r in flat)
    assert any(r["shard"] and r["origin"] == "external" for r in flat)
    for t in virtual(plan):
        c, r, w, h = t.window
        k = int(t.metadata["sliceDims"].partition("=")[2])
        data, _, _, _ = read(t)
        assert same(data[0], expected[k, r : r + h, c : c + w])
    assert (
        read(next(t for t in virtual(plan) if t.window[:2] == (72, 0)))[0] == 7.0
    ).all()


def test_header_functions_do_not_read_chunks(era5_v3_sharded, monkeypatch):
    from databricks.labs.gbx.pyrx import functions as prx
    from databricks.labs.gbx.zarr import _access

    src = H.registered_source(monkeypatch, era5_v3_sharded)
    plan = one_plan(src, isel={"time": 1})
    tile = next(t for t in virtual(plan) if t.window == (72, 37, 72, 36))
    row = tile.to_row()
    reads = []
    real = _access.FuseRangeReader.read

    def counting(self, regions):
        reads.append(len(regions))
        return real(self, regions)

    monkeypatch.setattr(_access.FuseRangeReader, "read", counting)
    got = {
        "width": prx._uf_width.func(row, None),
        "height": prx._uf_height.func(row, None),
        "srid": prx._uf_srid.func(row, None),
        "bbox": prx._uf_boundingbox.func(row, None),
        "metadata": prx._uf_metadata_udf.func(row, None),
    }
    assert reads == []  # not one byte range was read
    assert (got["width"], got["height"], got["srid"]) == (72, 36, 4326)
    from shapely import wkb

    assert wkb.loads(bytes(got["bbox"])).bounds == pytest.approx(
        (-1.25 + 2.5 * 72, 91.25 - 2.5 * 73, -1.25 + 2.5 * 144, 91.25 - 2.5 * 37)
    )
    meta = got["metadata"]
    assert (meta["driver"], meta["count"], meta["dtype"]) == ("Zarr", "1", "float32")
    assert meta["tag.sliceDims"] == "time=1" and meta["tag.units"] == "K"
    ot.materialize_to_bytes(tile)
    assert reads  # the same counter does see a pixel read


def test_a_zarr_tile_header_equals_the_header_of_its_materialization(era5_v2):
    """Every header accessor gives for a virtual Zarr tile what it gives for the GeoTIFF the
    tile becomes: the geotransform of a partial corner window of a v2 store, its CRS,
    NaN NoData, type and extent."""
    from shapely import wkb

    from databricks.labs.gbx.pyrx.core import accessors

    plan = one_plan(era5_v2, isel={"time": 0}, tile_size=(10, 6))
    tile = virtual(plan, [plan.n_tiles - 1])[0]
    assert tile.window == (140, 72, 4, 1)  # the last window: 4 x 1 cells
    mat = ot.materialize_to_bytes(tile)

    def norm(v):
        if isinstance(v, (bytes, bytearray)):
            return tuple(round(c, 9) for c in wkb.loads(bytes(v)).bounds)
        if isinstance(v, dict):
            return {k: norm(x) for k, x in v.items()}
        if isinstance(v, (list, tuple)):
            return [norm(x) for x in v]
        if isinstance(v, float):
            return "nan" if v != v else round(v, 9)
        return v

    names = (
        "width height numbands srid crs pixelwidth upperleftx upperlefty boundingbox "
        "scalex scaley type getnodata georeference subdatasets"
    ).split()
    with ot.open_header(tile) as virtual_header, ot.open_header(mat) as real_header:
        for name in names:
            fn = getattr(accessors, name)
            assert norm(fn(virtual_header)) == norm(fn(real_header)), name
        assert norm(accessors.bandmetadata(virtual_header, 1)) == norm(
            accessors.bandmetadata(real_header, 1)
        )
        assert (virtual_header.width, virtual_header.height) == (4, 1)


def test_pending_instructions_apply_to_the_decoded_window(era5_v3_unsharded):
    plan = one_plan(era5_v3_unsharded, isel={"time": 0}, tile_size=(8, 5))
    tile = virtual(plan, [0])[0]
    plain, _, _, _ = read(tile)
    md = dict(tile.metadata, pending_srid="3857", pending_nodata="-9999")
    pending = dataclasses.replace(tile, metadata=md)
    with ot.open_tile(pending) as ds:
        assert ds.crs.to_epsg() == 3857  # the relabel reached the bytes
        assert ds.nodata is None or np.isnan(ds.nodata)  # the source's NaN is kept
        assert same(ds.read(), plain)
    banded = dataclasses.replace(
        tile, metadata=dict(tile.metadata, pending_bands="1,1")
    )
    with ot.open_tile(banded) as ds:
        assert ds.count == 2 and same(ds.read()[1], plain[0])
    with ot.open_header(banded) as h:
        assert h.count == 2


def test_clip_and_reprojection_apply_to_a_zarr_tile(era5_v3_sharded):
    from shapely.geometry import box

    plan = one_plan(era5_v3_sharded, isel={"time": 0})
    tile = next(t for t in virtual(plan) if t.window[:2] == (0, 0))
    clipped = dataclasses.replace(
        tile, clip_polygon=box(10, 60, 40, 80).wkb, clip_crs="EPSG:4326"
    )
    data, transform, _, _ = read(clipped)
    assert data.shape[1] < 37 and data.shape[2] < 72  # only the cutline's cells
    full, _, _, _ = read(tile)
    x0 = round((transform.c - (-1.25)) / 2.5)
    y0 = round((91.25 - transform.f) / 2.5)
    assert same(
        data[0][~np.isnan(data[0])],
        full[0][y0 : y0 + data.shape[1], x0 : x0 + data.shape[2]][~np.isnan(data[0])],
    )
    warped = dataclasses.replace(
        tile, crs="EPSG:3857"
    )  # rst_transform's pending target
    with ot.open_tile(warped) as ds:
        assert ds.crs.to_epsg() == 3857 and ds.count == 1
    far = dataclasses.replace(tile, clip_polygon=box(-100, -50, -90, -40).wkb)
    with ot.open_tile(far) as ds:
        assert (ds.width, ds.height) == (
            1,
            1,
        )  # a disjoint cutline: the valid empty tile


# --- bands, decoding and the CRS ---------------------------------------------------------


def test_band_dim_stacks_slices(era5_v3_sharded):
    plan = one_plan(era5_v3_sharded, isel={"time": slice(3, 9)}, band_dim="time")
    assert plan.n_tiles == 4 and plan.count == 6  # one tile per window, six bands
    t2m = zarr.open_array(str(era5_v3_sharded / "t2m"), mode="r")
    for t in virtual(plan):
        md = t.metadata
        assert (md["count"], md["bandDim"], md["zarr_band_index"]) == (
            "6",
            "time",
            "3,4,5,6,7,8",
        )
        assert "sliceDims" not in md and md["bandCoords"].count(",") == 5
        assert md["bandDescriptions"].startswith("time=2023-01-01T03")
        assert len(json.loads(md["zarr_refs"])) == 6  # the objects of each slice
        c, r, w, h = t.window
        data, _, _, _ = read(t)
        assert same(data, t2m[3:9, r : r + h, c : c + w])
        with ot.open_header(t) as hdr:
            assert hdr.count == 6


def test_a_band_stack_reads_each_chunk_once(era5_v3_unsharded, monkeypatch):
    """Eight steps of one time chunk are eight bands of ONE chunk read from the file, not
    eight reads of it: a stack costs the chunks it touches, not the bands it has (the
    decode and the file read are the cost of a tile)."""
    from databricks.labs.gbx.zarr import _access

    src = H.registered_source(monkeypatch, era5_v3_unsharded)
    dense = one_plan(src, isel={"time": slice(0, 8)}, band_dim="time")
    sparse = one_plan(src, isel={"time": [0, 30, 47]}, band_dim="time")
    reads = []
    real = _access.FuseRangeReader.read
    monkeypatch.setattr(
        _access.FuseRangeReader,
        "read",
        lambda self, regions: reads.extend(regions) or real(self, regions),
    )
    t2m = zarr.open_array(str(era5_v3_unsharded / "t2m"), mode="r")
    data, _, _, _ = read(virtual(dense, [0])[0])
    assert len(reads) == 1 and same(data, t2m[0:8, :37, :72])
    reads.clear()  # steps in other chunks: each band right, one read per chunk
    data, _, _, _ = read(virtual(sparse, [0])[0])
    assert len(reads) == 3 and same(data, t2m[[0, 30, 47], :37, :72])


def test_a_band_stack_is_never_alpha_or_rgb(tmp_path):
    from test.pyrx import _alpha_layouts as al

    for nt in (3, 4):  # GDAL would label bands 1-3 RGB, and band 4 of four alpha
        store = cube(tmp_path / f"u{nt}.zarr", dtype="uint8", nt=nt)
        zarr.open_array(str(store / "v"), mode="r+")[nt - 1] = (
            0  # the last step is zeros
        )
        plan = one_plan(store, variables=("v",), band_dim="time")
        tile = virtual(plan, [0])[0]
        for t in (tile, ot.materialize_to_bytes(tile)):
            with ot.open_tile(t) as ds:
                assert ds.count == nt and ds.dtypes[0] == "uint8" and ds.nodata is None
                names = [c.name for c in ds.colorinterp]
                assert not {"alpha", "red", "green", "blue"} & set(names)
                assert (ds.read(nt) == 0).all() and ds.read_masks(
                    1
                ).all()  # none transparent
        assert al.labelled_alpha(ot.materialize_to_bytes(tile).raster) == []
        # a band subset (rst_band) keeps them plain data, by the band-subset rule
        subset = dataclasses.replace(
            tile, metadata=dict(tile.metadata, pending_bands=f"{nt},1")
        )
        with ot.open_tile(subset) as ds:
            assert ds.count == 2 and "alpha" not in [c.name for c in ds.colorinterp]
            assert ds.read_masks(1).all()
    one = one_plan(
        cube(tmp_path / "one.zarr", dtype="uint8"), variables=("v",), isel={"time": 0}
    )
    assert [c.name for c in read(virtual(one, [0])[0])[2]] == ["gray"]


def test_decoded_values_and_nodata(tmp_path):
    packed = {"scale_factor": 0.5, "add_offset": 100.0, "_FillValue": 40}
    store = cube(tmp_path / "p.zarr", dtype="int16", attrs=packed)
    plan = one_plan(store, variables=("v",), isel={"time": 1})
    data, _, _, nodata = read(virtual(plan, [0])[0])
    raw = zarr.open_array(str(store / "v"), mode="r")[1]
    want = np.where(raw == 40, np.nan, raw * 0.5 + 100.0)
    assert data.dtype.kind == "f" and np.isnan(nodata) and same(data[0], want)
    assert (raw == 40).any() and np.isnan(
        data
    ).any()  # the fill is NaN, the nodata value
    stored = one_plan(store, variables=("v",), isel={"time": 1}, decode=False)
    data, _, _, nodata = read(virtual(stored, [0])[0])
    assert data.dtype == np.int16 and nodata == 40 and same(data[0], raw)


def test_the_crs_comes_from_the_grid_mapping(tmp_path):
    x, y = np.arange(6) * 1000.0 + 500000, 4000000.0 - np.arange(6) * 1000.0
    store = cube(
        tmp_path / "utm.zarr",
        dtype="float32",
        attrs={"grid_mapping": "crs"},
        lat=y,
        lon=x,
    )
    g = zarr.open_group(str(store), mode="r+")
    for name in ("y", "x"):
        g[name].attrs.update(units="m", standard_name=f"projection_{name}_coordinate")
    crs = g.create_array(
        "crs", shape=(), dtype="int8", attributes={"grid_mapping_name": "tm"}
    )
    with pytest.raises(ZarrUnsupported, match="names no CRS GeoBrix can read") as e:
        one_plan(store, variables=("v",))  # a grid_mapping with no crs_wkt or epsg_code
    assert "Fix:" in str(e.value)
    crs.attrs.update(epsg_code="EPSG:32633")
    plan = one_plan(store, variables=("v",), isel={"time": 0})
    assert (
        plan.crs == "EPSG:32633" and plan.gt[0] == 499500.0 and plan.gt[3] == 4000500.0
    )
    with ot.open_tile(virtual(plan, [0])[0]) as ds:
        assert ds.crs.to_epsg() == 32633 and ds.transform.to_gdal()[5] == -1000.0
    bare = cube(tmp_path / "bare.zarr", dtype="float32", lat=y, lon=x)
    g = zarr.open_group(str(bare), mode="r+")
    for name in ("y", "x"):
        g[name].attrs.update(units="m", standard_name=f"projection_{name}_coordinate")
    with pytest.raises(ZarrUnsupported, match="no grid_mapping") as e:
        one_plan(bare, variables=("v",))
    assert "grid_mapping variable" in e.value.fix


# --- what a tile refuses -----------------------------------------------------------------


def test_tiles_reject_irregular_axis(tmp_path):
    lat = 90 * np.cos(
        np.linspace(0, np.pi, 73)
    )  # a Gaussian-like latitude: steps differ
    store = make_era5_like(
        tmp_path / "g.zarr", zarr_format=3, sharded=False, latitude=lat
    )
    with pytest.raises(ZarrUnsupported, match="not a regular grid") as e:
        one_plan(store)
    assert e.value.fix == (
        'use output="blocks" or output="table" for irregular or curvilinear grids'
    )
    assert str(e.value).endswith(f"Fix: {e.value.fix}")
    # the same store still reads as blocks
    assert H.read_blocks(store, variables=["t2m"], isel={"time": 0})


@pytest.mark.parametrize("ascending", [False, True], ids=["descending", "ascending"])
def test_the_grid_has_to_be_regular_only_where_the_tiles_lie(tmp_path, ascending):
    """Latitude steps 0.5 for six cells and 0.3 after: the axis is not regular, the first six
    cells are. A selection of those is tiled, with the geotransform placed on the plane
    from the selected cells; a selection that reaches the irregular cells is refused."""
    lat = np.array([10, 9.5, 9, 8.5, 8, 7.5, 7.2, 6.9])
    lat = lat[::-1] if ascending else lat
    store = cube(tmp_path / "p.zarr", dtype="float32", nt=2, lat=lat)
    arr = zarr.open_array(str(store / "v"), mode="r")[0]
    regular = slice(2, 8) if ascending else slice(0, 6)
    irregular = slice(0, 6) if ascending else slice(2, 8)
    with pytest.raises(ZarrUnsupported, match="not a regular grid over the selected"):
        one_plan(store, variables=("v",), isel={"y": irregular})
    with pytest.raises(ZarrUnsupported, match="not a regular grid"):
        one_plan(store, variables=("v",))
    plan = one_plan(store, variables=("v",), isel={"y": regular})
    assert plan.n_tiles == 2 and plan.gt[3] == 10.25 and plan.gt[5] == -0.5
    data, transform, _, _ = read(virtual(plan, [0])[0])
    assert transform.f == 10.25 and transform.e == -0.5 and data.shape == (1, 6, 6)
    assert same(data[0], arr[regular][::-1] if ascending else arr[regular])
    # a span that starts partway down: the plane's own origin is extrapolated to row 0
    low = slice(3, 6)
    plan = one_plan(store, variables=("v",), isel={"y": low})
    data, transform, _, _ = read(virtual(plan, [0])[0])
    assert plan.gt[3] == 10.25  # the whole plane's north edge, from three cells
    assert transform.f == (
        9.25 if ascending else 8.75
    )  # this window's, 2 or 3 rows down
    assert same(data[0], arr[low][::-1] if ascending else arr[low])


def test_a_variable_with_no_grid_is_left_out_by_default_and_refused_by_name(
    tmp_path, caplog
):
    store = cube(tmp_path / "m.zarr", dtype="float32", nt=2)
    g = zarr.open_group(str(store), mode="r+")
    s = g.create_array(
        "series", shape=(5,), dtype="float32", dimension_names=("station",)
    )
    s[:] = 1
    with caplog.at_level("WARNING"):
        (plan,) = tile_set(store, variables=None)
    assert plan.array == "v" and "series" in caplog.text
    with pytest.raises(ZarrUnsupported, match="cannot tell the latitude"):
        tile_set(store, variables=("series",))


def test_non_volume_root_raises_access_unavailable(era5_v3_sharded):
    plan = one_plan(era5_v3_sharded, isel={"time": 0})
    s3 = dataclasses.replace(plan, root="s3://bucket/era5.zarr", table="db.t")
    tile = virtual(s3, [0])[0]
    with pytest.raises(ZarrAccessUnavailable, match="s3://bucket") as e:
        ot.materialize_to_bytes(tile)
    assert e.value.fix == "materialize the tiles, or keep the store on a Volume"
    with ot.open_header(tile) as h:  # the header needs no files
        assert (h.width, h.height) == (72, 37)  # the window of a 144 x 73 grid
    dbfs = dataclasses.replace(
        plan, root=f"dbfs:{era5_v3_sharded}"
    )  # a Volume spelling
    assert read(virtual(dbfs, [0])[0])[0].shape == (1, 37, 72)


def test_a_tile_over_the_task_cap_is_refused_with_a_fix(era5_v3_sharded, monkeypatch):
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1000")  # the cap the driver measures
    plan = one_plan(era5_v3_sharded, isel={"time": 0})
    with pytest.raises(ZarrError, match="more than the 1000") as e:
        read(virtual(plan, [0])[0])  # 37 x 72 float32 cells
    assert "smaller tile_size" in e.value.fix
    small = one_plan(era5_v3_sharded, isel={"time": 0}, tile_size=(4, 4))  # 64 bytes
    assert read(virtual(small, [0])[0])[0].shape == (1, 4, 4)


def test_a_virtual_zarr_tile_is_written_as_a_geotiff(era5_v3_sharded):
    from databricks.labs.gbx.ds import _write

    tile = virtual(one_plan(era5_v3_sharded, isel={"time": 0}), [0])[0]
    assert tile.metadata["driver"] == "Zarr"  # the metadata a writer is handed
    raster = ot.materialize_to_bytes(tile).raster
    assert _write.tile_to_bytes(-1, raster, dict(tile.metadata)) == raster
    assert read(ot.materialize_to_bytes(tile))[0].shape == (1, 37, 72)
    # the writer's block-streaming branch (a tile over its cap) has no file to stream from
    streamed = ot._windowed_materialize_bytes(tile)
    assert same(read(VirtualTile(cellid=0, raster=streamed))[0], read(tile)[0])


def test_the_file_fast_path_never_takes_a_zarr_tile(era5_v3_sharded):
    from databricks.labs.gbx.pyrx import grouped_exec

    row = virtual(one_plan(era5_v3_sharded), [0])[0].to_row()
    assert grouped_exec._file_fast_path_uri(row) is None
    row["metadata"] = {}  # the same struct as a plain virtual tile has a path to open
    assert grouped_exec._file_fast_path_uri(row) == str(era5_v3_sharded)


# --- Spark: the read, a persisted table of tiles, the light functions ----------------------


def _tiles_df(spark, source, **kw):
    return _read.gbx_zarr_read(spark, source, output="tiles", **kw)


def test_read_tiles_returns_tile_structs(spark_delta, era5_v3_sharded):
    sel = dict(variables=["t2m"], isel={"time": slice(0, 2)})
    df = _tiles_df(spark_delta, str(era5_v3_sharded), **sel)
    from databricks.labs.gbx.ds.raster import reader_schema_v2

    assert df.schema == reader_schema_v2()
    rows = df.collect()
    plan = one_plan(era5_v3_sharded, isel=sel["isel"])
    want = _tiles.tile_rows(plan, range(plan.n_tiles))
    assert len(rows) == len(want) == 8

    def key(d):
        return (
            d["window"]["col_off"],
            d["window"]["row_off"],
            d["metadata"]["sliceDims"],
        )

    got = sorted((r.tile.asDict(recursive=True) for r in rows), key=key)
    assert got == sorted(want, key=key)  # the task makes the tiles tile_rows makes
    assert all(r.source.startswith('ZARR:"') and "[time=" in r.source for r in rows)
    mat = _tiles_df(
        spark_delta, str(era5_v3_sharded), virtual_tiles=False, **sel
    ).collect()
    assert len(mat) == 8 and all(r.tile.raster for r in mat)
    by = {
        (r.tile.window.col_off, r.tile.window.row_off, r.tile.metadata["sliceDims"]): r
        for r in mat
    }
    for w in want[:3]:
        expect = read(VirtualTile.from_row(w))[0]
        vt = VirtualTile.from_row(by[key(w)].tile.asDict(recursive=True))
        assert vt.metadata["driver"] == "GTiff" and same(read(vt)[0], expect)


def test_read_tiles_on_a_spark_connect_client(spark_connect_local, era5_v3_sharded):
    """The plan shapes of the read (a ranged ``spark.range``, ``mapInArrow`` with the tile
    schema) on a Connect client, the session type of Serverless, and the cap the driver
    measures there. Run alone: ``--path .../test_tiles.py -k connect`` (a classic session
    in the same process makes the fixture skip)."""
    source, sel = str(era5_v3_sharded), dict(
        variables=["t2m"], isel={"time": slice(0, 2)}
    )
    rows = _tiles_df(spark_connect_local, source, **sel).collect()
    assert len(rows) == 8 and {r.tile.metadata["format"] for r in rows} == {"zarr"}
    assert json.loads(rows[0].tile.metadata["zarr_meta"])["cap"] == 64 * 1024**2
    mat = _tiles_df(spark_connect_local, source, virtual_tiles=False, **sel).collect()
    assert len(mat) == 8 and all(r.tile.raster for r in mat)


def test_read_tiles_checks_its_arguments(spark_delta, era5_v3_sharded):
    path = str(era5_v3_sharded)
    with pytest.raises(ValueError, match="tile_size must be"):
        _tiles_df(spark_delta, path, tile_size=(0, 4))
    with pytest.raises(ValueError, match="band_dim 'nope' is not a dimension"):
        _tiles_df(spark_delta, path, band_dim="nope")
    with pytest.raises(ValueError, match="virtual_tiles must be"):
        _tiles_df(spark_delta, path, virtual_tiles="yes")
    with pytest.raises(ValueError, match="output must be one of"):
        _read.gbx_zarr_read(spark_delta, path, output="rasters")
    with pytest.raises(
        ZarrAccessUnavailable
    ) as e:  # direct access arrives with Task 18
        _tiles_df(spark_delta, path, access="direct")
    assert e.value.fix == 'use access="auto" or "fuse"'
    assert (
        _tiles_df(
            spark_delta, path, variables=["t2m"], isel={"time": slice(0, 0)}
        ).count()
        == 0
    )


@pytest.mark.parametrize("kind", ["registered", "plain"])
def test_persisted_tile_table_round_trip(spark_delta, era5_v3_sharded, tmp_path, kind):
    from databricks.labs.gbx.zarr._register import gbx_zarr_register

    table = f"gbx_zarr_tiles_{uuid.uuid4().hex[:8]}"
    source = str(era5_v3_sharded)
    if kind == "registered":
        gbx_zarr_register(spark_delta, source, table)
        source = table
    sel = dict(
        variables=["t2m"], isel={"time": slice(4, 6)}, sel={"longitude": slice(0, 40)}
    )
    out = str(tmp_path / "tiles")
    _tiles_df(spark_delta, source, **sel).write.format("delta").save(out)
    if kind == "registered":  # the tiles carry their own refs: no manifest is consulted
        spark_delta.sql(f"DROP TABLE {table}")
    rows = spark_delta.read.format("delta").load(out).collect()
    t2m = zarr.open_array(str(era5_v3_sharded / "t2m"), mode="r")
    assert (
        len(rows) == 2 * 2
    )  # two windows (the chunk's 72 columns reach lon 40) x two steps
    for r in rows:
        vt = VirtualTile.from_row(r.tile.asDict(recursive=True))
        assert _tiles.is_zarr_tile(vt)
        assert (vt.metadata["zarr_table"] == table) is (kind == "registered")
        c, rr, w, h = vt.window
        k = int(vt.metadata["sliceDims"].partition("=")[2])
        mat = ot.materialize_to_bytes(vt)
        assert same(read(mat)[0][0], t2m[k, rr : rr + h, c : c + w])


def test_rst_clip_and_transform_on_zarr_tiles(spark_delta, era5_v3_sharded):
    from pyspark.sql import functions as F
    from shapely.geometry import box

    from databricks.labs.gbx.pyrx import functions as prx

    df = _tiles_df(
        spark_delta, str(era5_v3_sharded), variables=["t2m"], isel={"time": 0}
    ).select("tile")
    tile = F.col("tile")
    one = df.where("tile.window.col_off = 0 AND tile.window.row_off = 0")
    row = one.select(
        prx.rst_width(tile).alias("w"),
        prx.rst_height(tile).alias("h"),
        prx.rst_srid(tile).alias("srid"),
        prx.rst_numbands(tile).alias("bands"),
        prx.rst_srid(prx.rst_transform(tile, F.lit(3857))).alias("srid_t"),
        prx.rst_width(
            prx.rst_clip(tile, F.lit(bytes(box(10, 60, 40, 80).wkb)), F.lit(False))
        ).alias("w_clip"),
        prx.rst_isempty(tile).alias("empty"),
    ).first()
    assert (row.w, row.h, row.srid, row.bands) == (72, 37, 4326, 1)
    assert row.srid_t == 3857 and 0 < row.w_clip < 72 and row.empty is False
    grouped = prx.rst_clip_grouped(
        one, bytes(box(10, 60, 40, 80).wkb), out_col="clipped"
    )
    assert grouped.select(prx.rst_width("clipped")).first()[0] == row.w_clip
    assert df.select(prx.rst_width(tile)).count() == 4
