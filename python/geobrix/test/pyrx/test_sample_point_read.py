"""rst_sample on a virtual tile reads the pixels at its points, not its window.

A virtual tile is a path and a window. ``rst_sample`` used to open it as every pixel op
does: decode the whole window and re-encode it as a GeoTIFF in memory (``open_tile``), then
sample one pixel of that copy. On an hourly global GeoTIFF that cost about a second a
point on Databricks. The rows here hold the point read to its budget and to the values of
the materialized tile.
"""

import os

import numpy as np
import pytest
import rasterio
import rasterio.io
import rasterio.warp
import shapely
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.pyrx.core import compression as _comp
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

SAMPLE = prx._sample_udf.func


def _write(path, data, *, crs="EPSG:32633", nodata=None, block=256, **extra):
    """A tiled ZSTD GeoTIFF of ``data`` ``(bands, rows, cols)``; origin (500000, 5000000), 10 m."""
    count, rows, cols = data.shape
    prof = dict(
        driver="GTiff",
        width=cols,
        height=rows,
        count=count,
        dtype=str(data.dtype),
        crs=crs,
        transform=from_origin(500000.0, 5000000.0, 10.0, 10.0),
        nodata=nodata,
        tiled=True,
        blockxsize=block,
        blockysize=block,
        compress="zstd",
        **extra,
    )
    with rasterio.open(path, "w", **prof) as dst:
        dst.write(data)
    return str(path)


def _tile(path, window=None, metadata=None, **fields):
    return {
        "cellid": 0,
        "raster": None,
        "path": path,
        "window": (
            None
            if window is None
            else dict(zip(("col_off", "row_off", "width", "height"), window))
        ),
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": dict(metadata or {}),
        **fields,
    }


def _centre(col, row):
    """The world point at the centre of pixel (col, row) of the files written by ``_write``."""
    return 500000.0 + (col + 0.5) * 10.0, 5000000.0 - (row + 0.5) * 10.0


def _wkb(x, y, srid=None):
    p = shapely.Point(x, y)
    if srid is not None:
        p = shapely.set_srid(p, srid)
        return shapely.to_wkb(p, include_srid=True)
    return shapely.to_wkb(p)


@pytest.fixture
def in_one_task(monkeypatch):
    """Run as inside one Spark task (attempt 1); ``task(n)`` moves to attempt ``n``."""
    state = {"id": 1}
    monkeypatch.setattr(ot, "_task_attempt_id", lambda: state["id"], raising=False)
    yield lambda n: state.update(id=n)
    ot._TASK_SOURCES.reset()


@pytest.fixture
def counters(monkeypatch):
    """Counts rasterio opens, the bytes of pixels read from files, and GeoTIFF encodes."""
    c = {"opens": 0, "read_bytes": 0, "encodes": 0}
    real_open, real_read = rasterio.open, rasterio.io.DatasetReader.read
    real_write = _comp.write_gtiff

    def _open(*a, **kw):
        c["opens"] += 1
        return real_open(*a, **kw)

    def _read(self, *a, **kw):
        out = real_read(self, *a, **kw)
        if not str(self.name).startswith("/vsimem/"):  # a file, not a MemoryFile
            c["read_bytes"] += out.nbytes
        return out

    def _encode(*a, **kw):
        c["encodes"] += 1
        return real_write(*a, **kw)

    monkeypatch.setattr(rasterio, "open", _open)
    monkeypatch.setattr(rasterio.io.DatasetReader, "read", _read)
    monkeypatch.setattr(_comp, "write_gtiff", _encode)
    return c


def test_points_on_one_virtual_tile_open_it_once_and_read_a_pixel_each(
    tmp_path, in_one_task, counters
):
    """20 points on one 1024 x 1024 float32 virtual tile, in one task: one open, a pixel
    read per point, no encode. Before: 20 opens, 20 whole-window reads (4 MiB each) and 20
    re-encodes."""
    rng = np.random.default_rng(3)
    data = rng.normal(280, 10, (1, 1024, 1024)).astype("float32")
    path = _write(tmp_path / "hour.tif", data)
    tile = _tile(path)
    cells = [(int(c), int(r)) for c, r in rng.integers(0, 1024, (20, 2))]
    counters.update(opens=0, read_bytes=0, encodes=0)  # count the samples only

    got = [SAMPLE(tile, _wkb(*_centre(c, r))) for c, r in cells]

    assert got == [[float(data[0, r, c])] for c, r in cells]
    assert counters == {"opens": 1, "read_bytes": 20 * 4, "encodes": 0}


def _matrix_cases(tmp_path):
    """(tile, points, want-not-None) per case: each case samples the virtual tile and the
    materialized tile at the same points."""
    rng = np.random.default_rng(5)
    f32 = rng.normal(280, 10, (1, 600, 700)).astype("float32")
    f32[0, 210, 310] = -9999.0
    plain = _write(tmp_path / "f32.tif", f32, nodata=-9999.0)
    u16 = rng.integers(1, 5000, (3, 300, 300)).astype("uint16")
    u16[:, 40, 50] = 0
    multi = _write(tmp_path / "u16.tif", u16)  # no NoData declared
    nocrs = _write(tmp_path / "nocrs.tif", f32, crs=None, nodata=-9999.0)
    masked = tmp_path / "masked.tif"
    _write(masked, u16[:1].astype("uint8"))
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with rasterio.open(masked, "r+") as ds:
            m = np.full((300, 300), 255, "uint8")
            m[:, :150] = 0
            ds.write_mask(m)
    lon, lat = rasterio.warp.transform(
        "EPSG:32633", "EPSG:4326", *zip(_centre(400, 300))
    )
    win = (300, 200, 400, 300)
    clip = shapely.to_wkb(shapely.box(*_centre(300, 400), *_centre(450, 250)))
    return {
        "whole file": (
            _tile(plain),
            [_centre(5, 7), _centre(699, 599), _centre(310, 210)],
        ),
        "sub-window": (
            _tile(plain, window=win),
            [_centre(310, 210), _centre(305, 450), _centre(10, 10), _centre(699, 499)],
        ),
        "outside the file": (_tile(plain), [(0.0, 0.0), _centre(-1, 3), _centre(3, 3)]),
        "band select": (
            _tile(multi, metadata={"pending_bands": "3,1"}),
            [_centre(1, 2), _centre(50, 40)],
        ),
        "pending nodata": (
            _tile(multi, window=(10, 10, 200, 200), metadata={"pending_nodata": "0"}),
            [_centre(50, 40), _centre(60, 60)],
        ),
        "pending nodata auto": (
            _tile(multi, metadata={"pending_nodata": "auto"}),
            [_centre(50, 40), _centre(61, 62)],
        ),
        "reprojected point": (
            _tile(plain),
            [(lon[0], lat[0], 4326), _centre(400, 300)],
        ),
        "pending srid": (
            _tile(nocrs, window=win, metadata={"pending_srid": "32633"}),
            [(lon[0], lat[0], 4326), _centre(400, 300)],
        ),
        "own mask": (_tile(str(masked)), [_centre(10, 10), _centre(200, 10)]),
        # a pending NoData on an own-mask source: the masked pixels (columns < 150) take it,
        # so the masked point reads None where the raw pixel has a value
        # a window running past the file's edge: not the point view (it would read past
        # the source), the window materialized as open_tile does
        "window past the edge": (
            _tile(plain, window=(600, 500, 200, 200)),
            [_centre(650, 550), _centre(699, 599)],
        ),
        "own mask + pending nodata": (
            _tile(str(masked), metadata={"pending_nodata": "255"}),
            [_centre(10, 10), _centre(200, 10)],
        ),
        "clip": (
            _tile(plain, clip_polygon=clip, clip_crs="EPSG:32633"),
            [_centre(350, 300), _centre(100, 100)],
        ),
        "warp target": (_tile(plain, crs="EPSG:4326"), [(lon[0], lat[0], 4326)]),
    }


@pytest.mark.parametrize(
    "case",
    [
        "whole file",
        "sub-window",
        "outside the file",
        "band select",
        "pending nodata",
        "pending nodata auto",
        "reprojected point",
        "pending srid",
        "own mask",
        "own mask + pending nodata",
        "window past the edge",
        "clip",
        "warp target",
    ],
)
def test_a_point_read_matches_the_materialized_tile(
    tmp_path, in_one_task, counters, case
):
    """The values of a virtual tile at its points are those of the same tile materialized
    (its window decoded, pending instructions, clip and warp applied, as bytes). A clip, a
    reprojection or an own mask band takes the window, materialized, itself."""
    tile, points = _matrix_cases(tmp_path)[case]
    mat = ot.materialize_to_bytes(VirtualTile.from_row(tile)).to_row()
    geoms = [_wkb(*p[:2], srid=p[2] if len(p) > 2 else None) for p in points]
    want = [SAMPLE(mat, g) for g in geoms]
    counters.update(encodes=0)
    got = [SAMPLE(tile, g) for g in geoms]
    assert got == want
    fallbacks = (
        "own mask",
        "own mask + pending nodata",
        "window past the edge",
        "clip",
        "warp target",
    )
    assert (counters["encodes"] > 0) == (case in fallbacks)
    if case == "own mask + pending nodata":
        assert want[0] == [None] and None not in want[1]  # the mask decides the value
    if case != "outside the file":
        assert any(v is not None and None not in v for v in want), "a vacuous case"


def test_a_task_shares_its_open_sources_and_closes_them_when_it_ends(
    tmp_path, in_one_task, counters
):
    """Within a task the source of a virtual tile is opened once; the next task opens it
    again and the last task's handle is closed. Outside a task each call opens and closes.
    """
    data = np.arange(64 * 64, dtype="float32").reshape(1, 64, 64)
    tile = _tile(_write(tmp_path / "a.tif", data, block=16))
    pt = _wkb(*_centre(3, 4))
    counters.update(opens=0)

    for _ in range(3):
        assert SAMPLE(tile, pt) == [float(4 * 64 + 3)]
    assert counters["opens"] == 1
    held = list(ot._TASK_SOURCES._lru._store.values())[0][0]

    in_one_task(2)
    assert SAMPLE(tile, pt) == [float(4 * 64 + 3)]
    assert counters["opens"] == 2 and held.closed

    in_one_task(None)  # not in a Spark task: nothing is kept open
    assert SAMPLE(tile, pt) == [float(4 * 64 + 3)]
    assert SAMPLE(tile, pt) == [float(4 * 64 + 3)]
    assert counters["opens"] == 4
    assert ot._TASK_SOURCES._lru is None


def test_in_a_spark_task_the_points_of_a_tile_share_one_open(spark, tmp_path):
    """The real task context (no stand-in): 40 points of one virtual tile in one task
    open its source once; the values are the file's."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    data = np.arange(128 * 128, dtype="float32").reshape(1, 128, 128)
    tile = _tile(_write(tmp_path / "t.tif", data, block=32))
    cells = [(i % 128, (7 * i) % 128) for i in range(40)]
    rows = [(tile, _wkb(*_centre(c, r)), c, r) for c, r in cells]
    schema = "tile {}, g BINARY, c INT, r INT".format(V2_TILE_SCHEMA.simpleString())
    df = spark.createDataFrame(rows, schema).coalesce(1)

    @F.udf(
        T.StructType(
            [T.StructField("v", T.DoubleType()), T.StructField("opens", T.LongType())]
        )
    )
    def sample_and_opens(t, g):
        from databricks.labs.gbx.pyrx import functions as _prx
        from databricks.labs.gbx.pyrx.core import open_tile as _ot

        v = _prx._sample_udf.func(t, g)
        return (v[0], _ot._TASK_SOURCES._lru.opens)

    out = df.select("c", "r", sample_and_opens("tile", "g").alias("s")).collect()
    assert sorted((o["c"], o["r"], o["s"]["v"]) for o in out) == sorted(
        (c, r, float(data[0, r, c])) for c, r in cells
    )
    assert max(o["s"]["opens"] for o in out) == 1


@pytest.fixture
def staged(tmp_path, monkeypatch):
    """Every Volume probe fails, so each source is copied to local disk: the copies go to
    the returned directory (``GBX_LOCAL_TEMP_DIR``)."""
    from databricks.labs.gbx.ds import file_gbx

    def _no_direct_reads(path):
        raise OSError("no random access")

    stage = tmp_path / "stage"
    stage.mkdir()
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(stage))
    monkeypatch.setattr(file_gbx, "_is_fuse_path", lambda p: True)
    monkeypatch.setattr(file_gbx, "_probe_direct_open", _no_direct_reads)
    return stage


def test_a_task_leaves_no_local_copy_on_disk(tmp_path, in_one_task, staged):
    """A source the probe could not read directly is copied to local disk; the task's
    handle on the copy reads it, but the copy is no longer on disk (unlinked once open), so
    its space comes back when the handle closes: on eviction (the 5th source of 4 slots),
    at the next task, or however the worker exits."""
    data = np.arange(32 * 32, dtype="float32").reshape(1, 32, 32)
    tiles = [
        _tile(_write(tmp_path / f"s{i}.tif", data + i, block=16)) for i in range(5)
    ]
    pt = _wkb(*_centre(3, 2))
    for i, tile in enumerate(tiles):
        assert SAMPLE(tile, pt) == [float(2 * 32 + 3 + i)]
        assert sorted(os.listdir(staged)) == []
        if i == 0:
            first = list(ot._TASK_SOURCES._lru._store.values())[0][0]
    assert first.closed  # evicted: its copy's space is released
    in_one_task(2)
    assert SAMPLE(tiles[4], pt) == [float(2 * 32 + 3 + 4)]
    assert sorted(os.listdir(staged)) == []


def test_a_worker_that_ends_with_os_exit_leaves_no_local_copy(tmp_path, staged):
    """A PySpark worker ends with ``os._exit`` (``pyspark/daemon.py``), which runs no
    ``atexit`` handler: the copy a task made must already be off the disk."""
    import subprocess
    import sys

    path = _write(tmp_path / "w.tif", np.ones((1, 16, 16), "float32"), block=16)
    code = "\n".join(
        [
            "import os, shapely",
            "from databricks.labs.gbx.ds import file_gbx",
            "from databricks.labs.gbx.pyrx import functions as prx",
            "from databricks.labs.gbx.pyrx.core import open_tile as ot",
            "def _no(p):",
            "    raise OSError('no random access')",
            "file_gbx._is_fuse_path = lambda p: True",
            "file_gbx._probe_direct_open = _no",
            "ot._task_attempt_id = lambda: 7",
            f"tile = {_tile(path)!r}",
            "pt = shapely.to_wkb(shapely.Point(500005.0, 4999995.0))",
            "print(prx._sample_udf.func(tile, pt), flush=True)",
            "os._exit(0)",
        ]
    )
    env = dict(os.environ, GBX_LOCAL_TEMP_DIR=str(staged))
    out = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )
    assert out.returncode == 0, out.stderr[-2000:]
    assert out.stdout.strip() == "[1.0]"
    assert sorted(os.listdir(staged)) == []


def test_a_failed_open_leaves_no_local_copy(tmp_path, in_one_task, staged):
    """A Volume file the probe cannot read is copied to local disk; when it is no raster
    at all the open of the copy raises, and the copy is removed with it (once per Spark
    attempt otherwise, with no cleanup when the worker ends)."""
    bad = tmp_path / "bad.tif"
    bad.write_bytes(b"not a raster" * 100)
    with pytest.raises(rasterio.errors.RasterioIOError):
        SAMPLE(_tile(str(bad)), _wkb(*_centre(1, 1)))
    assert sorted(os.listdir(staged)) == []


def test_a_copy_that_cannot_be_removed_while_open_is_removed_on_close(
    tmp_path, in_one_task, staged, monkeypatch
):
    """Where an open file cannot be removed (Windows), the copy stays on disk while the
    task holds it and is removed when the handle closes."""
    from databricks.labs.gbx.ds import file_gbx

    real_remove = os.remove
    refused = []

    def _remove(path):
        if not refused:  # the remove right after the open
            refused.append(path)
            raise PermissionError("in use")
        return real_remove(path)

    monkeypatch.setattr(file_gbx.os, "remove", _remove)
    data = np.arange(16 * 16, dtype="float32").reshape(1, 16, 16)
    tile = _tile(_write(tmp_path / "c.tif", data, block=16))
    assert SAMPLE(tile, _wkb(*_centre(2, 3))) == [float(3 * 16 + 2)]
    assert len(refused) == 1 and sorted(os.listdir(staged)) == [
        os.path.basename(refused[0])
    ]
    ot._TASK_SOURCES.reset()
    assert sorted(os.listdir(staged)) == []


def test_an_interrupted_open_leaves_no_local_copy(
    tmp_path, in_one_task, staged, monkeypatch
):
    """The staged copy goes when its open is interrupted too (a BaseException such as
    KeyboardInterrupt, not only an Exception)."""
    from databricks.labs.gbx.pyrx import _env

    def _interrupted(*a, **kw):
        raise KeyboardInterrupt

    monkeypatch.setattr(_env, "open_raster", _interrupted)
    tile = _tile(_write(tmp_path / "k.tif", np.ones((1, 16, 16), "float32"), block=16))
    with pytest.raises(KeyboardInterrupt):
        SAMPLE(tile, _wkb(*_centre(1, 1)))
    assert sorted(os.listdir(staged)) == []


def _netcdf(tmp_path, name="n.nc"):
    """A one-variable netCDF file (``Band1``) of a small float32 ramp, via GDAL CreateCopy."""
    import rasterio.shutil

    tif = _write(
        tmp_path / f"{name}.src.tif",
        np.arange(16 * 16, dtype="float32").reshape(1, 16, 16),
        block=16,
    )
    out = tmp_path / name
    rasterio.shutil.copy(tif, str(out), driver="netCDF")
    os.remove(tif)
    return str(out)


def test_a_staged_netcdf_copy_stays_on_disk_for_a_reopen_by_name(tmp_path, staged):
    """Only a GTiff/COG copy is removed from disk once open: a netCDF dataset may be opened
    again by name (its subdataset, NETCDF:"<path>":var), so its copy stays until the handle
    closes."""
    from databricks.labs.gbx.ds.file_gbx import _OpenerContext

    path = _netcdf(tmp_path)
    ctx = _OpenerContext(unlink_staged=True)
    ds = ctx.open(path)
    try:
        assert ds.driver == "netCDF" and len(os.listdir(staged)) == 1
        with rasterio.open(f'NETCDF:"{ds.name}":Band1') as sub:
            assert sub.shape == (16, 16)
    finally:
        ctx.close(ds)
    assert os.listdir(staged) == []


def test_a_staged_gtiff_copy_is_unlinked_at_open_and_weighed_by_its_size(
    tmp_path, staged
):
    """A GTiff copy leaves the disk at open, and the LRU still weighs it by its size."""
    from databricks.labs.gbx.ds.file_gbx import _OpenerContext

    path = _write(tmp_path / "g.tif", np.ones((1, 64, 64), "float32"), block=32)
    ctx = _OpenerContext(unlink_staged=True)
    ds = ctx.open(path)
    try:
        assert ds.driver == "GTiff" and os.listdir(staged) == []
        assert ctx.weigh(ds, path) == os.path.getsize(path)
    finally:
        ctx.close(ds)


def test_no_netcdf_copy_is_left_after_the_task_boundary(tmp_path, in_one_task, staged):
    """The netCDF copy the task cache keeps is removed when the next task attempt starts."""
    nc = _tile(_netcdf(tmp_path))
    gt = _tile(_write(tmp_path / "g.tif", np.ones((1, 16, 16), "float32"), block=16))
    assert SAMPLE(nc, _wkb(*_centre(2, 3))) == [float(3 * 16 + 2)]
    assert len(os.listdir(staged)) == 1  # kept while the task holds the handle
    in_one_task(2)
    assert SAMPLE(gt, _wkb(*_centre(2, 3))) == [1.0]
    assert (
        os.listdir(staged) == []
    )  # the netCDF copy closed with task 1; the GTiff unlinked
