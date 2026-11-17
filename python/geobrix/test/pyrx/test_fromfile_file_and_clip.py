import os
from test._cog_fixture import cog_bytes as _cog_bytes

import numpy as np
import shapely.geometry as sg
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.functions import _fromfile_impl


def _write(tmp_path, blob):
    p = tmp_path / "scene.tif"
    p.write_bytes(blob)
    return str(p)


def _arr(tile):
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as ds:
        return ds.read(), ds.width, ds.height


def test_file_value_reads_like_path(tmp_path):
    p = _write(tmp_path, _cog_bytes(width=600, height=500))
    a = _fromfile_impl(p, "GTiff", True)
    b = _fromfile_impl({"uri": "dbfs:" + p}, "GTiff", True)
    assert np.array_equal(_arr(a)[0], _arr(b)[0])


def test_file_object_reads_like_path(tmp_path):
    p = _write(tmp_path, _cog_bytes(width=600, height=500))
    ref = type("FileRef", (), {"uri": "dbfs:" + p})()
    assert np.array_equal(
        _arr(_fromfile_impl(p, "GTiff", True))[0],
        _arr(_fromfile_impl(ref, "GTiff", True))[0],
    )


def test_geom_clips(tmp_path):
    p = _write(tmp_path, _cog_bytes())
    t = _fromfile_impl(
        p, "GTiff", True, geom=sg.box(405000, 3990000, 412000, 3996000).wkb
    )
    _, w, h = _arr(t)
    assert (w, h) == (700, 600)


def test_geom_clips_like_rst_clip(tmp_path):
    from databricks.labs.gbx.pyrx.core.edit import clip_to_geom

    blob = _cog_bytes()
    p = _write(tmp_path, blob)
    poly = sg.Polygon([(405000, 3990000), (412000, 3991000), (409000, 3996000)])
    got = _fromfile_impl(p, "GTiff", True, geom=poly.wkt)
    with MemoryFile(blob) as mf, mf.open() as ds:
        want = clip_to_geom(ds, poly, False)
    with MemoryFile(want) as mf, mf.open() as ds:
        assert np.array_equal(_arr(got)[0], ds.read())


def test_geom_in_clip_crs(tmp_path):
    from pyproj import Transformer

    p = _write(tmp_path, _cog_bytes())
    t = Transformer.from_crs("EPSG:32614", "EPSG:4326", always_xy=True)
    box = sg.box(*t.transform(405000, 3990000), *t.transform(412000, 3996000))
    got = _fromfile_impl(p, "GTiff", True, geom=box.wkb, clip_crs="EPSG:4326")
    assert got is not None and _arr(got)[1] > 0


def test_embedded_srid_wins_over_clip_crs(tmp_path):
    import shapely

    p = _write(tmp_path, _cog_bytes())
    box = shapely.set_srid(sg.box(405000, 3990000, 412000, 3996000), 32614)
    ewkb = shapely.to_wkb(box, include_srid=True)
    # clip_crs says 4326, the EWKB says 32614: the SRID wins, so the box lands on the raster
    got = _fromfile_impl(p, "GTiff", True, geom=ewkb, clip_crs="EPSG:4326")
    assert got is not None and _arr(got)[1:] == (700, 600)


class _GeometryValue:
    """Stand-in for ``pyspark.sql.types.Geometry``, as a Python UDF receives a GEOMETRY column (measured on Serverless env 6):
    ``getBytes()`` is the WKB without the SRID and ``getSrid()`` the SRID."""

    def __init__(self, geom, srid):
        self._wkb, self._srid = geom.wkb, srid

    def getBytes(self):  # noqa: N802
        return self._wkb

    def getSrid(self):  # noqa: N802
        return self._srid


def test_a_geometry_value_clips_like_its_ewkb(tmp_path):
    import shapely

    p = _write(tmp_path, _cog_bytes())
    box = sg.box(405000, 3990000, 412000, 3996000)
    want = _fromfile_impl(
        p,
        "GTiff",
        True,
        geom=shapely.to_wkb(shapely.set_srid(box, 32614), include_srid=True),
    )
    got = _fromfile_impl(p, "GTiff", True, geom=_GeometryValue(box, 32614))
    assert got is not None and _arr(got)[1:] == (700, 600)
    assert np.array_equal(_arr(got)[0], _arr(want)[0])


def test_a_geometry_value_carries_its_srid_over_clip_crs(tmp_path):
    from pyproj import Transformer

    p = _write(tmp_path, _cog_bytes())
    t = Transformer.from_crs("EPSG:32614", "EPSG:4326", always_xy=True)
    box = sg.box(*t.transform(405000, 3990000), *t.transform(412000, 3996000))
    # the value is in 4326 and says so; clip_crs names another CRS, which must lose
    got = _fromfile_impl(
        p, "GTiff", True, geom=_GeometryValue(box, 4326), clip_crs="EPSG:32614"
    )
    assert got is not None and _arr(got)[1] > 0


def test_resolution_reads_overview(tmp_path):
    p = _write(tmp_path, _cog_bytes())
    t = _fromfile_impl(p, "GTiff", True, resolution=40.0)
    assert _arr(t)[1:] == (500, 375)  # 2000x1500 at factor 4


def test_geom_and_resolution_together(tmp_path):
    p = _write(tmp_path, _cog_bytes())
    t = _fromfile_impl(
        p,
        "GTiff",
        True,
        geom=sg.box(405000, 3990000, 413000, 3996000).wkb,
        resolution=40.0,
    )
    _, w, h = _arr(t)
    assert (w, h) == (200, 150)  # 8000 x 6000 m at 40 m


def test_geom_outside_is_null(tmp_path):
    p = _write(tmp_path, _cog_bytes())
    assert _fromfile_impl(p, "GTiff", True, geom=sg.box(0, 0, 1, 1).wkb) is None


def test_geom_gives_a_materialized_tile_even_when_virtual_requested(tmp_path):
    p = _write(tmp_path, _cog_bytes())
    t = _fromfile_impl(
        p, "GTiff", False, geom=sg.box(405000, 3990000, 412000, 3996000).wkb
    )
    assert t["raster"] is not None


def test_null_geom_and_resolution_keep_the_old_behaviour(tmp_path):
    p = _write(tmp_path, _cog_bytes(width=600, height=500))
    v = _fromfile_impl(p, "GTiff", False, geom=None, clip_crs=None, resolution=None)
    assert v["raster"] is None and v["path"] == p


# --- the routes a clip takes besides a plain local file ----------------------------------------------------------------

from test.pyrx.test_fromfile_vrt import _vrt_over  # noqa: E402

import pytest  # noqa: E402
import rasterio  # noqa: E402

from databricks.labs.gbx import _gdal_policy  # noqa: E402
from databricks.labs.gbx.ds.file_gbx import StageTooLargeError  # noqa: E402
from databricks.labs.gbx.pyrx import functions as prx  # noqa: E402

BOX = sg.box(401000, 3996000, 404000, 3999000)  # 300 x 300 pixels of a 600 x 500 scene


def _error_of(row):
    assert row is not None and row["raster"] is None and row["path"] is None
    return row["metadata"]["last_error"]


def _kernel_pixels(path, poly):
    from databricks.labs.gbx.pyrx.core.edit import clip_to_geom

    with rasterio.open(path) as ds:
        out = clip_to_geom(ds, poly, False)
    with MemoryFile(out) as mf, mf.open() as ds:
        return ds.read(), ds.nodata


def test_a_local_vrt_clips_like_the_kernel_on_a_full_read(tmp_path):
    tif = _write(tmp_path, _cog_bytes(width=600, height=500))
    vrt = _vrt_over(tif, str(tmp_path / "scene.vrt"))
    got = _fromfile_impl(str(vrt), "GTiff", True, geom=BOX.wkb)
    want, _ = _kernel_pixels(str(vrt), BOX)
    assert np.array_equal(_arr(got)[0], want)
    assert _arr(got)[1:] == (300, 300)


def test_a_file_with_an_aux_xml_sidecar_clips_with_its_nodata(tmp_path):
    p = _write(tmp_path, _cog_bytes(width=600, height=500, nodata=None))
    (tmp_path / "scene.tif.aux.xml").write_text(
        '<PAMDataset><PAMRasterBand band="1"><NoDataValue>7</NoDataValue></PAMRasterBand></PAMDataset>'
    )
    got = _fromfile_impl(p, "GTiff", True, geom=BOX.wkb)
    want, nodata = _kernel_pixels(p, BOX)
    assert nodata == 7.0  # the sidecar's NoData is what the kernel sees too
    with MemoryFile(got["raster"]) as mf, mf.open() as ds:
        assert ds.nodata == 7.0
        assert np.array_equal(ds.read(), want)


def test_an_unreadable_geom_is_an_error_tile_with_a_fix(tmp_path):
    p = _write(tmp_path, _cog_bytes(width=600, height=500))
    for bad in ("not a geometry", b"\x00\x01\x02"):
        msg = _error_of(_fromfile_impl(p, "GTiff", True, geom=bad))
        assert "the geom cannot be used" in msg and "Fix:" in msg, msg


def test_a_bad_clip_crs_is_an_error_tile_with_a_fix(tmp_path):
    p = _write(tmp_path, _cog_bytes(width=600, height=500))
    msg = _error_of(
        _fromfile_impl(p, "GTiff", True, geom=BOX.wkt, clip_crs="NOT A CRS")
    )
    assert "the geom cannot be used" in msg and "Fix:" in msg, msg


def test_a_window_over_the_cap_is_refused_with_a_fix(tmp_path):
    p = _write(tmp_path, _cog_bytes(width=600, height=500))
    with pytest.raises(StageTooLargeError, match="Fix:"):
        _fromfile_impl(p, "GTiff", True, cap_bytes=1024, geom=BOX.wkb)
    with pytest.raises(StageTooLargeError, match="Fix:"):
        _fromfile_impl(p, "GTiff", True, cap_bytes=1024, resolution=10.0)


def _plain_tif(tmp_path, name="plain.tif", w=400, h=300):
    p = str(tmp_path / name)
    data = (np.arange(w * h, dtype="uint16").reshape(1, h, w) % 251).astype("uint16")
    with rasterio.open(
        p, "w", driver="GTiff", width=w, height=h, count=1, dtype="uint16",
        crs="EPSG:32614", transform=from_origin(400000, 4000000, 10, 10),
    ) as dst:  # fmt: skip
        dst.write(data)
    return p


def _external_overviews(path):
    gdal = pytest.importorskip("osgeo.gdal")
    ds = gdal.Open(path)  # read-only: GDAL writes the overviews to a .ovr beside it
    ds.BuildOverviews("NEAREST", [2, 4])
    ds = None
    assert os.path.exists(path + ".ovr")


def test_a_resolution_read_uses_an_external_ovr_beside_the_file(tmp_path):
    p = _plain_tif(tmp_path)
    _external_overviews(p)
    assert _arr(_fromfile_impl(p, "GTiff", True, resolution=40.0))[1:] == (100, 75)


def test_a_resolution_read_of_a_staged_copy_keeps_the_external_ovr(
    tmp_path, monkeypatch
):
    from databricks.labs.gbx.ds import file_gbx

    p = _plain_tif(tmp_path)
    _external_overviews(p)
    monkeypatch.setattr(file_gbx, "_is_fuse_path", lambda path: True)
    monkeypatch.setenv("GBX_FORCE_STAGE", "1")  # the copy fallback of a Volume path
    assert _arr(_fromfile_impl(p, "GTiff", True, resolution=40.0))[1:] == (100, 75)


def test_a_resolution_read_of_a_file_with_sidecars_keeps_the_external_ovr(tmp_path):
    p = _plain_tif(tmp_path)
    _external_overviews(p)
    (tmp_path / "plain.tif.aux.xml").write_text(
        '<PAMDataset><PAMRasterBand band="1"><NoDataValue>9</NoDataValue></PAMRasterBand></PAMDataset>'
    )
    got = _fromfile_impl(p, "GTiff", True, resolution=40.0)
    assert _arr(got)[1:] == (100, 75)


# GDAL holds the GIL while it opens a URL, so the server runs in its own process (an in-process one never answers)
_SERVER = """
import http.server, os, sys
root = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def _send(self, head_only):
        path = os.path.join(root, self.path.lstrip("/"))
        if not os.path.isfile(path):
            self.send_response(404); self.end_headers(); return
        data = open(path, "rb").read()
        rng = self.headers.get("Range")
        part, status = data, 200
        if rng and rng.startswith("bytes="):
            a, _, b = rng[6:].partition("-")
            a = int(a)
            b = min(int(b) if b else len(data) - 1, len(data) - 1)
            part, status = data[a : b + 1], 206
        self.send_response(status)
        self.send_header("Content-Length", str(len(part)))
        self.send_header("Accept-Ranges", "bytes")
        if status == 206:
            self.send_header("Content-Range", "bytes %d-%d/%d" % (a, b, len(data)))
        self.end_headers()
        if not head_only:
            self.wfile.write(part)
    def do_GET(self): self._send(False)
    def do_HEAD(self): self._send(True)
    def log_message(self, *a): pass
srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
print(srv.server_address[1], flush=True)
srv.serve_forever()
"""


class _Http:
    """A local HTTP server for GDAL's /vsicurl/: HEAD, GET and byte ranges of the files of a folder."""

    def __init__(self, folder):
        import subprocess
        import sys

        self.proc = subprocess.Popen(
            [sys.executable, "-c", _SERVER, str(folder)],
            stdout=subprocess.PIPE,
            text=True,
        )
        self.base = f"http://127.0.0.1:{int(self.proc.stdout.readline())}"

    def close(self):
        self.proc.kill()
        self.proc.wait()


_REMOTE_VRT = (
    b'<VRTDataset rasterXSize="600" rasterYSize="500"><GeoTransform>400000,10,0,4000000,0,-10</GeoTransform><VRTRasterBand dataType="Byte" band="1">'
    b'<SimpleSource><SourceFilename relativeToVRT="0">/vsicurl/http://127.0.0.1:9/x.tif'
    b"</SourceFilename><SourceBand>1</SourceBand></SimpleSource></VRTRasterBand></VRTDataset>"
)


@pytest.fixture
def remote(tmp_path):
    (tmp_path / "scene.vrt").write_bytes(_REMOTE_VRT)
    (tmp_path / "scene.tif").write_bytes(_cog_bytes(600, 500))
    server = _Http(tmp_path)
    try:
        with _gdal_policy.carried(True):
            yield server
    finally:
        server.close()


@pytest.mark.parametrize(
    "kw",
    [{"geom": BOX.wkb}, {"resolution": 40.0}, {"geom": BOX.wkb, "resolution": 40.0}],
)
def test_a_remote_vrt_is_refused_on_every_clip_route(remote, kw):
    msg = _error_of(_fromfile_impl(remote.base + "/scene.vrt", "GTiff", True, **kw))
    assert "Fix:" in msg, msg


def test_a_remote_geotiff_clips_and_reads_an_overview(remote):
    url = remote.base + "/scene.tif"
    got = _fromfile_impl(url, "GTiff", True, geom=BOX.wkb)
    assert _arr(got)[1:] == (300, 300)
    assert _arr(_fromfile_impl(url, "GTiff", True, resolution=40.0))[1:] == (150, 125)


def test_the_overview_reopen_of_a_remote_source_is_judged_again_and_pinned(
    tmp_path, monkeypatch
):
    """A remote source that is a GeoTIFF at the first open and a VRT at the second is never read: the reopen names
    the driver the first open reported, and is refused when it is a VRT."""
    p = _write(tmp_path, _cog_bytes(width=600, height=500))
    opened = []

    class _Swapped:
        driver = "VRT"

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    real = prx.open_data

    def fake(target, *a, **kw):
        opened.append(kw)
        return real(p) if len(opened) == 1 else _Swapped()

    monkeypatch.setattr(prx, "open_data", fake)
    with pytest.raises(_gdal_policy.DriverPolicyError, match="Fix:"):
        prx._fromfile_clip_target(
            "http://example.invalid/a.tif", "GTiff", None, None, 40.0, None
        )
    assert len(opened) == 2
    assert opened[1].get("driver") == "GTiff" and opened[1].get("overview_level") == 1


# --- a geom column through the Python API and the SQL binding ---------------------------------------------------------


def _clip_frame(spark, tmp_path):
    p = _write(tmp_path, _cog_bytes())
    box = sg.box(405000, 3990000, 412000, 3996000)  # 700 x 600 pixels at 10 m
    return spark.createDataFrame([(p, box.wkb)], ["path", "g"])


@pytest.mark.parametrize("by", ["name", "col"])
def test_the_python_api_reads_geom_as_a_column(spark, tmp_path, by):
    from pyspark.sql import functions as F

    from databricks.labs.gbx.rasterx import functions as rx

    df = _clip_frame(spark, tmp_path)
    geom = "g" if by == "name" else F.col("g")
    tile = df.select(rx.rst_fromfile("path", geom=geom).alias("t")).first()["t"]
    assert tile is not None and tile["raster"] is not None, tile
    assert _arr(tile)[1:] == (700, 600)


def test_the_sql_binding_reads_a_geom_column(spark, tmp_path):
    from databricks.labs.gbx import register

    register(spark, only=["gbx_rst_fromfile"])
    _clip_frame(spark, tmp_path).createOrReplaceTempView("fromfile_geoms")
    tile = spark.sql(
        "SELECT gbx_rst_fromfile(path, 'GTiff', g) AS t FROM fromfile_geoms"
    ).first()["t"]
    assert tile is not None and tile["raster"] is not None, tile
    assert _arr(tile)[1:] == (700, 600)


# --- a FILE value with an offset: the slice is the raster, as the Unity Catalog gate row uc_fromfile_offset_file ------

from databricks.labs.gbx.bench.oracle_tables import (  # noqa: E402
    raster_constructor as _rc,
)


def _prefixed(tmp_path):
    """cog_prefixed.tif of the UC gate (examples/tests/uc_fixtures.py): FIXTURE_PREFIX_BYTES of other bytes, then
    cog.tif. Returns (path, the FILE value that names the cog.tif slice)."""
    n = _rc.FIXTURE_PREFIX_BYTES
    cog = _cog_bytes()
    p = tmp_path / "cog_prefixed.tif"
    p.write_bytes((bytes(range(256)) * (n // 256 + 1))[:n] + cog)
    return str(p), {"uri": "dbfs:" + str(p), "offset": n, "size": len(cog)}


def _band_means(tile):
    data, _, _ = _arr(tile)
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as ds:
        masked = ds.read(masked=True)
    return [float(masked[b].mean()) for b in range(data.shape[0])]


def test_an_offset_file_clips_like_the_uc_gate_row(tmp_path):
    _, value = _prefixed(tmp_path)
    got = _fromfile_impl(value, "GTiff", True, geom=sg.Polygon(
        [(405000, 3990000), (412000, 3990000), (412000, 3996000), (405000, 3996000)]
    ).wkb)  # fmt: skip
    assert got is not None and got["raster"] is not None, got
    assert _arr(got)[1:] == (700, 600)
    assert np.allclose(_band_means(got), _rc._clip_avg(), rtol=1e-6)


def test_an_offset_file_reads_whole_like_its_slice(tmp_path):
    _, value = _prefixed(tmp_path)
    whole = _fromfile_impl(value, "GTiff", True)
    assert whole is not None and whole["raster"] is not None, whole
    assert np.array_equal(_arr(whole)[0], _rc.cog_pixels())
    virtual = _fromfile_impl(value, "GTiff", False)
    assert virtual["raster"] is None and virtual["path"].startswith("/vsisubfile/")
    assert (virtual["window"]["width"], virtual["window"]["height"]) == (2000, 1500)
    with rasterio.open(virtual["path"]) as ds:
        assert np.array_equal(
            ds.read(1, window=((0, 4), (0, 4))), _rc.cog_pixels()[0, :4, :4]
        )


def test_an_offset_file_reads_an_overview_of_its_slice(tmp_path):
    _, value = _prefixed(tmp_path)
    assert _arr(_fromfile_impl(value, "GTiff", True, resolution=40.0))[1:] == (500, 375)


def test_the_same_path_without_the_offset_is_not_that_raster(tmp_path):
    path, value = _prefixed(tmp_path)
    assert (
        _fromfile_impl({"uri": value["uri"]}, "GTiff", True) is None
    )  # the prefix is no TIFF
    assert _fromfile_impl({**value, "offset": 0}, "GTiff", True) is None


def test_a_remote_offset_file_is_judged_by_its_url(remote, tmp_path, monkeypatch):
    n = 100
    (remote_dir := tmp_path / "srv").mkdir()
    (remote_dir / "pre.vrt").write_bytes(b"\0" * n + _REMOTE_VRT)
    (remote_dir / "pre.tif").write_bytes(b"\0" * n + _cog_bytes(600, 500))
    server = _Http(remote_dir)
    try:
        size = len(_REMOTE_VRT)
        vrt = {"uri": server.base + "/pre.vrt", "offset": n, "size": size}
        for kw in ({"geom": BOX.wkb}, {}):
            msg = _error_of(_fromfile_impl(vrt, "GTiff", True, **kw))
            assert "Fix:" in msg, msg
        tif = {
            "uri": server.base + "/pre.tif",
            "offset": n,
            "size": len(_cog_bytes(600, 500)),
        }
        assert _arr(_fromfile_impl(tif, "GTiff", True, geom=BOX.wkb))[1:] == (300, 300)
    finally:
        server.close()


def test_an_offset_file_at_a_remote_url_is_refused_without_the_switch(monkeypatch):
    monkeypatch.delenv(_gdal_policy.REMOTE_ENV, raising=False)
    with pytest.raises(_gdal_policy.RemotePathError):
        _fromfile_impl({"uri": "https://h.invalid/a.tif", "offset": 10}, "GTiff", True)


# --- a slice on a FUSE path is read in place; the FILE value's size follows Unity Catalog --------------------------


def test_an_offset_file_on_a_volume_is_read_in_place_and_fast(tmp_path, monkeypatch):
    """On a Volume (``_is_fuse_path``) the slice is opened in place through /vsisubfile/: the raw object, whose first
    bytes are no raster, is never probed (10 transient retries) nor copied whole."""
    import time

    from databricks.labs.gbx.ds import file_gbx
    from databricks.labs.gbx.pyrx.core import local_temp

    _, value = _prefixed(tmp_path)
    monkeypatch.setattr(file_gbx, "_is_fuse_path", lambda path: True)
    staged = []

    def _no_stage(*a, **k):
        staged.append(a)
        raise AssertionError("the raw object was staged")

    monkeypatch.setattr(file_gbx, "_stage_local_if_needed", _no_stage)
    monkeypatch.setattr(file_gbx, "copy_source_with_sidecars", _no_stage)
    monkeypatch.setattr(local_temp, "copy_source_with_sidecars", _no_stage)
    box = sg.box(405000, 3990000, 412000, 3996000).wkb
    t0 = time.monotonic()
    whole = _fromfile_impl(value, "GTiff", True)
    clip = _fromfile_impl(value, "GTiff", True, geom=box)
    assert time.monotonic() - t0 < 2.0
    assert whole is not None and whole["raster"] is not None, whole
    assert clip is not None and _arr(clip)[1:] == (700, 600), clip
    assert not staged


def test_a_volume_slice_that_cannot_open_in_place_reads_a_copy_of_the_slice_only(
    tmp_path, monkeypatch
):
    from databricks.labs.gbx.ds import file_gbx

    _, value = _prefixed(tmp_path)
    monkeypatch.setattr(file_gbx, "_is_fuse_path", lambda path: True)
    real = prx.open_data
    seen = []

    def _no_subfile(target, *a, **k):  # as a mount that serves no random reads
        if str(target).startswith("/vsisubfile/"):
            raise rasterio.errors.RasterioIOError("no random reads here")
        seen.append(os.path.getsize(target))
        return real(target, *a, **k)

    monkeypatch.setattr(prx, "open_data", _no_subfile)
    whole = _fromfile_impl(value, "GTiff", True)
    assert np.array_equal(_arr(whole)[0], _rc.cog_pixels())
    assert seen and set(seen) == {
        value["size"]
    }  # the copy holds the slice, not the object


def test_offset_zero_with_a_smaller_size_reads_only_that_slice(tmp_path):
    cog = _cog_bytes()
    p = tmp_path / "cog_trailer.tif"
    p.write_bytes(cog + b"\0" * 5000)  # the object runs past the FILE's size
    value = {"uri": "dbfs:" + str(p), "offset": 0, "size": len(cog)}
    virtual = _fromfile_impl(value, "GTiff", False)
    assert virtual["path"] == f"/vsisubfile/0_{len(cog)},{p}"
    assert np.array_equal(
        _arr(_fromfile_impl(value, "GTiff", True))[0], _rc.cog_pixels()
    )


@pytest.mark.parametrize(
    "span",
    [
        {"offset": 12345},
        {"offset": 12345, "size": 0},
        {"offset": 12345, "size": -1},
        {"size": 0},
    ],
    ids=["offset-no-size", "offset-size-0", "offset-size-negative", "size-0"],
)
def test_a_file_value_with_no_positive_size_for_its_slice_is_null(tmp_path, span):
    _, value = _prefixed(tmp_path)
    got = {"uri": value["uri"], **span}
    assert _fromfile_impl(got, "GTiff", True) is None
    assert _fromfile_impl(got, "GTiff", False) is None
    assert _fromfile_impl(got, "GTiff", True, geom=BOX.wkb) is None


@pytest.mark.parametrize("size", ["whole", None], ids=["its-own-size", "no-size"])
def test_a_plain_file_value_keeps_the_plain_path(tmp_path, size):
    p = _write(tmp_path, _cog_bytes(width=600, height=500))
    value = {"uri": "dbfs:" + p, "offset": 0}
    if size == "whole":
        value["size"] = os.path.getsize(p)
    virtual = _fromfile_impl(value, "GTiff", False)
    assert virtual["path"] == p  # no /vsisubfile/


def test_a_virtual_slice_tile_feeds_an_osgeo_and_a_rasterio_function(spark, tmp_path):
    """A FILE struct column with an offset gives a virtual tile over /vsisubfile/; gbx_rst_summary (the GDAL engine,
    osgeo's gdalinfo) and gbx_rst_avg (rasterio) both read it as the slice."""
    import json

    from databricks.labs.gbx import _osgeo

    try:
        _osgeo.bindings()
    except (
        _osgeo.OsgeoUnavailable
    ) as exc:  # the GDAL engine needs the env wheel's osgeo (the dev container has it)
        pytest.skip(str(exc))

    from pyspark.sql import functions as F

    from databricks.labs.gbx.rasterx import functions as rx

    _, value = _prefixed(tmp_path)
    df = spark.createDataFrame(
        [(value["uri"], value["offset"], value["size"])], ["uri", "offset", "size"]
    ).select(F.struct("uri", "offset", "size").alias("file"))
    row = (
        df.select(rx.rst_fromfile("file").alias("t"))
        .select(
            F.col("t.path").alias("path"),
            rx.rst_summary("t").alias("summary"),
            rx.rst_avg("t").alias("avg"),
        )
        .first()
    )
    assert row["path"].startswith("/vsisubfile/12345_")
    info = json.loads(row["summary"])
    assert info["size"] == [2000, 1500] and len(info["bands"]) == 3
    assert np.allclose(
        row["avg"], [float(b.mean()) for b in _rc.cog_pixels()], rtol=1e-6
    )


def _fail_subfile_opens(monkeypatch, fail):
    """Replace ``open_data`` so a ``/vsisubfile/`` open raises ``fail(n)`` (the n-th such open; None: open it). Returns
    the list of the (kind, size) of every open."""
    real = prx.open_data
    seen = []

    def _open(target, *a, **k):
        if str(target).startswith("/vsisubfile/"):
            seen.append(("subfile", None))
            exc = fail(len([s for s in seen if s[0] == "subfile"]))
            if exc is not None:
                raise exc
        else:
            seen.append(("copy", os.path.getsize(target)))
        return real(target, *a, **k)

    monkeypatch.setattr(prx, "open_data", _open)
    return seen


def test_a_volume_clip_that_cannot_open_in_place_reads_a_copy_of_the_slice_only(
    tmp_path, monkeypatch
):
    from databricks.labs.gbx.ds import file_gbx

    _, value = _prefixed(tmp_path)
    monkeypatch.setattr(file_gbx, "_is_fuse_path", lambda path: True)
    seen = _fail_subfile_opens(
        monkeypatch, lambda n: rasterio.errors.RasterioIOError("no random reads here")
    )
    got = _fromfile_impl(value, "GTiff", True, geom=BOX.wkb)
    assert got is not None and _arr(got)[1:] == (300, 300), got
    copies = {size for kind, size in seen if kind == "copy"}
    assert copies == {value["size"]}  # the copy holds the slice, not the object


def test_a_transient_miss_of_the_in_place_open_is_retried(tmp_path, monkeypatch):
    import types

    from databricks.labs.gbx.core import _retry
    from databricks.labs.gbx.ds import file_gbx

    _, value = _prefixed(tmp_path)
    monkeypatch.setattr(file_gbx, "_is_fuse_path", lambda path: True)
    monkeypatch.setattr(_retry, "time", types.SimpleNamespace(sleep=lambda s: None))
    miss = rasterio.errors.RasterioIOError(f"{value['uri']}: No such file or directory")
    seen = _fail_subfile_opens(monkeypatch, lambda n: miss if n == 1 else None)
    for kw in ({}, {"geom": BOX.wkb}):
        seen.clear()
        got = _fromfile_impl(value, "GTiff", True, **kw)
        assert got is not None and got["raster"] is not None, (kw, got)
        assert [k for k, _ in seen][:2] == [
            "subfile",
            "subfile",
        ]  # the miss, then the retry in place
        assert "copy" not in [k for k, _ in seen]  # no fallback copy for a miss


def test_a_volume_slice_that_is_no_raster_fails_fast_without_retries(
    tmp_path, monkeypatch
):
    import time

    from databricks.labs.gbx.ds import file_gbx

    _, value = _prefixed(tmp_path)
    monkeypatch.setattr(file_gbx, "_is_fuse_path", lambda path: True)
    seen = _fail_subfile_opens(monkeypatch, lambda n: None)
    bad = {**value, "offset": 100, "size": 5000}  # inside the prefix: no raster
    t0 = time.monotonic()
    assert _fromfile_impl(bad, "GTiff", True) is None
    assert _fromfile_impl(bad, "GTiff", True, geom=BOX.wkb) is None
    assert time.monotonic() - t0 < 2.0
    assert [k for k, _ in seen].count(
        "subfile"
    ) == 2  # one in-place open per call, no retry


def test_a_slice_over_the_stage_cap_is_refused(tmp_path, monkeypatch):
    from databricks.labs.gbx.ds import file_gbx
    from databricks.labs.gbx.ds.file_gbx import StageTooLargeError

    path, value = _prefixed(tmp_path)
    monkeypatch.setenv("GBX_STAGE_MAX_BYTES", "1024")
    with pytest.raises(StageTooLargeError, match="Fix:"):
        with prx._staged_slice(path, (value["offset"], value["size"])):
            pass
    # through the read: an in-place open that fails on a Volume falls back to the copy, which the cap refuses
    monkeypatch.setattr(file_gbx, "_is_fuse_path", lambda p: True)
    _fail_subfile_opens(
        monkeypatch, lambda n: rasterio.errors.RasterioIOError("no random reads here")
    )
    with pytest.raises(StageTooLargeError, match="GBX_STAGE_MAX_BYTES"):
        _fromfile_impl(value, "GTiff", True)
