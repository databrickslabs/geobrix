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
