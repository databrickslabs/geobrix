from test._cog_fixture import cog_bytes as _cog_bytes

import numpy as np
import shapely.geometry as sg
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.functions import _fromfile_impl


def _write(tmp_path, blob):
    p = tmp_path / "scene.tif"
    p.write_bytes(blob)
    return str(p)


def _arr(tile):
    with MemoryFile(tile["raster"]) as mf, mf.open() as ds:
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
