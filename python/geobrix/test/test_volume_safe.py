"""Test inputs written to ``tmp_path`` the way a Volume allows (``test._volume_safe``).

Under the Serverless runner ``tmp_path`` is on a Volume, where GDAL cannot update a file in place. The
``volume_like_tmp`` fixture refuses a rasterio or pyogrio write there; the shared input builders the suite
uses must still work under it.
"""

import os
from test._volume_safe import open_raster, written_locally

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

_PROF = dict(
    driver="GTiff",
    width=4,
    height=4,
    count=1,
    dtype="uint8",
    crs="EPSG:4326",
    transform=from_origin(0, 4, 1, 1),
)


def test_the_guard_refuses_a_gdal_write_in_place(volume_like_tmp):
    with pytest.raises(rasterio.errors.RasterioIOError, match="in place"):
        with rasterio.open(str(volume_like_tmp / "x.tif"), "w", **_PROF):
            pass


def test_a_write_and_an_update_are_made_locally_and_copied_over(volume_like_tmp):
    path = str(volume_like_tmp / "a.tif")
    with open_raster(path, "w", **_PROF) as dst:
        dst.write(np.full((1, 4, 4), 7, dtype="uint8"))
        dst.write_mask(np.full((4, 4), 255, dtype="uint8"))
    with open_raster(path, "r+") as dst:
        dst.update_tags(note="kept")
    with rasterio.open(path) as ds:
        assert ds.read(1).tolist() == [[7] * 4] * 4 and ds.tags()["note"] == "kept"


def test_sidecars_and_a_directory_output_are_copied_beside_the_target(
    volume_like_tmp,
):
    path = str(volume_like_tmp / "b.dat")
    with written_locally(path) as local:
        for name in (local, local + ".aux.xml"):
            with open(name, "w") as fh:
                fh.write("x")
        os.makedirs(local + ".d/0")
        with open(local + ".d/0/0.pbf", "w") as fh:
            fh.write("y")
    assert sorted(os.listdir(volume_like_tmp)) == ["b.dat", "b.dat.aux.xml", "b.dat.d"]
    assert os.path.exists(volume_like_tmp / "b.dat.d" / "0" / "0.pbf")


def test_the_shared_raster_and_vector_builders_work_on_a_volume(volume_like_tmp):
    """The builders other test files share: pyrx/_layouts, and a GeoPackage through pyogrio."""
    from test.pyrx import _layouts

    import pyarrow as pa
    import pyogrio
    import shapely

    for write in (
        _layouts.write_striped_gtiff,
        _layouts.write_tiled_gtiff,
        _layouts.write_cog,
    ):
        path = str(volume_like_tmp / f"{write.__name__}.tif")
        write(path, width=64, height=64)
        with rasterio.open(path) as ds:
            assert ds.width == 64
    gpkg = str(volume_like_tmp / "p.gpkg")
    tbl = pa.table({"geometry": pa.array([shapely.to_wkb(shapely.Point(1, 2))])})
    with written_locally(gpkg) as local:
        pyogrio.write_arrow(
            tbl,
            local,
            driver="GPKG",
            geometry_name="geometry",
            geometry_type="Point",
            crs="EPSG:4326",
        )
    assert pyogrio.read_info(gpkg)["features"] == 1


def test_the_osgeo_builders_work_on_a_volume(volume_like_tmp, monkeypatch):
    """The builders that write with GDAL's own bindings, which the guard cannot see: each goes through
    ``written_locally`` and its output lands at the path it was given."""
    from databricks.labs.gbx.engine import run

    if run.unavailable_reason():
        pytest.skip(run.unavailable_reason())
    from databricks.labs.gbx import _osgeo

    gb = _osgeo.bindings("test_volume_safe")
    import test._volume_safe as vs
    from test.drivers_extended import _raster, _vector
    from test.ds import conftest as ds_conftest
    from test.engine import test_warp_rows

    seen = []
    real = vs.written_locally

    def spy(path, *a, **k):
        seen.append(os.path.basename(str(path)))
        return real(path, *a, **k)

    for mod in (vs, _raster, _vector, ds_conftest, test_warp_rows):
        monkeypatch.setattr(mod, "written_locally", spy)
    tif = _raster.write_sample(
        gb, _raster.RasterCase("GTiff", ".tif"), str(volume_like_tmp)
    )
    gpkg = _vector.write_sample(
        gb, _vector.VectorCase("GPKG", "v.gpkg"), str(volume_like_tmp)
    )
    coded = ds_conftest._build_coded_domain_source(
        str(volume_like_tmp / "coded.gpkg"), "GPKG"
    )
    gcp = test_warp_rows._gcp_tile(volume_like_tmp)
    names = {os.path.basename(p) for p in (tif, gpkg, coded)} | {"gcp.tif"}
    assert names <= set(seen), seen
    for path in (tif, gpkg, coded):
        assert os.path.getsize(path) > 0
    assert gcp[:2] in (b"II", b"MM")
