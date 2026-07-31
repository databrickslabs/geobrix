import os

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import cog as gbxcog
from databricks.labs.gbx.pyrx.core.preparer import (
    cog_output_name,
    _subdataset_uri,
    prepare_cog,
    prepare_cog_measured,
)


def _write_src(path, w=512, h=512):
    profile = dict(
        driver="GTiff", width=w, height=h, count=1, dtype="uint8",
        crs="EPSG:4326", transform=from_origin(0, 60, 0.01, 0.01),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(np.arange(w * h, dtype="uint8").reshape(1, h, w))


def test_cog_output_name_appends_cog():
    assert cog_output_name("myfile1.tiff") == "myfile1.tiff.cog"
    assert cog_output_name("scene.tif") == "scene.tif.cog"
    assert cog_output_name("no_ext") == "no_ext.cog"


def test_subdataset_uri_bare_path_when_none():
    assert _subdataset_uri("/data/x.tif", None) == "/data/x.tif"


def test_subdataset_uri_builds_netcdf_uri():
    assert _subdataset_uri("/data/x.nc", "temp") == 'NETCDF:"/data/x.nc":temp'


def test_prepare_cog_produces_valid_cog_named_dot_cog(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out_path, status = prepare_cog(str(src), str(out), blocksize=256)
    assert status == "ok"
    assert out_path == str(out / "scene.tiff.cog")
    assert os.path.exists(out_path)
    with open(out_path, "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    assert info.is_cog is True and info.overview_levels >= 1


def test_prepare_cog_skips_when_exists_default(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    # Pre-create the target so skip_if_exists (default True) short-circuits.
    target = out / "scene.tiff.cog"
    target.write_bytes(b"sentinel-not-a-real-cog")
    out_path, status = prepare_cog(str(src), str(out), blocksize=256)
    assert status == "skipped"
    assert out_path == str(target)
    # Untouched — still the sentinel bytes (no reconvert).
    assert target.read_bytes() == b"sentinel-not-a-real-cog"


def test_prepare_cog_force_rebuild_when_skip_false(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    target = out / "scene.tiff.cog"
    target.write_bytes(b"sentinel")
    out_path, status = prepare_cog(
        str(src), str(out), blocksize=256, skip_if_exists=False
    )
    assert status == "ok"
    with open(out_path, "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    assert info.is_cog is True  # real COG now, sentinel overwritten


def test_prepare_cog_error_isolation_returns_status(tmp_path):
    out = tmp_path / "out"
    # Nonexistent source → convert fails; must return ('error:...') not raise.
    out_path, status = prepare_cog(str(tmp_path / "missing.tif"), str(out))
    assert out_path is None
    assert status.startswith("error:")


def test_prepare_cog_measured_ok_has_rss(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    r = prepare_cog_measured(str(src), str(out), blocksize=256)
    assert r["status"] == "ok"
    assert r["output_path"] == str(out / "scene.tiff.cog")
    assert isinstance(r["peak_rss_mib"], float) and r["peak_rss_mib"] > 0


def test_prepare_cog_measured_error_passthrough(tmp_path):
    out = tmp_path / "out"
    r = prepare_cog_measured(str(tmp_path / "missing.tif"), str(out))
    assert r["output_path"] is None
    assert r["status"].startswith("error:")
    # RSS is still reported (measured around the attempt).
    assert isinstance(r["peak_rss_mib"], float)


def test_prepare_cog_measured_skipped_status(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    (out / "scene.tiff.cog").write_bytes(b"sentinel")
    r = prepare_cog_measured(str(src), str(out))
    assert r["status"] == "skipped"
    assert r["output_path"] == str(out / "scene.tiff.cog")


def test_prepare_cog_out_name_overrides_basename(tmp_path):
    src = tmp_path / "in" / "staged_tmp12345.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    # out_name mimics prepare_cogs passing the ORIGINAL basename while the input
    # path is a staged temp.
    out_path, status = prepare_cog(
        str(src), str(out), blocksize=256, out_name="scene_original.tif"
    )
    assert status == "ok"
    assert out_path == str(out / "scene_original.tif.cog")
    assert os.path.exists(out_path)


def test_prepare_cog_out_name_none_uses_basename(tmp_path):
    src = tmp_path / "in" / "plain.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out_path, status = prepare_cog(str(src), str(out), blocksize=256)
    assert status == "ok"
    assert out_path == str(out / "plain.tif.cog")
