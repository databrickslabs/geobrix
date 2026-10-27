"""
Tests for databricks.labs.gbx.sample (sample-data bundle helpers).

Covers get_volumes_path, get_temp_dir, _ensure_dir, _is_volume_path,
_path_exists_for_skip, _copy_final_to_volumes, _bundle_debug, download_to_path
(skip_if_exists), and run_essential_bundle / run_complete_bundle (with mocked
requests where needed) to raise coverage without requiring network or UC. The
NYC FileGDB step (nyc/filegdb/NYC_Sample.gdb.zip) is tested offline against tiny
layers and needs GDAL's Python bindings plus geopandas (skipped without them).
"""

import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

# Public API + the _bundle module itself (needed for internal-helper coverage tests below).
from databricks.labs.gbx.sample import _bundle as _bundle_mod
from databricks.labs.gbx.sample import (
    get_temp_dir,
    get_volumes_path,
    run_complete_bundle,
    run_essential_bundle,
)

# ========== __init__ (package surface) ==========


def test_sample_package_all():
    """Package __all__ exposes the expected names."""
    import databricks.labs.gbx.sample as sample

    assert hasattr(sample, "get_volumes_path")
    assert hasattr(sample, "get_temp_dir")
    assert hasattr(sample, "run_essential_bundle")
    assert hasattr(sample, "run_complete_bundle")
    assert set(sample.__all__) == {
        "DemDownloader",
        "EmitDownloader",
        "NaipDownloader",
        "NasaNexDownloader",
        "OvertureClient",
        "TropomiDownloader",
        "WellsDownloader",
        "download_dem_aoi",
        "download_emit_aoi",
        "download_naip_aoi",
        "download_nasanex_aoi",
        "download_overture_aoi",
        "download_tropomi_aoi",
        "download_wells_aoi",
        "get_temp_dir",
        "get_volumes_path",
        "run_complete_bundle",
        "run_essential_bundle",
    }


# ========== get_volumes_path ==========


def test_get_volumes_path():
    assert get_volumes_path("main", "default", "geobrix_samples") == (
        "/Volumes/main/default/geobrix_samples/geobrix-examples"
    )
    assert get_volumes_path("cat", "sch", "vol") == (
        "/Volumes/cat/sch/vol/geobrix-examples"
    )


# ========== get_temp_dir ==========


def test_get_temp_dir_system_temp():
    p = get_temp_dir(None)
    assert p.is_dir()
    assert "geobrix_bundle_build" in str(p)
    assert p == Path(tempfile.gettempdir()) / "geobrix_bundle_build"


def test_get_temp_dir_provided_path():
    with tempfile.TemporaryDirectory() as d:
        p = get_temp_dir(d)
        assert p == Path(d)
        assert p.is_dir()


def test_get_temp_dir_provided_path_creates_parents():
    with tempfile.TemporaryDirectory() as d:
        sub = os.path.join(d, "a", "b")
        p = get_temp_dir(sub)
        assert p == Path(sub)
        assert p.is_dir()


# ========== _unity_catalog_volume_root ==========


def test_unity_catalog_volume_root_under_volumes():
    root = _bundle_mod._unity_catalog_volume_root(Path("/Volumes/cat/sch/vol/foo/bar"))
    assert root is not None
    assert root == Path("/Volumes/cat/sch/vol")


def test_unity_catalog_volume_root_not_volumes():
    assert _bundle_mod._unity_catalog_volume_root(Path("/tmp/foo")) is None
    assert _bundle_mod._unity_catalog_volume_root(Path("relative")) is None


def test_unity_catalog_volume_root_exactly_volume():
    root = _bundle_mod._unity_catalog_volume_root(Path("/Volumes/cat/sch/vol"))
    assert root == Path("/Volumes/cat/sch/vol")


# ========== _ensure_dir ==========


def test_ensure_dir_creates_path():
    with tempfile.TemporaryDirectory() as d:
        sub = Path(d) / "a" / "b"
        _bundle_mod._ensure_dir(sub)
        assert sub.is_dir()


def test_ensure_dir_volume_root_skip():
    with tempfile.TemporaryDirectory() as d:
        vol_root = Path(d)
        _bundle_mod._ensure_dir(vol_root, volume_root=vol_root)
        assert vol_root.is_dir()


# ========== _is_volume_path ==========


def test_is_volume_path_true():
    assert _bundle_mod._is_volume_path(Path("/Volumes/cat/sch/vol/foo")) is True
    assert _bundle_mod._is_volume_path(Path("/volumes/cat/sch/vol")) is True


def test_is_volume_path_false():
    assert _bundle_mod._is_volume_path(Path("/tmp/foo")) is False
    assert _bundle_mod._is_volume_path(Path("relative")) is False
    assert _bundle_mod._is_volume_path(Path("/Volumes")) is False
    assert _bundle_mod._is_volume_path(Path("/Volumes/cat")) is False


# ========== _path_exists_for_skip ==========


def test_path_exists_for_skip_missing():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "nonexistent"
        exists, size_mb = _bundle_mod._path_exists_for_skip(p)
    assert exists is False
    assert size_mb is None


def test_path_exists_for_skip_exists():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(b"hello")
        f.flush()
        path = Path(f.name)
    try:
        exists, size_mb = _bundle_mod._path_exists_for_skip(path)
        assert exists is True
        assert size_mb is not None
        assert size_mb >= 0
    finally:
        path.unlink(missing_ok=True)


# ========== _copy_final_to_volumes ==========


def test_copy_final_to_volumes():
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "src.txt"
        src.write_text("content")
        dest = Path(tmp) / "out" / "dest.txt"
        result = _bundle_mod._copy_final_to_volumes(src, dest, "Test copy")
        assert result == dest
        assert dest.read_text() == "content"


def test_copy_final_to_volumes_skip_if_exists(capsys):
    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp) / "existing.txt"
        dest.write_text("already")
        src = Path(tmp) / "new.txt"
        src.write_text("new")
        result = _bundle_mod._copy_final_to_volumes(src, dest, "Test skip")
        assert result == dest
        assert dest.read_text() == "already"
    out = capsys.readouterr().out
    assert "already exists" in out or "MB" in out


# ========== _bundle_debug ==========


def test_bundle_debug_no_env(capsys):
    with patch.dict(os.environ, {}, clear=False):
        if "GBX_BUNDLE_DEBUG" in os.environ:
            del os.environ["GBX_BUNDLE_DEBUG"]
        _bundle_mod._bundle_debug("msg")
    assert "[bundle]" not in capsys.readouterr().out


def test_bundle_debug_with_env(capsys):
    with patch.dict(os.environ, {"GBX_BUNDLE_DEBUG": "1"}):
        _bundle_mod._bundle_debug("test message")
    assert "[bundle] test message" in capsys.readouterr().out


# ========== download_to_path (skip_if_exists, no network) ==========


def test_download_to_path_skip_if_exists():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
        f.write(b"existing")
        f.flush()
        path = Path(f.name)
    try:
        result = _bundle_mod.download_to_path(
            "https://example.com/file.bin",
            path,
            "Existing file",
            skip_if_exists=True,
            quiet=True,
        )
        assert result == path
        assert path.read_bytes() == b"existing"
    finally:
        path.unlink(missing_ok=True)


def test_download_to_path_requires_requests_when_download_needed():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "new.bin"
        with patch.object(_bundle_mod, "requests", None):
            with pytest.raises(RuntimeError, match="requests is required"):
                _bundle_mod.download_to_path(
                    "https://example.com/file.bin",
                    path,
                    "Need download",
                    skip_if_exists=False,
                    quiet=True,
                )


# ========== run_essential_bundle ==========


@pytest.mark.integration
def test_run_essential_bundle_returns_dict_shape():
    """Run with temp path; may hit network errors but must return dict with expected keys."""
    with tempfile.TemporaryDirectory() as d:
        base = Path(d) / "geobrix-examples"
        result = run_essential_bundle(str(base), temp_dir=d)
    assert "errors" in result
    assert "file_count" in result
    assert "total_size_mb" in result
    assert isinstance(result["errors"], list)
    assert isinstance(result["file_count"], int)
    assert isinstance(result["total_size_mb"], (int, float))


# ========== run_complete_bundle ==========


@pytest.mark.integration
def test_run_complete_bundle_returns_dict_shape():
    """Run with temp path; may hit network errors but must return dict with expected keys."""
    with tempfile.TemporaryDirectory() as d:
        base = Path(d) / "geobrix-examples"
        result = run_complete_bundle(str(base), temp_dir=d)
    assert "errors" in result
    assert "file_count" in result
    assert "total_size_mb" in result
    assert isinstance(result["errors"], list)
    assert isinstance(result["file_count"], int)
    assert isinstance(result["total_size_mb"], (int, float))


# ========== NYC FileGDB step (nyc/filegdb/NYC_Sample.gdb.zip) ==========

FILEGDB_REL = Path("nyc") / "filegdb" / "NYC_Sample.gdb.zip"


def _write_tiny_nyc_layers(base: Path) -> None:
    """Write the four NYC layers the FileGDB step reads, in the bundle's own layout."""
    pytest.importorskip("osgeo")
    gpd = pytest.importorskip("geopandas")
    import zipfile

    from shapely.geometry import Point, Polygon

    def square(x, y):
        return Polygon([(x, y), (x + 1, y), (x + 1, y + 1), (x, y + 1)])

    def put(rel, gdf, driver):
        out = base / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(str(out), driver=driver)
        return out

    put(
        "nyc/boroughs/nyc_boroughs.geojson",
        gpd.GeoDataFrame(
            {"boro_name": ["Bronx", "Queens"], "boro_code": ["2", "4"]},
            geometry=[square(0, 0), square(2, 0)],
            crs="EPSG:4326",
        ),
        "GeoJSON",
    )
    put(
        "nyc/taxi-zones/nyc_taxi_zones.geojson",
        gpd.GeoDataFrame(
            {"zone": ["A", "B", "C"], "LocationID": [1, 2, 3]},
            geometry=[square(0, 0), square(1, 0), square(2, 0)],
            crs="EPSG:4326",
        ),
        "GeoJSON",
    )
    shapefiles = {
        "nyc/parks/nyc_parks.shp.zip": gpd.GeoDataFrame(
            {"name311": ["Park One"], "acres": [3.5], "borough": ["Q"]},
            geometry=[square(0, 0)],
            crs="EPSG:4326",
        ),
        "nyc/subway/nyc_subway.shp.zip": gpd.GeoDataFrame(
            {"Station_Na": ["Main St", "Elm St"], "Line": ["7", "N"]},
            geometry=[Point(0.5, 0.5), Point(2.5, 0.5)],
            crs="EPSG:4326",
        ),
    }
    for rel, gdf in shapefiles.items():
        zpath = base / rel
        zpath.parent.mkdir(parents=True, exist_ok=True)
        shp_dir = zpath.parent / "_shp"
        shp_dir.mkdir()
        gdf.to_file(str(shp_dir / "layer.shp"), driver="ESRI Shapefile")
        with zipfile.ZipFile(zpath, "w") as z:
            for f in shp_dir.iterdir():
                z.write(f, f.name)
        shutil.rmtree(shp_dir)


def test_run_complete_bundle_stages_nyc_filegdb_zip(tmp_path, monkeypatch):
    """The Complete bundle writes nyc/filegdb/NYC_Sample.gdb.zip next to its other NYC layers."""
    base = tmp_path / "geobrix-examples"
    _write_tiny_nyc_layers(base)

    def offline(*args, **kwargs):
        raise RuntimeError("offline")

    # Every download step fails and is recorded in errors; the FileGDB is built from local layers.
    monkeypatch.setattr(_bundle_mod, "download_to_path", offline)
    monkeypatch.setattr(_bundle_mod, "download_srtm_to_path", offline)
    result = run_complete_bundle(str(base), temp_dir=str(tmp_path / "tmp"))

    assert (base / FILEGDB_REL).is_file()
    assert "FileGDB" not in [name for name, _ in result["errors"]]


def test_nyc_filegdb_zip_holds_the_bundle_layers(tmp_path):
    """The zip opens as a File Geodatabase with the four feature classes and their rows."""
    base = tmp_path / "geobrix-examples"
    _write_tiny_nyc_layers(base)
    from osgeo import ogr

    dest = _bundle_mod._stage_nyc_filegdb(base, tmp_path / "work")

    assert dest == base / FILEGDB_REL
    ds = ogr.Open(f"/vsizip/{dest}/NYC_Sample.gdb")
    assert ds is not None
    counts = {
        ds.GetLayer(i).GetName(): ds.GetLayer(i).GetFeatureCount()
        for i in range(ds.GetLayerCount())
    }
    assert counts == {
        "NYC_Boroughs": 2,
        "NYC_TaxiZones": 3,
        "NYC_Parks": 1,
        "NYC_SubwayStations": 2,
    }
    boroughs = ds.GetLayerByName("NYC_Boroughs")
    assert sorted(f.GetField("BORO_NAME") for f in boroughs) == ["Bronx", "Queens"]
    zones = ds.GetLayerByName("NYC_TaxiZones")
    assert sorted(f.GetField("LOCATION_ID") for f in zones) == [1, 2, 3]
    ds = None


def test_nyc_filegdb_step_skips_when_present_and_needs_a_source_layer(tmp_path):
    base = tmp_path / "geobrix-examples"
    _write_tiny_nyc_layers(base)
    dest = _bundle_mod._stage_nyc_filegdb(base, tmp_path / "work")
    before = dest.stat().st_mtime_ns

    assert _bundle_mod._stage_nyc_filegdb(base, tmp_path / "work") == dest
    assert dest.stat().st_mtime_ns == before

    empty = tmp_path / "empty-examples"
    empty.mkdir()
    with pytest.raises(RuntimeError, match="no NYC source layers"):
        _bundle_mod._stage_nyc_filegdb(empty, tmp_path / "work2")
