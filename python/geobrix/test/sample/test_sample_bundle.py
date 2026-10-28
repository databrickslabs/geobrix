"""
Tests for databricks.labs.gbx.sample (sample-data bundle helpers).

Covers get_volumes_path, get_temp_dir, _ensure_dir, _is_volume_path,
_path_exists_for_skip, _copy_final_to_volumes, _bundle_debug, download_to_path
(skip_if_exists), and run_essential_bundle / run_complete_bundle (with mocked
requests where needed) to raise coverage without requiring network or UC. The
NYC FileGDB step (nyc/filegdb/NYC_Sample.gdb.zip) is tested offline against tiny
layers through geopandas and pyogrio only, so it runs in the light CI env too.
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
FILEGDB_LAYERS = {"NYC_Boroughs", "NYC_TaxiZones", "NYC_Parks", "NYC_SubwayStations"}


def _gdb_uri(zip_path: Path) -> str:
    return f"/vsizip/{zip_path.as_posix()}/NYC_Sample.gdb"


def _zip_layers(zip_path: Path) -> set:
    pyogrio = pytest.importorskip("pyogrio")
    return {str(row[0]) for row in pyogrio.list_layers(_gdb_uri(zip_path))}


def _write_tiny_nyc_layers(base: Path) -> None:
    """Write the four NYC layers the FileGDB step reads, in the bundle's own layout and columns."""
    gpd = pytest.importorskip("geopandas")
    pytest.importorskip("pyogrio")
    import zipfile

    from shapely.geometry import MultiPolygon, Point, Polygon

    def square(x, y):
        return Polygon([(x, y), (x + 1, y), (x + 1, y + 1), (x, y + 1)])

    def put(rel, gdf, driver):
        out = base / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(str(out), driver=driver)

    put(
        "nyc/boroughs/nyc_boroughs.geojson",
        gpd.GeoDataFrame(
            {"boroname": ["Bronx", "Queens"], "borocode": ["2", "4"]},
            geometry=[square(0, 0), square(2, 0)],
            crs="EPSG:4326",
        ),
        "GeoJSON",
    )
    put(
        "nyc/taxi-zones/nyc_taxi_zones.geojson",
        gpd.GeoDataFrame(
            {"zone": ["A", "B", "C"], "locationid": ["1", "2", "3"]},
            geometry=[square(0, 0), square(1, 0), square(2, 0)],
            crs="EPSG:4326",
        ),
        "GeoJSON",
    )
    shapefiles = {
        # one Polygon and one MultiPolygon: like the real parks layer, only a multi-type layer holds both
        "nyc/parks/nyc_parks.shp.zip": gpd.GeoDataFrame(
            {
                "name311": ["Park One", "Park Two"],
                "acres": [3.5, 1.25],
                "borough": ["Q", "M"],
            },
            geometry=[square(0, 0), MultiPolygon([square(5, 5), square(7, 7)])],
            crs="EPSG:4326",
        ),
        "nyc/subway/nyc_subway.shp.zip": gpd.GeoDataFrame(
            {"stop_name": ["Main St", "Elm St"], "line": ["Flushing", "Broadway"]},
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


def test_nyc_filegdb_zip_holds_the_four_feature_classes(tmp_path):
    """The zip opens as a File Geodatabase with the four feature classes, real attributes and multi geometries."""
    pyogrio = pytest.importorskip("pyogrio")
    base = tmp_path / "geobrix-examples"
    _write_tiny_nyc_layers(base)

    dest = _bundle_mod._stage_nyc_filegdb(base, tmp_path / "work")

    assert dest == base / FILEGDB_REL
    assert _zip_layers(dest) == FILEGDB_LAYERS

    def read(layer):
        return pyogrio.read_dataframe(_gdb_uri(dest), layer=layer)

    boroughs = read("NYC_Boroughs")
    assert sorted(boroughs["BORO_NAME"]) == ["Bronx", "Queens"]
    assert sorted(boroughs["BORO_CODE"]) == ["2", "4"]
    assert sorted(read("NYC_TaxiZones")["LOCATION_ID"]) == [1, 2, 3]
    parks = read("NYC_Parks")
    assert sorted(parks["PARK_NAME"]) == ["Park One", "Park Two"]
    assert set(parks.geom_type) == {"MultiPolygon"}  # the Polygon park was promoted
    subway = read("NYC_SubwayStations")
    assert sorted(subway["STATION_NAME"]) == ["Elm St", "Main St"]
    assert sorted(subway["LINE"]) == ["Broadway", "Flushing"]


def test_nyc_filegdb_step_writes_no_zip_when_a_source_layer_is_missing(tmp_path):
    """No partial geodatabase is frozen: a missing source layer raises and leaves no zip."""
    base = tmp_path / "geobrix-examples"
    _write_tiny_nyc_layers(base)
    (base / "nyc" / "subway" / "nyc_subway.shp.zip").unlink()

    with pytest.raises(RuntimeError, match="missing: nyc/subway/nyc_subway.shp.zip"):
        _bundle_mod._stage_nyc_filegdb(base, tmp_path / "work")

    assert not (base / FILEGDB_REL).exists()


def test_nyc_filegdb_step_writes_no_zip_when_a_source_column_is_missing(tmp_path):
    """A source layer without a mapped column fails loudly instead of writing an empty field."""
    gpd = pytest.importorskip("geopandas")
    base = tmp_path / "geobrix-examples"
    _write_tiny_nyc_layers(base)
    boroughs = base / "nyc" / "boroughs" / "nyc_boroughs.geojson"
    gpd.read_file(boroughs).rename(columns={"boroname": "name"}).to_file(
        str(boroughs), driver="GeoJSON"
    )

    with pytest.raises(RuntimeError, match="has no column boroname"):
        _bundle_mod._stage_nyc_filegdb(base, tmp_path / "work")

    assert not (base / FILEGDB_REL).exists()


def test_nyc_filegdb_step_skips_a_complete_zip_and_replaces_an_incomplete_one(tmp_path):
    gpd = pytest.importorskip("geopandas")
    base = tmp_path / "geobrix-examples"
    _write_tiny_nyc_layers(base)
    dest = _bundle_mod._stage_nyc_filegdb(base, tmp_path / "work")
    before = dest.stat().st_mtime_ns

    # a complete zip is left alone
    assert _bundle_mod._stage_nyc_filegdb(base, tmp_path / "work") == dest
    assert dest.stat().st_mtime_ns == before

    # a zip with only some of the feature classes is rebuilt
    gdb = tmp_path / "partial" / "NYC_Sample.gdb"
    gdb.parent.mkdir()
    gpd.read_file(base / "nyc" / "boroughs" / "nyc_boroughs.geojson").to_file(
        str(gdb), layer="NYC_Boroughs", driver="OpenFileGDB"
    )
    dest.unlink()
    shutil.make_archive(str(dest.with_suffix("")), "zip", gdb.parent, "NYC_Sample.gdb")
    assert _zip_layers(dest) == {"NYC_Boroughs"}
    assert _bundle_mod._stage_nyc_filegdb(base, tmp_path / "work") == dest
    assert _zip_layers(dest) == FILEGDB_LAYERS


@pytest.mark.parametrize(
    "kind,values,dtype,expected",
    [
        ("str", ["a", None, float("nan"), 3], "object", ["a", None, None, "3"]),
        (
            "str",
            ["a", "pd.NA"],
            "object",
            ["a", None],
        ),  # a nullable-dtype missing value
        ("int", ["1", "2"], "int32", [1, 2]),
        ("int", ["1", None, "3"], "float64", [1.0, None, 3.0]),  # int cannot hold null
        ("float", ["1.5", "x"], "float64", [1.5, None]),
    ],
)
def test_fgdb_column_converts_to_the_field_dtype_and_keeps_missing_values_null(
    kind, values, dtype, expected
):
    pd = pytest.importorskip("pandas")
    values = [pd.NA if v == "pd.NA" else v for v in values]

    out = _bundle_mod._fgdb_column(pd.Series(values, dtype=object), kind)

    assert str(out.dtype) == dtype
    assert [None if pd.isna(v) else v for v in out] == expected


def test_nyc_filegdb_step_keeps_a_zip_whose_size_cannot_be_read(tmp_path, monkeypatch):
    """A FUSE mount can report a file present but not its size; that zip is not rebuilt each run."""
    base = tmp_path / "geobrix-examples"
    _write_tiny_nyc_layers(base)
    dest = _bundle_mod._stage_nyc_filegdb(base, tmp_path / "work")
    before = dest.stat().st_mtime_ns
    exists = _bundle_mod._path_exists_for_skip
    monkeypatch.setattr(
        _bundle_mod,
        "_path_exists_for_skip",
        lambda p: (exists(p)[0], None) if Path(p) == dest else exists(p),
    )

    assert _bundle_mod._stage_nyc_filegdb(base, tmp_path / "work") == dest

    assert dest.stat().st_mtime_ns == before


@pytest.mark.parametrize(
    "error,reported",
    [
        (
            ImportError("No module named 'pyogrio'"),
            "geopandas and pyogrio required: No module named 'pyogrio'",
        ),
        (RuntimeError("source layer missing"), "source layer missing"),
    ],
)
def test_run_complete_bundle_records_a_failed_filegdb_step(
    tmp_path, monkeypatch, error, reported
):
    """A FileGDB step that fails is recorded in errors with its cause, and the bundle carries on."""

    def offline(*args, **kwargs):
        raise RuntimeError("offline")

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(_bundle_mod, "download_to_path", offline)
    monkeypatch.setattr(_bundle_mod, "download_srtm_to_path", offline)
    monkeypatch.setattr(_bundle_mod, "_stage_nyc_filegdb", fail)

    result = run_complete_bundle(
        str(tmp_path / "geobrix-examples"), temp_dir=str(tmp_path / "tmp")
    )

    assert ("FileGDB", reported) in result["errors"]
