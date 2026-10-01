import os
import zipfile
from unittest.mock import patch

from shapely import from_wkb


def test_generate_polygon_seed(spark):
    from databricks.labs.gbx.bench.corpus_vector import generate_polygon_seed

    df = generate_polygon_seed(spark, 200, srid="4326")
    assert [f.name for f in df.schema.fields] == [
        "geom_0",
        "geom_0_srid",
        "geom_0_srid_proj",
        "id",
        "name",
    ]
    assert df.count() == 200
    row = df.orderBy("id").first()
    assert row["geom_0_srid"] == "4326"
    g = from_wkb(bytes(row["geom_0"]))
    assert g.geom_type == "Polygon"
    assert -180.0 <= g.bounds[0] <= 180.0 and -90.0 <= g.bounds[1] <= 90.0


def test_transcode_vector_seed(spark, tmp_path):
    from databricks.labs.gbx.bench.corpus_vector import (
        generate_polygon_seed,
        transcode_vector_seed,
    )
    from databricks.labs.gbx.ds.register import register

    register(spark)
    seed = generate_polygon_seed(spark, 100)
    # file_gdb needs native osgeo (heavy natives) -> exclude locally
    fmts = ["geojson_gbx", "shapefile_gbx", "gpkg_gbx"]
    out = transcode_vector_seed(spark, seed, fmts, str(tmp_path / "vec"))
    for fmt in fmts:
        assert fmt in out
    # shapefile_gbx seed must be a .shp.zip (self-contained archive for dir-read parity)
    assert out["shapefile_gbx"].endswith(
        ".shp.zip"
    ), f"expected .shp.zip for shapefile_gbx, got {out['shapefile_gbx']}"
    # The .shp.zip archive must contain at least the .shp component at the zip root
    with zipfile.ZipFile(out["shapefile_gbx"]) as zf:
        names = zf.namelist()
    assert any(n.endswith(".shp") for n in names), f".shp missing from zip: {names}"
    assert not any(
        "/" in n for n in names
    ), f"zip entries should be flat (no subdir): {names}"
    # All three formats must read back via their *_gbx reader
    for fmt in fmts:
        back = spark.read.format(fmt).load(out[fmt])
        assert back.count() == 100


def test_replicate_vector_seed(spark, tmp_path):
    from databricks.labs.gbx.bench.corpus_vector import replicate_vector_seed

    # a fake single-file seed
    seed = str(tmp_path / "seed.geojson")
    with open(seed, "w") as fh:
        fh.write('{"type":"FeatureCollection","features":[]}')
    copies_dir = str(tmp_path / "copies")
    paths = replicate_vector_seed(seed, 5, copies_dir)
    assert len(paths) == 5
    assert all(os.path.exists(p) for p in paths)
    assert sorted(os.listdir(copies_dir)) == [f"copy_{i}.geojson" for i in range(5)]


def test_replicate_vector_seed_shp_zip(spark, tmp_path):
    """A .shp.zip seed (compound extension) is copied with the full .shp.zip ext."""
    from databricks.labs.gbx.bench.corpus_vector import replicate_vector_seed

    seed = str(tmp_path / "seed.shp.zip")
    with open(seed, "wb") as fh:
        # minimal valid zip (empty archive)
        with zipfile.ZipFile(fh, "w") as zf:
            zf.writestr("seed.shp", b"")
    copies_dir = str(tmp_path / "copies_shp")
    paths = replicate_vector_seed(seed, 3, copies_dir)
    assert len(paths) == 3
    assert all(os.path.exists(p) for p in paths)
    assert sorted(os.listdir(copies_dir)) == [f"copy_{i}.shp.zip" for i in range(3)]


def test_build_vector_corpus(spark, tmp_path):
    from databricks.labs.gbx.bench.corpus_vector import build_vector_corpus
    from databricks.labs.gbx.ds.register import register

    register(spark)
    out = build_vector_corpus(
        spark,
        rows=50,
        copies=3,
        formats=["geojson_gbx", "gpkg_gbx", "shapefile_gbx"],
        out_base=str(tmp_path / "vc"),
    )
    for fmt in ("geojson_gbx", "gpkg_gbx", "shapefile_gbx"):
        assert os.path.exists(out[fmt]["seed"])
        assert len(out[fmt]["copies"]) == 3
        assert spark.read.format(fmt).load(out[fmt]["seed"]).count() == 50
    # shapefile copies must all be .shp.zip files
    for copy_path in out["shapefile_gbx"]["copies"]:
        assert copy_path.endswith(
            ".shp.zip"
        ), f"expected .shp.zip copy, got {copy_path}"
        assert os.path.exists(copy_path)


# ---------------------------------------------------------------------------
# Wiring: bench source enumerators route through list_files (fail-on-revert)
# ---------------------------------------------------------------------------


def test_zip_shapefile_calls_list_files_for_sidecars(tmp_path):
    """_zip_shapefile routes sidecar listing through list_files (fail-on-revert)."""
    stem = "roads"
    # Create minimal sidecar files
    for ext in (".shp", ".dbf", ".shx"):
        (tmp_path / f"{stem}{ext}").write_bytes(b"x")
    from databricks.labs.gbx.bench.corpus_vector import _zip_shapefile

    with patch("databricks.labs.gbx.ds._listing.list_files") as mock_lf:
        mock_lf.return_value = [
            str(tmp_path / f"{stem}.shp"),
            str(tmp_path / f"{stem}.dbf"),
            str(tmp_path / f"{stem}.shx"),
        ]
        try:
            _zip_shapefile(str(tmp_path), stem)
        except Exception:
            pass  # zip write may fail in test env; listing call already captured
    mock_lf.assert_called()
    _args, kw = mock_lf.call_args
    assert kw.get("recursive", True) is False
    assert kw.get("raise_on_empty", True) is False
