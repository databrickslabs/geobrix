"""geobrix Lakeflow connector: contract, configuration, offsets, Spark reads."""

import json
import os
import shutil

import pytest
from conftest import BOROUGHS, GPKG, set_mtime

from databricks.labs.community_connector.sources.geobrix import (
    GeoBrixDataSource,
    GeoBrixLakeflowConnect,
)

pytestmark = pytest.mark.filterwarnings("ignore:driver .* does not support open option")


# ---------------------------------------------------------------- packaging
def test_framework_resolves_connector_by_source_name():
    from databricks.labs.community_connector.sparkpds import find_data_source

    assert find_data_source("geobrix") is GeoBrixDataSource


def test_geobrix_and_framework_share_the_databricks_labs_namespace():
    import databricks.labs.community_connector as cc
    import databricks.labs.gbx.ds.vector as gbx_vector

    assert cc.__file__ and gbx_vector.VectorGbxReader


# ---------------------------------------------------------------- configuration
def test_reader_options_map_connector_options_and_drop_framework_keys():
    opts = GeoBrixLakeflowConnect({}).reader_options(
        {
            "format": "gpkg",
            "layer": "taxi_zones",
            "chunk_size": "500",
            "where": "borough = 'Queens'",
            "bbox": "-74.1,40.6,-73.8,40.9",
            "file_glob": "zones_*.gpkg",
            "tableName": "zones",
            "databricks.connection": "conn",
            "sourceName": "geobrix",
            "root_path": "/Volumes/c/s/v",
            "path": "zones",
        },
        "gpkg_gbx",
    )
    assert opts == {
        "layerName": "taxi_zones",
        "chunkSize": "500",
        "where": "borough = 'Queens'",
        "bbox": "-74.1,40.6,-73.8,40.9",
    }


_VECTOR_FORMATS = ("vector", "gpkg", "geojson", "geojsonl", "shapefile", "file_gdb")


@pytest.mark.parametrize("fmt", _VECTOR_FORMATS)
def test_vector_tables_take_layer_where_bbox_and_chunk_size_but_not_mode(fmt):
    c = GeoBrixLakeflowConnect({})
    opts = {"layer": "l", "where": "k = 1", "bbox": "0,0,1,1", "chunk_size": "10"}
    assert c.reader_options(opts, fmt) == {
        "layerName": "l",
        "where": "k = 1",
        "bbox": "0,0,1,1",
        "chunkSize": "10",
    }
    with pytest.raises(ValueError, match="`mode` applies to lidar and exif tables"):
        c.reader_options({"mode": "points"}, fmt)


@pytest.mark.parametrize("fmt", ["lidar", "exif"])
def test_lidar_and_exif_tables_take_mode_only(fmt):
    c = GeoBrixLakeflowConnect({})
    assert c.reader_options({"mode": "metadata", "file_glob": "*"}, fmt) == {
        "mode": "metadata"
    }
    for key in ("layer", "where", "bbox", "chunk_size"):
        msg = f"`{key}` applies to vector, gpkg, .*file_gdb tables"
        with pytest.raises(ValueError, match=msg):
            c.reader_options({key: "1"}, fmt)


@pytest.mark.parametrize("fmt", ["raster", "gtiff"])
def test_raster_tables_take_none_of_the_reader_options(fmt):
    c = GeoBrixLakeflowConnect({})
    opts = {"path": "p", "format": fmt, "file_glob": "*.tif"}
    assert c.reader_options(opts, fmt) == {}
    for key in ("layer", "where", "bbox", "chunk_size", "mode"):
        with pytest.raises(ValueError, match=f"`{key}` applies to"):
            c.reader_options({key: "1"}, fmt)


def test_an_option_for_another_reader_fails_the_table_schema(root):
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    opts = {"format": "geojson", "mode": "points"}
    with pytest.raises(
        ValueError, match="`mode` applies to lidar and exif.*geojson_gbx"
    ):
        c.get_table_schema("nyc_boroughs", opts)


def test_unknown_format_is_rejected():
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import (
        format_for,
    )

    with pytest.raises(ValueError, match="does not support"):
        format_for({"format": "kml"}, ["/x/a.kml"])


def test_filegdb_is_not_a_format_name(tmp_path):
    """One name per reader: the File Geodatabase reader is `file_gdb` (or
    `file_gdb_gbx`); `filegdb` fails with the supported formats."""
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import (
        format_for,
    )

    msg = r"'filegdb'\. Supported formats: .*\bfile_gdb\b"
    with pytest.raises(ValueError, match=msg):
        format_for({"format": "filegdb"}, [str(tmp_path / "a.gdb.zip")])
    assert format_for({"format": "file_gdb"}, []) == "file_gdb_gbx"


def test_relative_path_resolves_against_root(root):
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    assert c._resolve_path("t", {"path": "nyc_boroughs"}) == str(root / "nyc_boroughs")


@pytest.mark.parametrize("path", ["../outside", "nyc_boroughs/../../outside", "/etc"])
def test_paths_outside_root_are_rejected(root, path):
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    with pytest.raises(ValueError, match="outside root_path"):
        c._resolve_path("t", {"path": path})


def test_relative_path_without_root_is_rejected():
    with pytest.raises(ValueError, match="needs a root_path"):
        GeoBrixLakeflowConnect({})._resolve_path("t", {"path": "data"})


def test_unknown_table_name_without_path_is_rejected(root):
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    with pytest.raises(ValueError, match="Cannot resolve table"):
        c._resolve_path("no_such_table", {})


def test_list_tables_discovers_files_and_folders_under_root(root):
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    assert c.list_tables() == ["nyc_boroughs", "nyc_complete"]
    assert c._resolve_path("nyc_complete", {}) == str(root / "nyc_complete.gpkg")
    assert GeoBrixLakeflowConnect({}).list_tables() == []


def test_schema_is_reader_schema_plus_lineage(root):
    schema = GeoBrixLakeflowConnect({"root_path": str(root)}).get_table_schema(
        "nyc_boroughs", {"format": "geojson"}
    )
    names = [f.name for f in schema.fields]
    assert names[-2:] == ["_source_path", "_source_mtime"]
    assert {"boroname", "geom_0", "geom_0_srid"} <= set(names)


def test_schema_of_a_table_with_no_files_yet_is_a_clear_error(root):
    (root / "empty").mkdir()
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    with pytest.raises(ValueError, match="no matching files yet"):
        c.get_table_schema("empty", {"path": "empty", "format": "geojson"})


def test_metadata_is_append_with_mtime_cursor():
    meta = GeoBrixLakeflowConnect({}).read_table_metadata("t", {})
    assert meta == {
        "primary_keys": [],
        "cursor_field": "_source_mtime",
        "ingestion_type": "append",
    }


def test_format_takes_reader_names_with_optional_suffix_in_any_case():
    """The `format` option follows read_paths: a reader name, `_gbx` optional."""
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import (
        format_for,
    )
    from databricks.labs.gbx.ds.paths import SUPPORTED_FORMATS

    for name in SUPPORTED_FORMATS:
        short = name[: -len("_gbx")]
        for value in (short, short.upper(), name, name.upper()):
            assert format_for({"format": value}, []) == name, value


def test_discovery_infers_format_from_extension(tmp_path):
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import (
        format_for,
    )

    assert format_for({}, [str(tmp_path / "a.gpkg")]) == "gpkg_gbx"
    assert format_for({}, [str(tmp_path / "a.laz")]) == "lidar_gbx"
    assert format_for({}, [str(tmp_path / "a.jpg")]) == "exif_gbx"
    assert format_for({}, [str(tmp_path / "a.tif")]) == "gtiff_gbx"
    assert format_for({}, [str(tmp_path / "a.TIFF")]) == "gtiff_gbx"
    assert (
        format_for({"format": "geojson"}, [str(tmp_path / "a.json")]) == "geojson_gbx"
    )


def test_mixed_folder_without_format_errors(tmp_path):
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import (
        format_for,
    )

    with pytest.raises(ValueError, match="format"):
        format_for({}, [str(tmp_path / "a.gpkg"), str(tmp_path / "b.geojson")])


def test_connector_reads_through_shared_core(root, monkeypatch):
    """Wiring: member reads go through ds.paths.iter_path_batches."""
    from databricks.labs.community_connector.sources.geobrix import (
        lakeflow_connect as LC,
    )
    from databricks.labs.gbx.ds import paths as P

    calls = []
    real = P.iter_path_batches
    monkeypatch.setattr(
        LC, "iter_path_batches", lambda *a, **k: calls.append(a[0]) or real(*a, **k)
    )
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    rows, _ = c.read_table("nyc_boroughs", {}, {"format": "geojson"})
    assert len(list(rows)) == 5
    assert calls == [str(root / "nyc_boroughs" / "part_a.geojson")]


def test_lidar_table_reads(tmp_path):
    import laspy
    import numpy as np

    hdr = laspy.LasHeader(point_format=3, version="1.4")
    las = laspy.LasData(hdr)
    las.x = np.arange(4.0)
    las.y = np.arange(4.0)
    las.z = np.arange(4.0)
    (tmp_path / "pts").mkdir()
    las.write(str(tmp_path / "pts" / "a.las"))
    c = GeoBrixLakeflowConnect({"root_path": str(tmp_path)})
    assert "pts" in c.list_tables()
    rows, _ = c.read_table("pts", {}, {})
    assert len(list(rows)) == 4


def test_raster_table_reads(tmp_path):
    import numpy as np
    import rasterio

    (tmp_path / "tiles").mkdir()
    tif = str(tmp_path / "tiles" / "a.tif")
    profile = dict(
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="uint8",
        crs="EPSG:3857",
        transform=rasterio.Affine(1, 0, 0, 0, -1, 8),
    )
    with rasterio.open(tif, "w", **profile) as dst:
        dst.write(np.ones((1, 8, 8), dtype="uint8"))
    c = GeoBrixLakeflowConnect({"root_path": str(tmp_path)})
    assert "tiles" in c.list_tables()
    rows = list(c.read_table("tiles", {}, {})[0])
    assert len(rows) == 1 and rows[0]["tile"]["path"] == tif
    assert rows[0]["_source_path"] == tif


def _las(path):
    import laspy
    import numpy as np

    las = laspy.LasData(laspy.LasHeader(point_format=3, version="1.4"))
    las.x = las.y = las.z = np.arange(4.0)
    las.write(str(path))


def _jpeg(path):
    from PIL import Image
    from PIL.TiffImagePlugin import IFDRational

    img = Image.new("RGB", (64, 48), (128, 128, 128))
    exif = img.getexif()
    exif[271] = "GoPro"
    exif[272] = "HERO5"
    gps = exif.get_ifd(34853)
    gps[1] = "N"
    gps[2] = (IFDRational(3775, 100), IFDRational(0, 1), IFDRational(0, 1))
    gps[3] = "W"
    gps[4] = (IFDRational(12245, 100), IFDRational(0, 1), IFDRational(0, 1))
    img.save(str(path), "jpeg", exif=exif.tobytes())


def _mixed_folder(tmp_path):
    """tmp_path/mix: one .gpkg, one .geojson, one .las, one .jpg, one .tif."""
    for src in (BOROUGHS, GPKG):
        if not src.exists():
            pytest.skip(f"sample data missing: {src}")
    d = tmp_path / "mix"
    d.mkdir()
    shutil.copy(GPKG, d / "a.gpkg")
    shutil.copy(BOROUGHS, d / "b.geojson")
    _las(d / "c.las")
    _jpeg(d / "d.jpg")
    (d / "e.tif").write_bytes(b"")  # listed only, never opened
    return d


def _members_of(c, opts):
    return _paths(c.get_partitions("mix", {"path": "mix", **opts}))


def test_explicit_format_on_a_mixed_folder_keeps_only_that_format(tmp_path):
    _mixed_folder(tmp_path)
    c = GeoBrixLakeflowConnect({"root_path": str(tmp_path)})
    opts = {"path": "mix", "format": "gpkg"}
    assert _members_of(c, {"format": "gpkg"}) == ["a.gpkg"]
    names = [f.name for f in c.get_table_schema("mix", opts).fields]
    assert names[-2:] == ["_source_path", "_source_mtime"] and "geom" in names


def test_generic_vector_format_excludes_lidar_exif_and_raster(tmp_path):
    _mixed_folder(tmp_path)
    c = GeoBrixLakeflowConnect({"root_path": str(tmp_path)})
    assert _members_of(c, {"format": "vector"}) == ["a.gpkg", "b.geojson"]
    assert _members_of(c, {"format": "raster"}) == ["e.tif"]
    assert _members_of(c, {"format": "gtiff"}) == ["e.tif"]
    assert c.get_table_schema("mix", {"path": "mix", "format": "vector"}).fieldNames()


def test_single_kind_folder_infers_the_reader(tmp_path):
    d = tmp_path / "mix"
    d.mkdir()
    shutil.copy(BOROUGHS, d / "a.geojson")
    shutil.copy(BOROUGHS, d / "b.json")
    (d / "notes.txt").write_text("ignored")
    c = GeoBrixLakeflowConnect({"root_path": str(tmp_path)})
    assert _members_of(c, {}) == ["a.geojson", "b.json"]
    assert "boroname" in c.get_table_schema("mix", {"path": "mix"}).fieldNames()
    assert c._formats["mix"] == "geojson_gbx"


def test_mixed_kinds_without_format_is_a_clear_error(tmp_path):
    _mixed_folder(tmp_path)
    c = GeoBrixLakeflowConnect({"root_path": str(tmp_path)})
    with pytest.raises(ValueError, match="set the `format` table option"):
        c.get_table_schema("mix", {"path": "mix"})


def test_zipped_shapefile_and_gdb_members_map_to_their_readers(tmp_path):
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import (
        format_for,
    )

    d = tmp_path / "mix"
    d.mkdir()
    (d / "a.shp.zip").write_bytes(b"")
    (d / "b.gdb.zip").write_bytes(b"")
    c = GeoBrixLakeflowConnect({"root_path": str(tmp_path)})
    assert _members_of(c, {"format": "shapefile"}) == ["a.shp.zip"]
    assert _members_of(c, {"format": "file_gdb"}) == ["b.gdb.zip"]
    assert _members_of(c, {}) == ["a.shp.zip", "b.gdb.zip"]
    assert format_for({}, [str(d / "a.shp.zip")]) == "shapefile_gbx"
    assert format_for({}, [str(d / "b.gdb.zip")]) == "file_gdb_gbx"


def test_gdb_table_path_is_a_single_member_read_with_the_filegdb_reader(tmp_path):
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import (
        format_for,
    )

    gdb = tmp_path / "survey.gdb"
    gdb.mkdir()
    c = GeoBrixLakeflowConnect({"root_path": str(tmp_path)})
    assert c.list_tables() == ["survey"]
    members = c._members(c._resolve_path("survey", {}), {})
    assert members == [str(gdb)]
    assert format_for({}, members) == "file_gdb_gbx"


def test_a_later_file_of_another_kind_fails_instead_of_using_another_reader(tmp_path):
    d = tmp_path / "mix"
    d.mkdir()
    shutil.copy(BOROUGHS, d / "a.geojson")
    c = GeoBrixLakeflowConnect({"root_path": str(tmp_path)})
    opts = {"path": "mix"}
    c.get_table_schema("mix", opts)  # pins geojson_gbx for the table
    _las(d / "late.las")
    with pytest.raises(
        ValueError, match="lidar_gbx file in a table read with geojson_gbx"
    ):
        list(c.iter_member_batches(str(d / "late.las"), 0, opts, table_name="mix"))


def test_exif_table_reads_through_the_connector(tmp_path):
    d = tmp_path / "photos"
    d.mkdir()
    _jpeg(d / "a.jpg")
    c = GeoBrixLakeflowConnect({"root_path": str(tmp_path)})
    assert c.list_tables() == ["photos"]
    rows, _ = c.read_table("photos", {}, {})
    rows = list(rows)
    assert len(rows) == 1
    assert (rows[0]["camera_make"], rows[0]["camera_model"]) == ("GoPro", "HERO5")
    assert rows[0]["latitude"] == pytest.approx(37.75)
    assert rows[0]["_source_path"] == str(d / "a.jpg")


# ---------------------------------------------------------------- offsets
def _three_files(root):
    d = root / "nyc_boroughs"
    shutil.copy(BOROUGHS, d / "part_b.geojson")
    shutil.copy(BOROUGHS, d / "part_c.geojson")
    set_mtime(d / "part_a.geojson", 100_000)
    set_mtime(d / "part_b.geojson", 200_000)
    set_mtime(d / "part_c.geojson", 200_000)
    return d


def _paths(parts):
    return sorted(os.path.basename(p["path"]) for p in parts)


def test_streaming_offsets_select_only_new_files(root):
    d = _three_files(root)
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    opts = {"path": "nyc_boroughs", "format": "geojson"}

    end1 = c.latest_offset("t", opts, {})
    assert end1 == {
        "max_mtime_ns": 200_000,
        "at_max": [str(d / "part_b.geojson"), str(d / "part_c.geojson")],
    }
    assert _paths(c.get_partitions("t", opts, {}, end1)) == [
        "part_a.geojson",
        "part_b.geojson",
        "part_c.geojson",
    ]

    # Nothing new: next micro-batch is empty.
    assert c.get_partitions("t", opts, end1, c.latest_offset("t", opts, end1)) == []

    # A late file with the SAME mtime as the previous max is still picked up next time.
    shutil.copy(BOROUGHS, d / "part_d.geojson")
    set_mtime(d / "part_d.geojson", 200_000)
    end2 = c.latest_offset("t", opts, end1)
    assert _paths(c.get_partitions("t", opts, end1, end2)) == ["part_d.geojson"]


def test_file_landing_after_the_end_offset_is_deferred_not_double_read(root):
    d = _three_files(root)
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    opts = {"path": "nyc_boroughs", "format": "geojson"}
    end1 = c.latest_offset("t", opts, {})

    # Lands after end1 was taken, with mtime == end1.max but not in end1.at_max.
    shutil.copy(BOROUGHS, d / "part_late.geojson")
    set_mtime(d / "part_late.geojson", 200_000)
    batch1 = _paths(c.get_partitions("t", opts, {}, end1))
    assert "part_late.geojson" not in batch1

    end2 = c.latest_offset("t", opts, end1)
    assert _paths(c.get_partitions("t", opts, end1, end2)) == ["part_late.geojson"]


def test_batch_partitions_cover_every_file(root):
    _three_files(root)
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    assert (
        len(c.get_partitions("t", {"path": "nyc_boroughs", "format": "geojson"})) == 3
    )


def test_file_glob_filters_members(root):
    _three_files(root)
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    parts = c.get_partitions(
        "t", {"path": "nyc_boroughs", "format": "geojson", "file_glob": "part_[ab]*"}
    )
    assert _paths(parts) == ["part_a.geojson", "part_b.geojson"]


# ---------------------------------------------------------------- dict contract
def test_read_partition_and_read_table_yield_rows_with_lineage(root):
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    opts = {"path": "nyc_boroughs", "format": "geojson"}
    part = c.get_partitions("t", opts)[0]
    rows = list(c.read_partition("t", part, opts))
    assert len(rows) == 5
    assert rows[0]["_source_path"] == part["path"]
    assert rows[0]["_source_mtime"].timestamp() == pytest.approx(
        part["mtime_ns"] / 1e9, abs=1e-5
    )

    records, end = c.read_table("t", {}, opts)
    assert len(list(records)) == 5
    assert end == c.latest_offset("t", opts, {})
    records2, _ = c.read_table("t", end, opts)
    assert list(records2) == []


# ---------------------------------------------------------------- Spark reads
def _read(spark, **options):
    return spark.read.format("lakeflow_connect").options(**options).load()


def test_spark_batch_read_geojson_folder(spark, root):
    df = _read(spark, tableName="nyc_boroughs", root_path=str(root), format="geojson")
    assert df.count() == 5
    assert df.columns[-2:] == ["_source_path", "_source_mtime"]
    row = df.select("_source_path", "_source_mtime").first()
    assert row["_source_path"].endswith("part_a.geojson")
    expected = os.stat(root / "nyc_boroughs" / "part_a.geojson").st_mtime_ns / 1e9
    assert row["_source_mtime"].timestamp() == pytest.approx(expected, abs=1e-5)


def test_spark_batch_read_gpkg_default_layer_and_named_layer(spark, root):
    assert (
        _read(
            spark, tableName="nyc_complete", root_path=str(root), format="gpkg"
        ).count()
        == 5
    )
    zones = _read(
        spark,
        tableName="nyc_complete",
        root_path=str(root),
        format="gpkg",
        layer="taxi_zones",
    )
    assert zones.count() == 263
    assert "locationid" in zones.columns


def test_spark_where_option_is_applied(spark, root):
    df = _read(
        spark,
        tableName="nyc_boroughs",
        root_path=str(root),
        format="geojson",
        where="boroname = 'Manhattan'",
    )
    assert [r.boroname for r in df.select("boroname").collect()] == ["Manhattan"]


def test_spark_framework_metadata_virtual_table(spark, root):
    df = _read(
        spark,
        tableName="_community_table_metadata",
        tableNameList=json.dumps(["nyc_boroughs"]),
        root_path=str(root),
    )
    row = df.first()
    assert (row["tableName"], row["ingestion_type"], row["cursor_field"]) == (
        "nyc_boroughs",
        "append",
        "_source_mtime",
    )


def test_spark_streaming_available_now_appends_only_new_files(
    spark, root, tmp_path_factory
):
    out = tmp_path_factory.mktemp("sink")
    checkpoint, sink = str(out / "checkpoint"), str(out / "data")

    def run_once():
        q = (
            spark.readStream.format("lakeflow_connect")
            .option("tableName", "nyc_boroughs")
            .option("root_path", str(root))
            .option("format", "geojson")
            .load()
            .writeStream.format("parquet")
            .option("checkpointLocation", checkpoint)
            .option("path", sink)
            .trigger(availableNow=True)
            .start()
        )
        q.awaitTermination()
        assert q.exception() is None

    run_once()
    first = spark.read.parquet(sink)
    assert first.count() == 5

    shutil.copy(BOROUGHS, root / "nyc_boroughs" / "part_b.geojson")
    run_once()
    total = spark.read.parquet(sink)
    assert total.count() == 10
    assert total.select("_source_path").distinct().count() == 2

    run_once()  # nothing new
    assert spark.read.parquet(sink).count() == 10
