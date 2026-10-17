"""geobrix Lakeflow connector: contract, configuration, offsets, Spark reads."""

import json
import os
import shutil

import pytest
from conftest import BOROUGHS, set_mtime

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
            "tableName": "zones",
            "databricks.connection": "conn",
            "sourceName": "geobrix",
            "root_path": "/Volumes/c/s/v",
            "path": "zones",
        }
    )
    assert opts == {
        "layerName": "taxi_zones",
        "chunkSize": "500",
        "where": "borough = 'Queens'",
        "bbox": "-74.1,40.6,-73.8,40.9",
    }


def test_unknown_format_is_rejected():
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import format_for

    with pytest.raises(ValueError, match="does not support"):
        format_for({"format": "kml"}, ["/x/a.kml"])


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
    assert meta == {"primary_keys": [], "cursor_field": "_source_mtime", "ingestion_type": "append"}


def test_format_aliases_resolve_to_reader_names():
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import (
        FORMAT_ALIASES,
    )
    from databricks.labs.gbx.ds.paths import resolve_format

    for alias, name in FORMAT_ALIASES.items():
        assert resolve_format(name).name() == name, alias


def test_discovery_infers_format_from_extension(tmp_path):
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import format_for

    assert format_for({}, [str(tmp_path / "a.gpkg")]) == "gpkg_gbx"
    assert format_for({}, [str(tmp_path / "a.laz")]) == "lidar_gbx"
    assert format_for({}, [str(tmp_path / "a.jpg")]) == "exif_gbx"
    assert format_for({"format": "geojson"}, [str(tmp_path / "a.json")]) == "geojson_gbx"


def test_mixed_folder_without_format_errors(tmp_path):
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import format_for

    with pytest.raises(ValueError, match="format"):
        format_for({}, [str(tmp_path / "a.gpkg"), str(tmp_path / "b.geojson")])


def test_connector_reads_through_shared_core(root, monkeypatch):
    """Wiring: member reads go through ds.paths.iter_path_batches."""
    from databricks.labs.community_connector.sources.geobrix import lakeflow_connect as LC
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
    laspy = pytest.importorskip("laspy")
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
    assert end1 == {"max_mtime_ns": 200_000, "at_max": [str(d / "part_b.geojson"), str(d / "part_c.geojson")]}
    assert _paths(c.get_partitions("t", opts, {}, end1)) == ["part_a.geojson", "part_b.geojson", "part_c.geojson"]

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
    assert len(c.get_partitions("t", {"path": "nyc_boroughs", "format": "geojson"})) == 3


def test_file_glob_filters_members(root):
    _three_files(root)
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    parts = c.get_partitions("t", {"path": "nyc_boroughs", "format": "geojson", "file_glob": "part_[ab]*"})
    assert _paths(parts) == ["part_a.geojson", "part_b.geojson"]


# ---------------------------------------------------------------- dict contract
def test_read_partition_and_read_table_yield_rows_with_lineage(root):
    c = GeoBrixLakeflowConnect({"root_path": str(root)})
    opts = {"path": "nyc_boroughs", "format": "geojson"}
    part = c.get_partitions("t", opts)[0]
    rows = list(c.read_partition("t", part, opts))
    assert len(rows) == 5
    assert rows[0]["_source_path"] == part["path"]
    assert rows[0]["_source_mtime"].timestamp() == pytest.approx(part["mtime_ns"] / 1e9, abs=1e-5)

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
    assert _read(spark, tableName="nyc_complete", root_path=str(root), format="gpkg").count() == 5
    zones = _read(spark, tableName="nyc_complete", root_path=str(root), format="gpkg", layer="taxi_zones")
    assert zones.count() == 263
    assert "locationid" in zones.columns


def test_spark_where_option_is_applied(spark, root):
    df = _read(
        spark, tableName="nyc_boroughs", root_path=str(root), format="geojson",
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
        "nyc_boroughs", "append", "_source_mtime",
    )


def test_spark_streaming_available_now_appends_only_new_files(spark, root, tmp_path_factory):
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
