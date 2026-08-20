"""Task 7: vector_file_write — function-layer FILE write."""

from unittest.mock import MagicMock, patch


def test_vector_file_write_managed_builds_bytes_row_and_delegates(tmp_path):
    from databricks.labs.gbx.pyvx.file_write import vector_file_write

    out = tmp_path / "out.geojson"
    out.write_bytes(b'{"type":"FeatureCollection","features":[]}')
    spark = MagicMock()
    spark.createDataFrame.return_value = "DF"
    with patch("databricks.labs.gbx.pyvx.file_write.gbx_file_write") as gfw:
        vector_file_write(
            spark,
            str(out),
            "cat.sch.tbl",
            driver="GeoJSON",
            file_mode="managed",
            filespace="/Volumes/c/s/v",
        )
    rows = spark.createDataFrame.call_args[0][0]
    assert rows[0]["tile"]["raster"] is not None
    assert rows[0]["tile"]["path"] is None
    gfw.assert_called_once()
    assert gfw.call_args.kwargs["file_mode"] == "managed"
    assert gfw.call_args.kwargs["filespace"] == "/Volumes/c/s/v"


def test_vector_file_write_external_copies_and_references(tmp_path):
    import os

    from databricks.labs.gbx.pyvx.file_write import vector_file_write

    source_bytes = b'{"type":"FeatureCollection","features":[]}'
    out = tmp_path / "out.geojson"
    out.write_bytes(source_bytes)
    vol = tmp_path / "vol"
    vol.mkdir()
    spark = MagicMock()
    with patch("databricks.labs.gbx.pyvx.file_write.gbx_file_write") as gfw:
        vector_file_write(
            spark,
            str(out),
            "cat.sch.tbl",
            driver="GeoJSON",
            file_mode="external",
            filespace=str(vol),
        )
    rows = spark.createDataFrame.call_args[0][0]
    assert rows[0]["tile"]["raster"] is None
    volume_path = rows[0]["tile"]["path"]
    assert volume_path.startswith(str(vol))
    assert os.path.isfile(volume_path), f"expected copied file at {volume_path}"
    assert open(volume_path, "rb").read() == source_bytes, "copied bytes must match source"
    gfw.assert_called_once()
    assert gfw.call_args.kwargs["file_mode"] == "external"
    assert gfw.call_args.kwargs["filespace"] is None


def test_vector_file_write_layout_forwarded(tmp_path):
    """layout kwarg is forwarded to gbx_file_write."""
    from databricks.labs.gbx.pyvx.file_write import vector_file_write

    out = tmp_path / "out.gpkg"
    out.write_bytes(b"GPKG")
    spark = MagicMock()
    spark.createDataFrame.return_value = "DF"
    with patch("databricks.labs.gbx.pyvx.file_write.gbx_file_write") as gfw:
        vector_file_write(
            spark,
            str(out),
            "cat.sch.t",
            driver="GPKG",
            file_mode="managed",
            filespace="/Volumes/c/s/v",
            layout="plain",
        )
    assert gfw.call_args.kwargs["layout"] == "plain"


def test_vector_file_write_target_forwarded(tmp_path):
    """target is forwarded as the first positional arg to gbx_file_write."""
    from databricks.labs.gbx.pyvx.file_write import vector_file_write

    out = tmp_path / "out.gpkg"
    out.write_bytes(b"GPKG")
    spark = MagicMock()
    spark.createDataFrame.return_value = "DF"
    with patch("databricks.labs.gbx.pyvx.file_write.gbx_file_write") as gfw:
        vector_file_write(
            spark,
            str(out),
            "my_catalog.my_schema.my_roads",
            driver="GPKG",
            file_mode="managed",
            filespace="/Volumes/c/s/v",
        )
    assert gfw.call_args.args[1] == "my_catalog.my_schema.my_roads"


def test_vector_file_write_external_no_filespace_raises(tmp_path):
    """external mode without filespace raises ValueError."""
    from databricks.labs.gbx.pyvx.file_write import vector_file_write

    out = tmp_path / "out.gpkg"
    out.write_bytes(b"x")
    spark = MagicMock()
    with patch("databricks.labs.gbx.pyvx.file_write.gbx_file_write"):
        try:
            vector_file_write(
                spark,
                str(out),
                "cat.sch.t",
                driver="GPKG",
                file_mode="external",
                filespace=None,
            )
            raised = False
        except ValueError as e:
            raised = True
            assert "filespace" in str(e).lower()
    assert raised, "expected ValueError for external without filespace"


def test_vector_file_write_invalid_mode_raises(tmp_path):
    """Unknown file_mode raises ValueError."""
    from databricks.labs.gbx.pyvx.file_write import vector_file_write

    out = tmp_path / "out.gpkg"
    out.write_bytes(b"x")
    spark = MagicMock()
    try:
        vector_file_write(
            spark,
            str(out),
            "cat.sch.t",
            driver="GPKG",
            file_mode="fuse",
        )
        raised = False
    except ValueError:
        raised = True
    assert raised, "expected ValueError for unknown file_mode"
