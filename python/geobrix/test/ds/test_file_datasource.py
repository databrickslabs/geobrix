import os
from databricks.labs.gbx.ds.file import FileGbxReader, FILE_SCHEMA


def _touch(p, data=b"x"):
    with open(p, "wb") as f:
        f.write(data)


def test_lists_files_with_extension(tmp_path):
    _touch(str(tmp_path / "a.tif"))
    _touch(str(tmp_path / "b.TIFF"))
    _touch(str(tmp_path / "noext"))
    r = FileGbxReader({"path": str(tmp_path)})
    rows = [row for part in r.partitions() for row in r.read(part)]
    by_name = {row[1]: row for row in rows}  # name -> (path,name,ext,size,mtime)
    assert by_name["a.tif"][2] == "tif"
    assert by_name["b.TIFF"][2] == "tiff"      # lowercased
    assert by_name["noext"][2] is None          # null when no extension
    assert all(row[3] >= 1 for row in rows)     # size
    assert by_name["a.tif"][0].endswith("a.tif")  # path


def test_filter_regex(tmp_path):
    _touch(str(tmp_path / "keep.tif"))
    _touch(str(tmp_path / "skip.nc"))
    r = FileGbxReader({"path": str(tmp_path), "filterRegex": r".*\.tif$"})
    rows = [row for part in r.partitions() for row in r.read(part)]
    assert [row[1] for row in rows] == ["keep.tif"]


def test_never_reads_content(tmp_path):
    # A non-raster file must list fine — proving no raster/content open.
    _touch(str(tmp_path / "notaraster.tif"), b"not a tiff at all")
    r = FileGbxReader({"path": str(tmp_path)})
    rows = [row for part in r.partitions() for row in r.read(part)]
    assert rows[0][1] == "notaraster.tif"  # listed, never opened/decoded
