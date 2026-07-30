import os

from databricks.labs.gbx.ds.file import FileGbxReader
from databricks.labs.gbx.ds import _listing


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
    assert by_name["b.TIFF"][2] == "tiff"  # lowercased
    assert by_name["noext"][2] is None  # null when no extension
    assert all(row[3] >= 1 for row in rows)  # size
    assert by_name["a.tif"][0].endswith("a.tif")  # path


def test_filter_regex(tmp_path):
    _touch(str(tmp_path / "keep.tif"))
    _touch(str(tmp_path / "skip.nc"))
    r = FileGbxReader({"path": str(tmp_path), "filterRegex": r".*\.tif$"})
    rows = [row for part in r.partitions() for row in r.read(part)]
    assert [row[1] for row in rows] == ["keep.tif"]


def test_read_retries_transient_stat_failure(tmp_path, monkeypatch):
    """read() must succeed when os.stat raises FileNotFoundError transiently.
    Simulates UC Volume FUSE eventual-consistency: first call fails, second succeeds."""
    _touch(str(tmp_path / "retry.tif"), b"x" * 16)
    r = FileGbxReader({"path": str(tmp_path)})
    parts = r.partitions()
    assert len(parts) == 1

    real_stat = os.stat
    call_count = [0]

    def _flaky_stat(path, *args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise FileNotFoundError("transient FUSE miss")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(_listing.time, "sleep", lambda s: None)
    monkeypatch.setattr(os, "stat", _flaky_stat)
    rows = list(r.read(parts[0]))
    assert len(rows) == 1
    assert rows[0][1] == "retry.tif"
    assert call_count[0] == 2  # one failure then success


def test_never_reads_content(tmp_path):
    # A non-raster file must list fine — proving no raster/content open.
    _touch(str(tmp_path / "notaraster.tif"), b"not a tiff at all")
    r = FileGbxReader({"path": str(tmp_path)})
    rows = [row for part in r.partitions() for row in r.read(part)]
    assert rows[0][1] == "notaraster.tif"  # listed, never opened/decoded
