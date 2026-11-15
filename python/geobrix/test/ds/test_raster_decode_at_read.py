"""``raster_gbx`` with ``virtualTiles=false``: a plain read decodes whole files at read, in bounded Arrow batches.

Planning opens no header; a task takes ``files_per_task(n)`` files and yields them through
``tile_batches.materialize_batches``. The rows are the rows the per-file plan gave.
"""

import os
from test._volume_safe import open_raster

import numpy as np
import pyarrow as pa
import pytest
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds import _quarantine
from databricks.labs.gbx.ds import raster as R
from databricks.labs.gbx.pyrx import tile_batches


def _tif(path, size=32, value=1.0, dtype="float32"):
    with open_raster(
        path, "w", driver="GTiff", width=size, height=size, count=1, dtype=dtype, crs="EPSG:4326",
        transform=from_origin(0, size, 1, 1),
    ) as dst:  # fmt: skip
        dst.write(np.full((1, size, size), value, dtype=dtype))
    return str(path)


def _folder(tmp_path, n, size=32):
    return [
        _tif(tmp_path / f"t{i:04d}.tif", size=size, value=float(i)) for i in range(n)
    ]


def _reader(tmp_path, **opts):
    return R.RasterGbxReader({"path": str(tmp_path), "virtualTiles": "false", **opts})


def _rows(reader, parts):
    """Every (source, tile dict) a reader gives for `parts`, in order."""
    out = []
    keys = (
        "cellid",
        "raster",
        "path",
        "path_mode",
        "window",
        "clip_polygon",
        "clip_crs",
        "crs",
        "metadata",
    )
    for p in parts:
        for item in reader.read(p):
            if isinstance(item, pa.RecordBatch):
                for r in item.to_pylist():
                    r["tile"]["metadata"] = dict(r["tile"]["metadata"] or ())
                    out.append((r["source"], r["tile"]))
            else:
                src, row = item
                out.append((src, dict(zip(keys, row))))
    return out


@pytest.mark.parametrize(
    "n,per_task",
    [(1, 1), (127, 1), (128, 2), (1000, 8), (10000, 8), (512, 8), (200, 3)],
)
def test_files_per_task_rule(n, per_task):
    assert R.files_per_task(n) == per_task


def test_planning_a_folder_opens_no_header_and_groups_the_files(
    tmp_path, named_raster_reads, monkeypatch
):
    files = _folder(tmp_path, 10)
    opened = named_raster_reads
    monkeypatch.setattr(R, "_FILES_PER_TASK_FROM", 3)  # 3 files per task for ten files
    parts = _reader(tmp_path).partitions()
    assert opened == [], "the planner opened a header"
    assert [len(p.file_paths) for p in parts] == [3, 3, 3, 1]
    assert [f for p in parts for f in p.file_paths] == files


@pytest.mark.parametrize(
    "opts", [{"tileSize": "16"}, {"windows": "[0,0,16,16]"}, {"sizeInMB": "1"}]
)
def test_a_read_with_a_selector_or_a_split_budget_is_planned_a_tile_at_a_time(
    tmp_path, opts
):
    _folder(tmp_path, 2)
    parts = _reader(tmp_path, **opts).partitions()
    assert parts and all(isinstance(p, R._TilePartition) for p in parts)


def test_the_batched_rows_are_the_rows_of_the_per_file_plan(tmp_path, monkeypatch):
    _folder(tmp_path, 7)
    monkeypatch.setattr(R, "_FILES_PER_TASK_FROM", 3)
    reader = _reader(tmp_path)
    batched = _rows(reader, reader.partitions())
    per_file = []
    for f in sorted(p for q in reader.partitions() for p in q.file_paths):
        per_file += _rows(reader, R._plan_partitions_for_file(f, 0, emit_virtual=False))
    assert [s for s, _ in batched] == [s for s, _ in per_file] and len(batched) == 7
    for (_, got), (_, want) in zip(batched, per_file):
        assert got == want


def test_a_task_yields_batches_bounded_by_decoded_bytes_in_order(tmp_path, monkeypatch):
    _folder(tmp_path, 5)
    monkeypatch.setattr(R, "_FILES_PER_TASK_FROM", 1)
    monkeypatch.setattr(R, "_FILES_PER_TASK_MAX", 5)
    reader = _reader(tmp_path)
    (part,) = reader.partitions()
    assert len(part.file_paths) == 5
    monkeypatch.setattr(tile_batches, "_OUT_CHUNK_BYTES", 1)  # a batch per tile
    batches = list(reader.read(part))
    assert [b.num_rows for b in batches] == [1] * 5
    got = [b.to_pylist()[0]["source"] for b in batches]
    assert got == [R._listing.to_spark_uri(f) for f in part.file_paths]
    monkeypatch.setattr(tile_batches, "_OUT_CHUNK_BYTES", 1 << 30)
    assert [b.num_rows for b in reader.read(part)] == [5]


def test_a_batch_never_holds_more_than_the_decoded_budget(monkeypatch):
    from databricks.labs.gbx.pyrx.core import budget

    monkeypatch.setattr(budget, "decoded_budget_bytes", lambda strategy: 10)
    assert tile_batches.out_chunk_bytes() == 10
    monkeypatch.setattr(budget, "decoded_budget_bytes", lambda strategy: 1 << 40)
    assert tile_batches.out_chunk_bytes() == 64 << 20


def test_a_tile_over_the_read_cap_fails_the_read_with_its_fix(tmp_path):
    _folder(tmp_path, 1, size=64)
    reader = _reader(tmp_path)
    reader._read_cap = 1000  # the driver's cap: the file is bigger
    (part,) = reader.partitions()
    with pytest.raises(ValueError, match="retile it first"):
        list(reader.read(part))


def test_the_read_cap_is_the_drivers_connect_aware_cap(tmp_path, monkeypatch):
    from databricks.labs.gbx.ds import file_gbx
    from databricks.labs.gbx.ds.file_gbx import report_detected_cap

    monkeypatch.delenv("GBX_STREAM_MAX_BYTES", raising=False)
    _folder(tmp_path, 1)
    # with a session (the driver): classic here (256 MiB); Serverless is 64 MiB
    session = object()
    monkeypatch.setattr(file_gbx, "_resolve_session_for_cap", lambda: session)
    assert _reader(tmp_path)._read_cap == report_detected_cap(session)
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "12345")
    assert _reader(tmp_path)._read_cap == 12345
    assert (
        R.RasterGbxReader({"path": str(tmp_path)})._read_cap is None
    )  # virtual tiles decode nothing at read


def test_a_reader_with_no_session_enforces_the_cap_the_driver_carried(
    tmp_path, monkeypatch
):
    """The planner process and the per-file readers of read_paths may have no active session, where the cap would
    resolve to the classic 256 MiB: the registration carries the driver's 64 MiB into the reader, and nothing
    re-resolves it."""
    from pyspark.sql import SparkSession

    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.ds import file_gbx

    _folder(tmp_path, 1)
    monkeypatch.delenv("GBX_STREAM_MAX_BYTES", raising=False)
    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: None))
    source = _gdal_policy.carrying_source(R.RasterGbxDataSource, True, 64 << 20)(
        {"path": str(tmp_path), "virtualTiles": "false"}
    )
    reader = source.reader(source.schema())
    monkeypatch.setattr(
        file_gbx,
        "report_detected_cap",
        lambda *a: pytest.fail("the carried cap was resolved again"),
    )
    assert reader._read_cap == 64 << 20
    with pytest.raises(ValueError, match="retile it first"):
        tile_batches.refuse_over_cap(65 << 20, reader._read_cap, "the tile")
    tile_batches.refuse_over_cap(63 << 20, reader._read_cap, "the tile")


def test_registering_the_sources_carries_the_session_cap_into_the_reader(
    tmp_path, monkeypatch
):
    """``register_sources`` resolves the session's cap and hands it to each registered source: a dropped argument
    would leave the readers to resolve it where there is no session."""
    from databricks.labs.gbx import manager
    from databricks.labs.gbx.ds import file_gbx

    _folder(tmp_path, 1)
    seen = []
    monkeypatch.setattr(file_gbx, "report_detected_cap", lambda *a: 12345)

    class FakeSpark:
        class dataSource:  # noqa: N801
            register = staticmethod(seen.append)

    assert manager.register_sources(FakeSpark, only=["raster_gbx"]) == ["raster_gbx"]
    (registered,) = seen
    source = registered({"path": str(tmp_path), "virtualTiles": "false"})
    assert source.reader(source.schema())._read_cap == 12345


def test_a_reader_with_no_carried_cap_and_no_session_fails_safe_to_the_serverless_cap(
    tmp_path, monkeypatch
):
    """A reader built outside registration and ``read_paths`` resolves its cap lazily; on a worker there is no
    session, which cannot tell Serverless from classic: it must take the 64 MiB, not the classic 256 MiB.
    """
    from databricks.labs.gbx.ds import file_gbx

    _folder(tmp_path, 1)
    monkeypatch.delenv("GBX_STREAM_MAX_BYTES", raising=False)
    monkeypatch.setattr(file_gbx, "_resolve_session_for_cap", lambda: None)
    assert _reader(tmp_path)._read_cap == 64 << 20
    monkeypatch.setenv(
        "GBX_STREAM_MAX_BYTES", "12345"
    )  # an explicit override still wins
    assert _reader(tmp_path)._read_cap == 12345


def test_a_corrupt_file_among_good_ones_fails_the_read(tmp_path, monkeypatch):
    _folder(tmp_path, 3)
    (tmp_path / "t0001.tif").write_bytes(b"not a raster")
    monkeypatch.setattr(R, "_FILES_PER_TASK_FROM", 1)
    monkeypatch.setattr(R, "_FILES_PER_TASK_MAX", 3)
    reader = _reader(tmp_path)
    (part,) = reader.partitions()
    with pytest.raises(rasterio.errors.RasterioIOError, match="t0001.tif"):
        list(reader.read(part))


def _three_with_a_corrupt_middle(tmp_path, monkeypatch):
    _folder(tmp_path, 3)
    bad = tmp_path / "t0001.tif"
    bad.write_bytes(b"not a raster")
    monkeypatch.setattr(R, "_FILES_PER_TASK_FROM", 1)
    monkeypatch.setattr(R, "_FILES_PER_TASK_MAX", 3)
    reader = _reader(tmp_path)
    (part,) = reader.partitions()
    assert [os.path.basename(p) for p in part.file_paths] == [
        "t0000.tif",
        "t0001.tif",
        "t0002.tif",
    ]
    return reader, part, str(bad)


def test_a_bad_file_in_a_batch_is_set_aside_by_its_own_path_and_the_others_kept(
    tmp_path, monkeypatch
):
    """With the bad-record hook of read_paths(on_error="skip"|"quarantine") the failure unit is the file: the middle
    file is recorded under its own path (not the task's first file) and left out, files 1 and 3 are read. Without
    the hook the read fails naming the middle file."""
    reader, part, bad = _three_with_a_corrupt_middle(tmp_path, monkeypatch)
    with pytest.raises(rasterio.errors.RasterioIOError) as failed:
        list(reader.read(part))
    notes = " ".join(getattr(failed.value, "__notes__", ()))
    assert bad in str(failed.value) + notes

    recorded = []
    setattr(
        reader,
        _quarantine.HOOK,
        lambda *a: recorded.append(a),
    )
    rows = _rows(reader, [part])
    assert [os.path.basename(s) for s, _ in rows] == ["t0000.tif", "t0002.tif"]
    assert all(t["raster"] for _, t in rows)
    ((index, error_class, message),) = [a[:3] for a in recorded]
    assert index is None and "RasterioIOError" in error_class and bad in message


def test_a_transient_miss_reading_a_header_is_retried_and_a_bad_file_is_not(
    tmp_path, monkeypatch
):
    from databricks.labs.gbx.core import _retry

    monkeypatch.setattr(_retry.time, "sleep", lambda s: None)
    (path,) = _folder(tmp_path, 1)
    real, calls = R.open_raster, []

    def flaky(p, *a, **k):
        calls.append(p)
        if len(calls) == 1:
            raise FileNotFoundError(2, "No such file or directory", p)
        return real(p, *a, **k)

    monkeypatch.setattr(R, "open_raster", flaky)
    assert R._whole_file_header(path)[2] == 32 and len(calls) == 2
    bad = tmp_path / "bad.tif"
    bad.write_bytes(b"not a raster")
    calls.clear()
    monkeypatch.setattr(R, "open_raster", real)
    with pytest.raises(rasterio.errors.RasterioIOError):
        R._whole_file_header(str(bad))


def test_a_gdal_worded_missing_file_with_no_errno_is_retried(tmp_path, monkeypatch):
    # GDAL words a file a Serverless Volume does not show yet as an IO error with no errno.
    from databricks.labs.gbx.core import _retry

    monkeypatch.setattr(_retry.time, "sleep", lambda s: None)
    (path,) = _folder(tmp_path, 1)
    real, calls = R.open_raster, []

    def flaky(p, *a, **k):
        calls.append(p)
        if len(calls) == 1:
            raise rasterio.errors.RasterioIOError(
                f"{p} does not exist in the file system, and is not recognized as a supported dataset name."
            )
        return real(p, *a, **k)

    monkeypatch.setattr(R, "open_raster", flaky)
    assert R._whole_file_header(path)[2] == 32 and len(calls) == 2


def test_a_non_gtiff_file_is_re_encoded_to_the_row_of_the_per_file_plan(tmp_path):
    path = str(tmp_path / "a.img")
    with open_raster(
        path, "w", driver="HFA", width=16, height=16, count=2, dtype="uint8", crs="EPSG:32633",
        transform=from_origin(500000, 4000000, 10, 10),
    ) as dst:  # fmt: skip
        dst.write(np.arange(512, dtype="uint8").reshape(2, 16, 16))
    reader = _reader(tmp_path, extensions="img")
    (part,) = reader.partitions()
    assert part.file_paths == [path]
    batched = _rows(reader, [part])
    per_file = _rows(reader, R._plan_partitions_for_file(path, 0, emit_virtual=False))
    assert len(batched) == len(per_file) == 1
    (src_b, got), (src_p, want) = batched[0], per_file[0]
    assert src_b == src_p and got["crs"] == want["crs"] == "EPSG:32633"
    assert got["window"] == want["window"] and got["raster"] and want["raster"]
    assert got["metadata"] == want["metadata"]
    with (
        rasterio.MemoryFile(got["raster"]) as a,
        rasterio.MemoryFile(want["raster"]) as b,
        a.open() as da,
        b.open() as db,
    ):
        assert da.driver == "GTiff" and da.crs == db.crs
        assert da.transform == db.transform
        assert np.array_equal(da.read(), db.read())
