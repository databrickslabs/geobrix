"""``raster_gbx`` with ``virtualTiles=false``: a plain read decodes whole files at read, in bounded Arrow batches.

Planning opens no header; a task takes ``files_per_task(n)`` files and yields them through
``tile_batches.materialize_batches``. The rows are the rows the per-file plan gave.
"""

import numpy as np
import pyarrow as pa
import pytest
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds import raster as R
from databricks.labs.gbx.pyrx import tile_batches


def _tif(path, size=32, value=1.0, dtype="float32"):
    with rasterio.open(
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


def test_the_read_cap_is_the_drivers_connect_aware_cap(tmp_path):
    _folder(tmp_path, 1)
    assert (
        _reader(tmp_path)._read_cap == 256 << 20
    )  # classic here; Serverless is 64 MiB
    assert (
        R.RasterGbxReader({"path": str(tmp_path)})._read_cap is None
    )  # virtual tiles decode nothing at read


def test_a_corrupt_file_among_good_ones_fails_the_read(tmp_path, monkeypatch):
    _folder(tmp_path, 3)
    (tmp_path / "t0001.tif").write_bytes(b"not a raster")
    monkeypatch.setattr(R, "_FILES_PER_TASK_FROM", 1)
    monkeypatch.setattr(R, "_FILES_PER_TASK_MAX", 3)
    reader = _reader(tmp_path)
    (part,) = reader.partitions()
    with pytest.raises(Exception, match="t0001|not recognized|TIFF"):
        list(reader.read(part))
