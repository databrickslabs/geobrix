"""Unit test: writer bench times a light write and emits a ResultRow."""

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.bench import readers


def _write_sample(path):
    data = np.arange(12, dtype="float32").reshape(3, 4)
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_run_format_write_light(spark, tmp_path):
    src_dir = tmp_path / "in"
    src_dir.mkdir()
    for i in range(2):
        _write_sample(str(src_dir / f"s{i}.tif"))
    out = str(tmp_path / "out")
    r = readers.run_format_write(
        spark,
        str(src_dir),
        out,
        "t",
        warmup=1,
        measured=2,
        write_api="lightweight",
        read_fmt="raster_gbx",
        write_fmt="gtiff_gbx",
        options={"filterRegex": r".*\.tif$"},
        where="venv",
    )
    assert r.api == "lightweight"
    assert r.fn == "raster_write"
    assert r.category == "writer"
    assert r.mode == "spark-path"
    assert r.rows == 2
    assert r.status == "ok"
    assert r.iter_median_s >= 0.0
    # Default (empty) label leaves the note as the canonical "<fmt> write of N tiles".
    assert r.note == "gtiff_gbx write of 2 tiles"


def test_run_format_write_label_appends_to_note(spark, tmp_path):
    """A non-empty label is appended to the note so parts vs single rows are distinct."""
    src_dir = tmp_path / "in"
    src_dir.mkdir()
    for i in range(2):
        _write_sample(str(src_dir / f"s{i}.tif"))
    out = str(tmp_path / "out")
    r = readers.run_format_write(
        spark,
        str(src_dir),
        out,
        "t",
        warmup=1,
        measured=2,
        write_api="lightweight",
        read_fmt="raster_gbx",
        write_fmt="gtiff_gbx",
        options={"filterRegex": r".*\.tif$"},
        label="singleFile",
        where="venv",
    )
    assert r.status == "ok"
    assert r.note == "gtiff_gbx write of 2 tiles [singleFile]"
