"""Unit test: reader bench pure-local path produces a ResultRow with timing."""

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


def test_pure_local_reader_bench_emits_result(tmp_path):
    f = tmp_path / "s.tif"
    _write_sample(str(f))
    rows = readers.run_pure_local_reader(
        files=[str(f)],
        run_id="t",
        warmup=1,
        measured=3,
        size_mib=16,
    )
    assert len(rows) == 1
    r = rows[0]
    assert r.api == "lightweight"
    assert r.fn == "raster_read"
    assert r.mode == "pure-core"
    assert r.iter_median_s >= 0.0
    assert r.status == "ok"


# ---------------------------------------------------------------------------
# sizeInMB passthrough: run_format_read must set the sizeInMB reader option for
# the heavy raster formats (netcdf_gdal / gdal / gtiff_gdal), not only for the
# light raster_gbx reader. The netcdf raster leg passes size_mib=-1 (one tile
# per grid var) so the heavy reader matches the light reader's granularity.
# ---------------------------------------------------------------------------


class _StubReader:
    """Records every .option(k, v) call and yields a 1-row DataFrame on load."""

    def __init__(self, fmt, sink):
        self._fmt = fmt
        self._sink = sink  # dict shared with the parent stub for assertions

    def option(self, k, v):
        self._sink.setdefault("options", {})[k] = v
        return self

    def load(self, path):
        return _StubDataFrame()


class _StubDataFrame:
    def count(self):
        return 1


class _StubReadNamespace:
    def __init__(self, sink):
        self._sink = sink

    def format(self, fmt):
        self._sink["fmt"] = fmt
        return _StubReader(fmt, self._sink)


class _StubSpark:
    def __init__(self):
        self.sink = {}
        self.read = _StubReadNamespace(self.sink)


def test_run_format_read_passes_sizeinmb_to_netcdf_gdal():
    spark = _StubSpark()
    r = readers.run_format_read(
        spark,
        "/tmp/netcdf",
        run_id="t",
        warmup=1,
        measured=1,
        api="heavyweight",
        fmt="netcdf_gdal",
        options={"filterRegex": r".*\.nc$"},
        size_mib=-1,
        where="venv",
    )
    assert r.status == "ok", f"expected ok, got: {r.note}"
    # The heavy raster reader must have received the sizeInMB knob (=-1 here).
    assert spark.sink["options"].get("sizeInMB") == "-1"
    # Explicit format-passed options are still applied.
    assert spark.sink["options"].get("filterRegex") == r".*\.nc$"


def test_run_format_read_passes_sizeinmb_to_gdal_and_gtiff_gdal():
    for fmt in ("gdal", "gtiff_gdal"):
        spark = _StubSpark()
        readers.run_format_read(
            spark,
            "/tmp/x",
            run_id="t",
            warmup=1,
            measured=1,
            api="heavyweight",
            fmt=fmt,
            size_mib=32,
            where="venv",
        )
        assert spark.sink["options"].get("sizeInMB") == "32", f"{fmt} missing sizeInMB"
