"""The engine's light-level options that need no GDAL: FILE references reach the virtual-tile read, the force-output
UDF shapes the engine's result, and the `*_grouped` core runs the engine on an open tile's bytes.

The engine run itself is replaced by a stub (the real runs are the SPEC_SE rows and the parity tests, which need the
payload's osgeo), so every row here runs on the host.
"""

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import grouped, l3, run

MAN = manifest.load()
SLOPE = MAN.functions["gbx_rst_slope"]
MALG = MAN.functions["gbx_rst_mapalgebra"]


def _virtual(n):
    return {"cellid": n, "raster": None, "path": f"/tmp/gbx_v{n}.tif", "metadata": {}}


def _recorder(monkeypatch):
    seen = []
    monkeypatch.setattr(
        l3,
        "_virtual_raster",
        lambda fn, t, cap, ref=None, decimate=None, sizes_out=None, stack=None: seen.append(
            (t["cellid"], ref)
        )
        or b"II*\x00",
    )
    return seen


def test_one_file_reference_serves_a_single_tile_input(monkeypatch):
    seen = _recorder(monkeypatch)
    rasters, _, cellid = l3._inputs(SLOPE, _virtual(7), None, "REF")
    assert rasters == [b"II*\x00"] and cellid == 7 and seen == [(7, "REF")]


def test_an_array_input_takes_one_file_reference_per_tile(monkeypatch):
    seen = _recorder(monkeypatch)
    l3._inputs(MALG, [_virtual(1), _virtual(2)], None, ["R1", "R2"])
    assert seen == [(1, "R1"), (2, "R2")]


def test_no_file_reference_reads_the_tiles_path_as_before(monkeypatch):
    seen = _recorder(monkeypatch)
    l3._inputs(MALG, [_virtual(1), _virtual(2)])
    assert seen == [(1, None), (2, None)]


def _stub_run(monkeypatch, result):
    calls = []
    monkeypatch.setattr(l3, "_setup", lambda grid_dirs: None)
    monkeypatch.setattr(
        run,
        "run",
        lambda fn, rasters, values, section=None, src_sizes=None: calls.append(
            (fn, rasters, values)
        )
        or result,
    )
    return calls


def test_the_file_reference_form_takes_the_reference_second_and_then_the_row_parameters(
    monkeypatch,
):
    seen = _recorder(monkeypatch)
    calls = _stub_run(monkeypatch, run.Result(b"II*\x00out", None, "cmd"))
    row = l3.c0("gbx_rst_slope", file_ref=True).func(
        _virtual(3), "REF", "percent", None, None
    )
    assert seen == [(3, "REF")]
    assert calls[0][2] == {"unit": "percent", "xscale": None, "yscale": None}
    assert row["raster"] == b"II*\x00out" and row["cellid"] == 3


def test_the_plain_form_still_takes_the_row_parameters_second(monkeypatch):
    seen = _recorder(monkeypatch)
    calls = _stub_run(monkeypatch, run.Result(b"II*\x00out", None, "cmd"))
    l3.c0("gbx_rst_slope").func(_virtual(3), "percent")
    assert seen == [(3, None)]
    assert calls[0][2] == {"unit": "percent", "xscale": None, "yscale": None}


def _tile(raster=b"II*\x00in"):
    return {"cellid": 5, "raster": raster, "path": None, "metadata": {}}


@pytest.mark.parametrize("mat", [True, None], ids=["materialize", "natural"])
def test_the_force_output_form_shapes_the_engines_bytes(monkeypatch, mat):
    calls = _stub_run(monkeypatch, run.Result(b"II*\x00out", None, "cmd"))
    row = l3.forced_c0("gbx_rst_slope").func(
        _tile(), "degrees", None, None, None, None, mat
    )
    assert row["raster"] == b"II*\x00out" and row["path"] is None and row["cellid"] == 5
    assert calls[0][1] == [b"II*\x00in"]
    assert calls[0][2] == {"unit": "degrees", "xscale": None, "yscale": None}


def _gtiff():
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=4,
            height=4,
            count=1,
            dtype="float32",
            crs="EPSG:3857",
            transform=from_origin(0, 4, 1, 1),
        ) as ds:
            ds.write(np.ones((1, 4, 4), dtype="float32"))
        return mf.read()


def test_the_force_output_form_writes_a_virtual_tile_for_virtualize_dir(
    monkeypatch, tmp_path
):
    out = _gtiff()
    _stub_run(monkeypatch, run.Result(out, None, "cmd"))
    row = l3.forced_c0("gbx_rst_slope").func(
        _tile(), "degrees", None, None, str(tmp_path), "run1", None
    )
    assert row["raster"] is None and row["path"].startswith(str(tmp_path))
    assert row["path"].rsplit("/", 1)[1].startswith("run1_")
    with open(row["path"], "rb") as fh:
        assert fh.read() == out


def test_the_force_output_form_gives_the_engines_error_as_an_error_tile(monkeypatch):
    _stub_run(monkeypatch, run.Result(None, "slope: boom. Fix: x", None))
    row = l3.forced_c0("gbx_rst_slope").func(
        _tile(), "degrees", None, None, None, None, True
    )
    assert row["raster"] is None and "boom" in row["metadata"]["last_error"]


def test_the_force_output_form_returns_a_null_and_an_error_tile_as_they_are(
    monkeypatch,
):
    calls = _stub_run(monkeypatch, run.Result(b"never", None, None))
    forced = l3.forced_c0("gbx_rst_slope").func
    assert forced(None, "degrees", None, None, None, None, True) is None
    err = {
        "cellid": 1,
        "raster": None,
        "path": None,
        "metadata": {"error_message": "up"},
    }
    assert forced(err, "degrees", None, None, None, None, True) == err
    assert calls == []


def test_the_force_output_form_fills_the_manifest_defaults_for_missing_parameters(
    monkeypatch,
):
    calls = _stub_run(monkeypatch, run.Result(b"II*\x00out", None, "cmd"))
    l3.forced_c0("gbx_rst_slope").func(_tile(), None, None, True)
    assert calls[0][2] == {"unit": "degrees", "xscale": None, "yscale": None}


def test_the_force_output_form_gives_a_refused_input_as_an_error_tile(monkeypatch):
    calls = _stub_run(monkeypatch, run.Result(b"never", None, None))

    def refuse(
        fn,
        first,
        read_cap=None,
        file_refs=None,
        sizes_out=None,
        values=None,
        stack=None,
    ):
        raise run.DataError("tile is unreadable. Fix: pass a GeoTIFF tile")

    monkeypatch.setattr(l3, "_inputs", refuse)
    row = l3.forced_c0("gbx_rst_slope").func(
        _tile(), "degrees", None, None, None, None, True
    )
    assert row["raster"] is None and "unreadable" in row["metadata"]["last_error"]
    assert row["cellid"] == 5 and calls == []


def test_the_grouped_core_raises_on_the_driver_when_the_engine_cannot_run(monkeypatch):
    monkeypatch.setattr(run, "unavailable_reason", lambda: "no osgeo here")
    with pytest.raises(RuntimeError, match=r"no osgeo here.*Fix:"):
        grouped.core("gbx_rst_slope", {"unit": "degrees"})


def test_the_grouped_core_runs_the_engine_on_the_open_tiles_bytes(monkeypatch):
    from databricks.labs.gbx.pyrx import functions as prx

    monkeypatch.setattr(run, "unavailable_reason", lambda: None)
    calls = _stub_run(monkeypatch, run.Result(b"II*\x00out", None, "cmd"))
    monkeypatch.setattr(prx, "_dataset_to_gtiff_bytes", lambda ds: b"II*\x00" + ds)
    core = grouped.core(
        "gbx_rst_slope", {"unit": "percent", "xscale": None, "yscale": None}
    )
    row = core(b"ds", 9)
    assert row["raster"] == b"II*\x00out" and row["cellid"] == 9
    assert calls[0][0] is SLOPE and calls[0][1] == [b"II*\x00ds"]
    assert calls[0][2] == {"unit": "percent", "xscale": None, "yscale": None}


def test_the_python_api_force_output_path_builds_the_engine_udf(spark8):
    """`rst_slope(materialize=True)` builds the row's engine force-output UDF (the manifest's `extra_impl`)."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.rasterx import functions as rx

    assert SLOPE.python.extra_factory_args == "engine"
    col = rx.rst_slope(F.col("tile"), "percent", materialize=True)
    assert "_forced" in str(col)


def test_the_force_output_udf_carries_the_remote_path_switch_the_driver_set(
    monkeypatch,
):
    """A call built while GBX_ALLOW_REMOTE_PATHS is on runs its function with the switch on, as a registration does."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType

    from databricks.labs.gbx import _gdal_policy

    monkeypatch.setattr(_gdal_policy, "_session_read_cap_now", lambda: None)
    probe = F.udf(lambda x: _gdal_policy.remote_paths_allowed(), BooleanType())
    monkeypatch.setenv("GBX_ALLOW_REMOTE_PATHS", "1")
    built = _gdal_policy.carrying_call(probe)
    monkeypatch.delenv("GBX_ALLOW_REMOTE_PATHS")  # the worker never reads it itself
    assert built.func(None) is True
    assert probe.func(None) is False


@pytest.mark.parametrize("sql_name", ["gbx_rst_slope"])
def test_the_python_api_force_output_runs_the_engine(spark8, tmp_path, sql_name):
    """`rst_slope(materialize=True)` and `virtualize_dir=` run the engine and shape its tile (needs the payload)."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.rasterx import functions as rx

    if run.unavailable_reason():
        pytest.skip(run.unavailable_reason())
    tile = oracles._build("dem_3857", str(tmp_path))
    df = spark8.createDataFrame([(tile,)], f"tile {TILE_SQL}")
    (r,) = df.select(
        rx.rst_slope(F.col("tile"), "percent", materialize=True).alias("s")
    ).collect()
    assert r["s"]["raster"] is not None and r["s"]["path"] is None
    (r,) = df.select(
        rx.rst_slope(F.col("tile"), virtualize_dir=str(tmp_path / "v")).alias("s")
    ).collect()
    assert r["s"]["raster"] is None and r["s"]["path"].startswith(str(tmp_path / "v"))


# --- a virtual tile whose file is missing: retried as transient, then raised, never NULL ---------------------------


def _read_script(monkeypatch, failures, error):
    """`_read_virtual` fails `failures` times with `error`, then returns bytes; the retry's sleeps are recorded."""
    from databricks.labs.gbx.core import _retry

    calls, sleeps = [], []
    monkeypatch.setattr(run, "virtual_source", lambda sql_name, path: None)
    monkeypatch.setattr(_retry.time, "sleep", sleeps.append)

    def read(fn, vt, cap, ref=None, decimate=None, sizes_out=None):
        calls.append(vt.path)
        if len(calls) <= failures:
            raise error
        return b"II*\x00"

    monkeypatch.setattr(l3, "_read_virtual", read)
    return calls, sleeps


def test_a_file_that_appears_after_two_failed_attempts_is_read(monkeypatch):
    calls, sleeps = _read_script(
        monkeypatch, 2, FileNotFoundError("/tmp/gbx_v1.tif: No such file or directory")
    )
    assert l3._virtual_raster(SLOPE, _virtual(1), None) == b"II*\x00"
    assert len(calls) == 3 and len(sleeps) == 2


def test_a_file_still_missing_raises_with_the_path_and_a_fix_after_ten_attempts(
    monkeypatch,
):
    calls, sleeps = _read_script(
        monkeypatch, 99, FileNotFoundError("/tmp/gbx_v1.tif: No such file or directory")
    )
    with pytest.raises(run.MissingSourceError) as raised:
        l3._virtual_raster(SLOPE, _virtual(1), None)
    assert "/tmp/gbx_v1.tif" in str(raised.value) and "Fix:" in str(raised.value)
    assert isinstance(raised.value, FileNotFoundError)
    assert len(calls) == 10 and len(sleeps) == 9


def test_gdals_own_missing_file_message_is_retried_the_same(monkeypatch):
    err = OSError("Read failed. See previous exception")
    err.__cause__ = RuntimeError("/tmp/gbx_v1.tif: No such file or directory")
    calls, _ = _read_script(monkeypatch, 99, err)
    with pytest.raises(run.MissingSourceError):
        l3._virtual_raster(SLOPE, _virtual(1), None)
    assert len(calls) == 10


def test_unreadable_bytes_stay_an_error_tile_and_are_not_retried(monkeypatch):
    calls, sleeps = _read_script(
        monkeypatch, 99, OSError("x.tif: TIFFReadDirectory: corrupt directory")
    )
    with pytest.raises(run.DataError, match="could not be read"):
        l3._virtual_raster(SLOPE, _virtual(1), None)
    assert len(calls) == 1 and not sleeps


def test_a_missing_file_is_not_swallowed_into_a_null_by_the_udf_input_path(
    monkeypatch,
):
    _read_script(monkeypatch, 99, FileNotFoundError("gone"))
    with pytest.raises(run.MissingSourceError):
        l3._inputs(SLOPE, _virtual(1))
