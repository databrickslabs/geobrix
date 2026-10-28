"""Engines: manifest arrays through zarr-python, plain stores through tensorstore or zarr."""

import subprocess
import sys
import threading
import time

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")

from databricks.labs.gbx.zarr import _engine  # noqa: E402
from databricks.labs.gbx.zarr._access import FuseRangeReader  # noqa: E402
from databricks.labs.gbx.zarr._engine import (  # noqa: E402
    open_manifest_array,
    open_plain_array,
    read_box,
    read_points,
)
from databricks.labs.gbx.zarr._store import ManifestStore  # noqa: E402
from databricks.labs.gbx.zarr._testing import plain_store_rows  # noqa: E402
from databricks.labs.gbx.zarr.errors import ZarrError, ZarrUnsupported  # noqa: E402

PIPELINE_PATH = "codec_pipeline.path"
# t2m is (48, 73, 144): a box that crosses the last chunk and shard edges of every axis
EDGE_BOX = (slice(0, 5), slice(60, 73), slice(140, 144))
FIXTURES = ["era5_v3_sharded", "era5_v3_unsharded", "era5_v2"]


def _manifest(path):
    metas, rows, arrays = plain_store_rows(path)
    return ManifestStore(metas, rows, FuseRangeReader(str(path)), arrays)


def _direct(path, name="t2m"):
    return zarr.open_array(str(path / name), mode="r")[:]


def _pipeline(arr):
    return type(arr._async_array.codec_pipeline).__name__


# --- manifest arrays ------------------------------------------------------------


@pytest.mark.parametrize("pipeline", ["default", "native", "auto"])
@pytest.mark.parametrize("fixture", ["era5_v3_sharded", "era5_v2"])
def test_manifest_array_matches_zarr_direct(fixture, pipeline, request):
    path = request.getfixturevalue(fixture)
    store = _manifest(path)
    for name in ("t2m", "u10"):
        arr = open_manifest_array(store, name, pipeline=pipeline)
        if pipeline == "default":
            assert isinstance(arr, zarr.Array) and arr.read_only
        want = _direct(path, name)
        assert arr.shape == want.shape and np.dtype(arr.dtype) == want.dtype
        np.testing.assert_array_equal(read_box(arr, (slice(None),) * 3), want)


def test_the_auto_pipeline_is_the_native_engine(era5_v3_sharded):
    """G1: zarr-python over a ManifestStore opens a file per chunk and was many times
    slower than tensorstore, so ``auto`` is the native engine."""
    store = _manifest(era5_v3_sharded)
    assert open_manifest_array(store, "t2m").pipeline == "native"
    assert open_manifest_array(store, "t2m", pipeline="native").pipeline == "native"
    default = open_manifest_array(store, "t2m", pipeline="default")
    assert _pipeline(default) == "BatchedCodecPipeline"


def test_the_zarrs_pipeline_is_refused_for_a_manifest_store(era5_v3_sharded):
    """zarrs-python supports local, object and HTTP stores only; zarr would quietly
    fall back to its own pipeline, so an explicit request is an error instead."""
    store = _manifest(era5_v3_sharded)
    before = zarr.config.get(PIPELINE_PATH)
    with pytest.raises(ZarrUnsupported) as ei:
        open_manifest_array(store, "t2m", pipeline="zarrs")
    assert "ManifestStore" in str(ei.value) and "Fix:" in str(ei.value)
    assert "pipeline='default'" in ei.value.fix
    assert zarr.config.get(PIPELINE_PATH) == before


def test_zarrs_config_is_scoped(era5_v3_sharded, era5_v2):
    """Neither opening nor reading leaves zarrs selected for the rest of the process."""
    before = zarr.config.get(PIPELINE_PATH)
    assert before != "zarrs.ZarrsCodecPipeline"
    with pytest.raises(ZarrUnsupported):
        open_manifest_array(_manifest(era5_v3_sharded), "t2m", pipeline="zarrs")
    assert zarr.config.get(PIPELINE_PATH) == before
    plain = open_plain_array(str(era5_v3_sharded), "t2m", engine="zarr")
    assert plain.pipeline == "zarrs"
    read_box(plain, EDGE_BOX)
    assert zarr.config.get(PIPELINE_PATH) == before
    # arrays opened elsewhere in the process still get zarr's default
    assert _pipeline(zarr.open_array(str(era5_v2 / "t2m"), mode="r")) == (
        "BatchedCodecPipeline"
    )


def test_concurrent_opens_never_leave_zarrs_selected(era5_v3_sharded, era5_v2):
    """The config scope is process-wide while it lasts; opens must not interleave."""
    before = zarr.config.get(PIPELINE_PATH)
    errors = []

    def open_many(path):
        try:
            for _ in range(8):
                open_plain_array(str(path), "t2m", engine="zarr")
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [
        threading.Thread(target=open_many, args=(p,))
        for p in (era5_v3_sharded, era5_v2) * 4
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert zarr.config.get(PIPELINE_PATH) == before


def test_a_default_open_never_runs_inside_a_zarrs_scope(era5_v3_sharded):
    """The config scope is process-wide while it lasts, so every GeoBrix open takes the
    same lock: a default open that starts during a zarrs scope waits for it."""
    path = str(era5_v3_sharded)
    entered, release = threading.Event(), threading.Event()

    def hold_a_zarrs_scope():
        with _engine._zarrs_scope():
            entered.set()
            release.wait(10)

    holder = threading.Thread(target=hold_a_zarrs_scope)
    holder.start()
    assert entered.wait(10)
    got = {}

    def default_open():
        got["plain"] = open_plain_array(
            path, "t2m", engine="zarr", pipeline="default"
        ).pipeline
        got["manifest"] = _pipeline(
            open_manifest_array(_manifest(era5_v3_sharded), "t2m", pipeline="default")
        )

    opener = threading.Thread(target=default_open)
    opener.start()
    time.sleep(0.3)  # long enough for an unlocked open to finish inside the scope
    release.set()
    holder.join()
    opener.join()
    assert got == {"plain": "default", "manifest": "BatchedCodecPipeline"}


def test_mixed_default_and_zarrs_opens_each_get_their_own_pipeline(
    era5_v3_sharded, era5_v2
):
    before = zarr.config.get(PIPELINE_PATH)
    wrong, errors = [], []

    def opens(path, pipeline):
        try:
            for _ in range(10):
                arr = open_plain_array(
                    str(path), "t2m", engine="zarr", pipeline=pipeline
                )
                if arr.pipeline != pipeline:
                    wrong.append((pipeline, arr.pipeline))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [
        threading.Thread(target=opens, args=(p, pl))
        for p in (era5_v3_sharded, era5_v2)
        for pl in ("zarrs", "default", "zarrs", "default")
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors and not wrong
    assert zarr.config.get(PIPELINE_PATH) == before


def _slow_opens(monkeypatch, seconds, record):
    """zarr.open_array that takes a while, recording how many run at once."""
    real = zarr.open_array
    lock = threading.Lock()
    state = {"now": 0, "peak": 0}

    def slow(*args, **kwargs):
        with lock:
            state["now"] += 1
            state["peak"] = max(state["peak"], state["now"])
        try:
            time.sleep(seconds)
            return real(*args, **kwargs)
        finally:
            with lock:
                state["now"] -= 1

    monkeypatch.setattr(zarr, "open_array", slow)
    record.append(state)


def test_default_opens_run_at_the_same_time(era5_v3_sharded, monkeypatch):
    """Only a zarrs scope needs the process to itself; plain opens share it."""
    state = []
    _slow_opens(monkeypatch, 0.2, state)
    path = str(era5_v3_sharded)
    pipelines = []
    threads = [
        threading.Thread(
            target=lambda: pipelines.append(
                open_plain_array(
                    path, "t2m", engine="zarr", pipeline="default"
                ).pipeline
            )
        )
        for _ in range(8)
    ]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    took = time.perf_counter() - start
    assert pipelines == ["default"] * 8
    assert state[0]["peak"] == 8
    assert took < 8 * 0.2 / 2  # not one after another


def test_a_zarrs_scope_waits_for_default_opens_in_flight(era5_v3_sharded, monkeypatch):
    path = str(era5_v3_sharded)
    inside, release = threading.Event(), threading.Event()
    real = zarr.open_array
    first = []

    def blocking(*args, **kwargs):
        if not first:  # the default open parks inside zarr.open_array
            first.append(1)
            inside.set()
            release.wait(10)
        return real(*args, **kwargs)

    monkeypatch.setattr(zarr, "open_array", blocking)
    default = threading.Thread(
        target=lambda: open_plain_array(path, "t2m", engine="zarr", pipeline="default")
    )
    default.start()
    assert inside.wait(10)
    done = threading.Event()

    def zarrs_open():
        open_plain_array(path, "t2m", engine="zarr", pipeline="zarrs")
        done.set()

    zarrs = threading.Thread(target=zarrs_open)
    zarrs.start()
    assert not done.wait(0.4)  # a zarrs scope cannot start while a default open runs
    release.set()
    default.join()
    zarrs.join()
    assert done.is_set()


def test_every_open_holds_the_config_gate(era5_v3_sharded, monkeypatch):
    held = []
    real = zarr.open_array

    def checking(*args, **kwargs):
        held.append(_engine._CONFIG_GATE.held())
        return real(*args, **kwargs)

    monkeypatch.setattr(zarr, "open_array", checking)
    path = str(era5_v3_sharded)
    open_plain_array(path, "t2m", engine="zarr", pipeline="default")
    open_plain_array(path, "t2m", engine="zarr", pipeline="zarrs")
    store = _manifest(era5_v3_sharded)
    open_manifest_array(store, "t2m", pipeline="default")
    assert held == [True, True, True]


def _in_thread(fn, seconds=3.0):
    """Run ``fn`` in a daemon thread; ``(finished, result_or_error)``. A deadlock is a ``False``."""
    box = {}

    def run():
        try:
            box["value"] = fn()
        except BaseException as e:  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(seconds)
    return (not t.is_alive()), box


def test_a_nested_shared_hold_does_not_wait_for_a_writer():
    """A thread that already holds the gate shared may take it shared again while a writer
    waits; a fresh shared request queues behind the writer."""
    gate = _engine._ConfigGate()
    order = []

    def writer():
        with gate.exclusive():
            order.append("writer")

    def reader():
        with gate.shared():
            w = threading.Thread(target=writer, daemon=True)
            w.start()
            deadline = time.time() + 5
            while not gate._waiting and time.time() < deadline:
                time.sleep(0.01)
            assert gate._waiting == 1, "the writer never queued"
            with gate.shared():  # nested: it must not queue behind the writer
                order.append("nested")
            assert gate.held()  # still inside the outer hold
        w.join(5)
        return list(order)

    finished, box = _in_thread(reader, 10)
    assert finished, "a nested shared hold deadlocked behind a waiting writer"
    assert box.get("value") == ["nested", "writer"], box


def test_a_thread_holding_the_gate_shared_cannot_upgrade_it():
    gate = _engine._ConfigGate()

    def upgrade():
        with gate.shared():
            try:
                with gate.exclusive():
                    return "upgraded"
            except ZarrError as e:
                return e

    finished, box = _in_thread(upgrade)
    assert finished, "a shared -> exclusive upgrade deadlocked"
    err = box["value"]
    assert isinstance(err, ZarrError) and "Fix:" in str(err)
    assert "exclusive" in str(err) and "shared" in str(err)
    with gate.exclusive():  # the failed upgrade left the gate usable
        assert gate.held()


def test_the_gate_is_free_after_nested_holds():
    gate = _engine._ConfigGate()
    with gate.shared():
        with gate.shared():
            pass
        assert gate.held()  # the outer hold survives the inner exit
    assert not gate.held()
    finished, _ = _in_thread(lambda: gate.exclusive().__enter__())
    assert finished


def test_a_zarrs_scope_may_open_inside_itself_and_shared_inside_it():
    gate = _engine._ConfigGate()
    with gate.exclusive():
        with gate.exclusive():
            with gate.shared():
                assert gate.held()
    assert not gate.held()


def test_a_manifest_array_cannot_be_written(era5_v3_sharded):
    arr = open_manifest_array(_manifest(era5_v3_sharded), "t2m", pipeline="default")
    with pytest.raises(Exception) as ei:
        arr[0, 0, 0] = 1.0
    assert "read" in str(ei.value).lower()


# --- plain stores ---------------------------------------------------------------


@pytest.mark.parametrize("fixture", FIXTURES)
def test_plain_tensorstore_matches_zarr(fixture, request):
    pytest.importorskip("tensorstore")
    path = request.getfixturevalue(fixture)
    for name in ("t2m", "u10"):
        ts_arr = open_plain_array(str(path), name, engine="tensorstore")
        zarr_arr = open_plain_array(str(path), name, engine="zarr")
        want = _direct(path, name)
        assert ts_arr.engine == "tensorstore" and zarr_arr.engine == "zarr"
        for arr in (ts_arr, zarr_arr):
            assert arr.shape == want.shape and np.dtype(arr.dtype) == want.dtype
            np.testing.assert_array_equal(arr.read((slice(None),) * 3), want)


@pytest.mark.parametrize("fixture", FIXTURES)
def test_plain_zarr_engine_matches_zarr(fixture, request):
    path = request.getfixturevalue(fixture)
    arr = open_plain_array(str(path), "t2m", engine="zarr")
    np.testing.assert_array_equal(read_box(arr, EDGE_BOX), _direct(path)[EDGE_BOX])


def test_plain_zarr_engine_uses_zarrs_when_it_can(era5_v3_sharded):
    arr = open_plain_array(str(era5_v3_sharded), "t2m", engine="zarr")
    assert arr.pipeline == "zarrs"


def test_plain_zarr_engine_can_decode_with_zarr_default(era5_v3_sharded):
    """The zarrs pipeline is a choice, not a given: G1 found it slower than zarr's own
    on box reads over a Volume, so a caller can measure and pick."""
    path = era5_v3_sharded
    default = open_plain_array(str(path), "t2m", engine="zarr", pipeline="default")
    zarrs = open_plain_array(str(path), "t2m", engine="zarr", pipeline="zarrs")
    assert (default.engine, default.pipeline) == ("zarr", "default")
    assert (zarrs.engine, zarrs.pipeline) == ("zarr", "zarrs")
    want = _direct(path)[EDGE_BOX]
    for arr in (default, zarrs):
        np.testing.assert_array_equal(read_box(arr, EDGE_BOX), want)
    # a pipeline names a zarr option, so it makes the automatic engine zarr
    assert open_plain_array(str(path), "t2m", pipeline="default").engine == "zarr"
    with pytest.raises(ValueError, match="pipeline"):
        open_plain_array(str(path), "t2m", engine="tensorstore", pipeline="default")
    with pytest.raises(ValueError, match="pipeline"):
        open_plain_array(str(path), "t2m", engine="zarr", pipeline="rust")


def test_the_zarrs_pipeline_needs_the_package(era5_v3_sharded, monkeypatch):
    monkeypatch.setitem(sys.modules, "zarrs", None)
    path = str(era5_v3_sharded)
    assert open_plain_array(path, "t2m", engine="zarr").pipeline == "default"
    with pytest.raises(ZarrUnsupported) as ei:
        open_plain_array(path, "t2m", engine="zarr", pipeline="zarrs")
    assert "zarrs" in str(ei.value) and "pipeline='default'" in ei.value.fix


def test_auto_engine_is_tensorstore(era5_v3_sharded):
    pytest.importorskip("tensorstore")
    assert open_plain_array(str(era5_v3_sharded), "t2m").engine == "tensorstore"


def test_auto_engine_falls_back_to_zarr_without_tensorstore(
    era5_v3_sharded, monkeypatch
):
    monkeypatch.setitem(sys.modules, "tensorstore", None)  # import raises ImportError
    arr = open_plain_array(str(era5_v3_sharded), "t2m")
    assert arr.engine == "zarr"
    np.testing.assert_array_equal(
        read_box(arr, EDGE_BOX), _direct(era5_v3_sharded)[EDGE_BOX]
    )


def test_explicit_tensorstore_without_the_package_says_how_to_install_it(
    era5_v3_sharded, monkeypatch
):
    monkeypatch.setitem(sys.modules, "tensorstore", None)
    with pytest.raises(ZarrUnsupported) as ei:
        open_plain_array(str(era5_v3_sharded), "t2m", engine="tensorstore")
    assert "tensorstore" in str(ei.value) and "zarr_dbr19" in ei.value.fix


# --- read_box -------------------------------------------------------------------


@pytest.mark.parametrize("fixture", FIXTURES)
def test_read_box_partial_edge(fixture, request):
    """The same box, through every engine, equals numpy slicing of the direct read."""
    path = request.getfixturevalue(fixture)
    want = _direct(path)[EDGE_BOX]
    assert want.shape == (5, 13, 4)
    arrays = [
        open_manifest_array(_manifest(path), "t2m", pipeline="native"),
        open_manifest_array(_manifest(path), "t2m", pipeline="default"),
        open_plain_array(str(path), "t2m", engine="zarr"),
    ]
    try:
        import tensorstore  # noqa: F401
    except ImportError:  # only the tensorstore leg needs the package
        pass
    else:
        arrays.append(open_plain_array(str(path), "t2m", engine="tensorstore"))
    for arr in arrays:
        got = read_box(arr, EDGE_BOX)
        assert isinstance(got, np.ndarray) and got.shape == want.shape
        np.testing.assert_array_equal(got, want)


@pytest.mark.parametrize("kind", ["manifest", "native", "zarr", "tensorstore"])
def test_read_box_clamps_like_numpy_and_pads_missing_axes(era5_v3_sharded, kind):
    if kind == "tensorstore":
        pytest.importorskip("tensorstore")
    if kind in ("manifest", "native"):
        pipeline = "default" if kind == "manifest" else "native"
        arr = open_manifest_array(_manifest(era5_v3_sharded), "t2m", pipeline=pipeline)
    else:
        arr = open_plain_array(str(era5_v3_sharded), "t2m", engine=kind)
    want = _direct(era5_v3_sharded)
    np.testing.assert_array_equal(read_box(arr, (slice(40, 100),)), want[40:100])
    np.testing.assert_array_equal(read_box(arr, ()), want)
    np.testing.assert_array_equal(
        read_box(arr, (slice(None), slice(-3, None), slice(None, 2))),
        want[:, -3:, :2],
    )
    assert read_box(arr, (slice(10, 10),)).shape == (0, 73, 144)


@pytest.mark.parametrize("fixture", ["era5_v3_sharded", "era5_v2"])
@pytest.mark.parametrize("kind", ["manifest", "native", "zarr", "tensorstore"])
def test_read_points_matches_numpy_and_reads_only_the_chunks_with_cells(
    fixture, kind, request
):
    if kind == "tensorstore":
        pytest.importorskip("tensorstore")
    path = request.getfixturevalue(fixture)
    store = _manifest(path)
    if kind in ("manifest", "native"):
        pipeline = "default" if kind == "manifest" else "native"
        arr = open_manifest_array(store, "t2m", pipeline=pipeline)
    else:
        arr = open_plain_array(str(path), "t2m", engine=kind)
    want = _direct(path)
    rng = np.random.default_rng(3)
    cells = [rng.integers(0, n, 300) for n in want.shape]
    cells = [np.append(c, c[:5]) for c in cells]  # cells may repeat
    got = read_points(arr, cells)
    assert got.shape == (305,) and got.dtype == want.dtype
    np.testing.assert_array_equal(got, want[tuple(cells)])
    assert read_points(arr, [np.array([], dtype=int)] * 3).shape == (0,)
    with pytest.raises(ValueError, match="out of range on axis 1"):
        read_points(arr, [np.array([0]), np.array([73]), np.array([0])])
    with pytest.raises(ValueError, match="2 coordinate arrays for an array of 3 axes"):
        read_points(arr, [np.array([0]), np.array([0])])

    if (
        kind == "native"
    ):  # chunks (12, 37, 72): two cells in one chunk, one in a far one
        asked = []
        real = store.chunk_regions
        store.chunk_regions = lambda name, coords: asked.append(list(coords)) or real(
            name, coords
        )
        cells = [np.array([0, 11, 47]), np.array([0, 36, 72]), np.array([0, 71, 143])]
        np.testing.assert_array_equal(read_points(arr, cells), want[tuple(cells)])
        assert asked == [
            [(0, 0, 0), (3, 1, 1)]
        ]  # not the 16 chunks of their bounding box


@pytest.mark.parametrize("kind", ["manifest", "native", "zarr"])
def test_read_box_rejects_a_step_and_too_many_axes(era5_v2, kind):
    if kind in ("manifest", "native"):
        pipeline = "default" if kind == "manifest" else "native"
        arr = open_manifest_array(_manifest(era5_v2), "t2m", pipeline=pipeline)
    else:
        arr = open_plain_array(str(era5_v2), "t2m", engine=kind)
    with pytest.raises(ValueError, match="step"):
        read_box(arr, (slice(0, 8, 2),))
    with pytest.raises(ValueError, match="axes"):
        read_box(arr, (slice(None),) * 4)
    with pytest.raises(ValueError, match="slice"):
        read_box(arr, (3,))


# --- arguments and errors -------------------------------------------------------


def test_unknown_engine_or_pipeline_is_a_value_error(era5_v2):
    with pytest.raises(ValueError, match="engine"):
        open_plain_array(str(era5_v2), "t2m", engine="gdal")
    with pytest.raises(ValueError, match="pipeline"):
        open_manifest_array(_manifest(era5_v2), "t2m", pipeline="rust")


def test_a_missing_array_is_a_zarr_error_with_a_fix(era5_v2):
    for pipeline in ("auto", "native", "default"):
        with pytest.raises(ZarrError) as ei:
            open_manifest_array(_manifest(era5_v2), "no_such_array", pipeline=pipeline)
        assert "no_such_array" in str(ei.value) and "Fix:" in str(ei.value)
    for engine in ("zarr", "tensorstore"):
        if engine == "tensorstore":
            pytest.importorskip("tensorstore")
        with pytest.raises(ZarrError) as ei:
            open_plain_array(str(era5_v2), "no_such_array", engine=engine)
        assert "no_such_array" in str(ei.value) and "Fix:" in str(ei.value)


def test_importing_the_engines_does_not_load_tensorstore():
    code = (
        "import sys\n"
        "import databricks.labs.gbx.zarr._engine\n"
        "assert 'tensorstore' not in sys.modules, 'tensorstore loaded at import'\n"
    )
    done = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
