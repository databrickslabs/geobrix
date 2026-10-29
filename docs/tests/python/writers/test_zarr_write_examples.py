"""Executes the Zarr writer doc examples on the sample cube (Docker only).

Every code block on ``docs/docs/writers/zarr.mdx`` is a function of ``zarr_write_examples.py``;
these tests run each one against the real sample cube on a local Delta session and assert what
the page says it does, reading the store back and comparing it with the cube. The manifest of
a Zarr store is a Delta table, so the session is the Delta one from ``zarr_delta.py``.

Each test makes the store it needs, so the file can be run one test at a time. Registered
stores are copies of the cube in a temporary directory: a local session lists a ``/Volumes``
path through Spark, which only Databricks can do.
"""

import hashlib
import json
import os
import re
import sys
import uuid
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import zarr_write_examples as ex  # noqa: E402
from zarr_delta import copy_of_cube, cube, new_table, spark  # noqa: E402,F401

xr = pytest.importorskip("xarray")
zarr = pytest.importorskip("zarr")

pytestmark = pytest.mark.delta_session  # runs after the tests that use the shared session

from databricks.labs.gbx.zarr import (  # noqa: E402
    BLOCKS_SCHEMA,
    ZarrUnsupported,
    ZarrWriteConflict,
    block_to_numpy,
    gbx_zarr_export,
    gbx_zarr_history,
    gbx_zarr_read,
    gbx_zarr_register,
    gbx_zarr_write,
)

DOCS = Path(__file__).resolve().parents[3] / "docs"
PAGE = DOCS / "writers" / "zarr.mdx"
VARS = ["t2m", "msl", "time", "latitude", "longitude"]


# --- helpers ---------------------------------------------------------------------------


def shown(capsys):
    """What an example printed, without the progress lines register and sync print."""
    lines = capsys.readouterr().out.splitlines()
    return "\n".join(ln for ln in lines if not ln.startswith("[register] ")).strip()


def assemble(spark, source, variable="t2m", **kw):
    """``variable`` of ``source`` as one array: its blocks laid back at their positions."""
    rows = gbx_zarr_read(spark, source, variables=[variable], **kw).collect()
    shape = tuple(max(r["stop"][d] for r in rows) for d in range(len(rows[0]["stop"])))
    out = np.full(shape, np.nan, dtype="float32")
    for r in rows:
        out[tuple(slice(a, b) for a, b in zip(r["start"], r["stop"]))] = block_to_numpy(
            r
        )
    return out


def hours(spark, cube, start, stop, names=VARS):
    return gbx_zarr_read(
        spark, cube, variables=names, sel={"time": slice(start, stop)}
    )


def first_day(spark, cube, new_table, tmp_path, label="story", **kw):
    """A managed store holding the first 12 hours of ``t2m`` and ``msl``."""
    table = new_table(label)
    root = str(tmp_path / f"root_{label}")
    version = gbx_zarr_write(
        hours(spark, cube, "2023-01-01T00:00", "2023-01-01T11:00"),
        table,
        mode="create",
        root=root,
        **kw,
    )
    assert version == 1
    return table, root


def tree_digest(path):
    h = hashlib.sha256()
    for f in sorted(p for p in Path(path).rglob("*") if p.is_file()):
        h.update(f.relative_to(path).as_posix().encode())
        h.update(f.read_bytes())
    return h.hexdigest()


def chunk_patch(spark, table, delta=-1.0):
    """Blocks of the first inner chunk of ``t2m`` (12 hours x 91 x 180), moved by ``delta``."""
    rows = gbx_zarr_read(
        spark,
        table,
        variables=["t2m"],
        isel={"time": slice(0, 12), "latitude": slice(0, 91), "longitude": slice(0, 180)},
    ).collect()
    assert len(rows) == 1
    moved = [
        {**r.asDict(), "data": (block_to_numpy(r) + delta).astype("float32").tobytes()}
        for r in rows
    ]
    return spark.createDataFrame(moved, BLOCKS_SCHEMA)


def update_one_chunk(spark, table, delta=-1.0):
    return gbx_zarr_write(chunk_patch(spark, table, delta), table, mode="update")


@pytest.fixture(scope="module")
def truth(cube):
    return xr.open_zarr(cube, consolidated=False)[["t2m", "msl"]].load()


# --- sample data -----------------------------------------------------------------------


def test_setup_paths_match_the_sample_data_layout():
    assert ex.CUBE_SUBPATH in ex.SETUP
    assert ex.SETUP.count("cube = ") == 1


# --- create: layouts, inputs ------------------------------------------------------------


def test_a_layout_sets_the_chunks_the_shards_and_the_codecs(
    spark, cube, truth, new_table, tmp_path, capsys
):
    table = new_table("layout")
    assert ex.create_with_a_layout(spark, cube, table, str(tmp_path / "layout")) == 1
    capsys.readouterr()
    ex.describe_the_layout(spark, table)
    assert shown(capsys) == ex.DESCRIBE_THE_LAYOUT_output.strip()
    np.testing.assert_array_equal(assemble(spark, table), truth["t2m"].values[:12])
    # and it is a Zarr store any tool opens once it is exported
    out = str(tmp_path / "out.zarr")
    gbx_zarr_export(spark, table, out)
    got = zarr.open_group(out, mode="r")["t2m"]
    assert got.chunks == (12, 91, 180)
    np.testing.assert_array_equal(got[:], truth["t2m"].values[:12])


def test_the_default_layout_is_about_4_mib_chunks_in_shards(
    spark, cube, new_table, tmp_path
):
    table, root = first_day(spark, cube, new_table, tmp_path, "default")
    spec = json.loads(
        spark.sql(f"SELECT meta FROM {table} WHERE kind = 'array' AND array = 't2m'")
        .first()["meta"]
    )
    inner = spec["codecs"][0]["configuration"]
    assert inner["chunk_shape"][0] == 12  # the length of the data, clipped from 24
    codecs = [c["name"] for c in inner["codecs"]]
    assert codecs == ["bytes", "zstd", "crc32c"]
    zstd = inner["codecs"][1]["configuration"]
    assert zstd["level"] == 3


def test_a_store_is_created_from_an_xarray_dataset(
    spark, cube, truth, new_table, tmp_path
):
    table = new_table("xr")
    version = ex.create_from_xarray(spark, cube, table, str(tmp_path / "xr"))
    assert version == 1
    np.testing.assert_array_equal(assemble(spark, table), truth["t2m"].values[:12])


def test_blocks_without_coordinates_make_a_store_labelled_by_position(
    spark, cube, new_table, tmp_path, caplog
):
    table = new_table("nolabels")
    blocks = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m"],
        sel={"time": slice("2023-01-01T00:00", "2023-01-01T05:00")},
    )
    with caplog.at_level("WARNING"):
        gbx_zarr_write(blocks, table, mode="create", root=str(tmp_path / "nl"))
    assert "carry no coordinate for" in caplog.text
    arrays = {
        r["array"]
        for r in spark.sql(f"SELECT array FROM {table} WHERE kind = 'array'").collect()
    }
    assert arrays == {"t2m"}


def test_a_create_that_died_before_its_commit_is_adopted_by_a_retry(
    spark, cube, truth, new_table, tmp_path, monkeypatch
):
    """The table and the root's marker survive a crash; the same create run again adopts them."""
    from databricks.labs.gbx.zarr import _register, _write

    table, root = new_table("adopt"), str(tmp_path / "adopt")
    blocks = hours(spark, cube, "2023-01-01T00:00", "2023-01-01T11:00")

    def killed(write_id):
        raise RuntimeError("killed")

    monkeypatch.setattr(_write, "_BEFORE_COMMIT", killed)
    monkeypatch.setattr(_register, "_discard", lambda *a, **k: False)  # a crash cleans up nothing
    with pytest.raises(Exception, match="killed"):
        gbx_zarr_write(blocks, table, mode="create", root=root)
    monkeypatch.undo()
    assert spark.catalog.tableExists(table)
    assert gbx_zarr_write(blocks, table, mode="create", root=root) == 1
    np.testing.assert_array_equal(assemble(spark, table), truth["t2m"].values[:12])


def test_a_computed_input_is_staged_in_a_table_first(
    spark, cube, truth, new_table, tmp_path
):
    table, staged = new_table("staged"), new_table("blocks")
    assert ex.stage_the_blocks_first(
        spark, cube, table, staged, str(tmp_path / "staged")
    ) == 1
    np.testing.assert_array_equal(assemble(spark, table), truth["t2m"].values[:12])


def test_an_opaque_input_is_evaluated_twice_by_a_create_and_once_by_an_update(
    spark, cube, new_table, tmp_path
):
    """A create or an append plans from a first pass over the block positions, then writes;
    an update or overwrite writes in one pass."""
    log = tmp_path / "evaluations.txt"
    names = ["t2m", "time", "latitude", "longitude"]
    rows = hours(spark, cube, "2023-01-01T00:00", "2023-01-01T02:00", names).collect()
    local = spark.createDataFrame([r.asDict() for r in rows], BLOCKS_SCHEMA).repartition(1)

    def counted(batches):
        with open(log, "a") as fh:
            fh.write("evaluated\n")
        yield from batches

    opaque = local.mapInArrow(counted, BLOCKS_SCHEMA)
    table = new_table("twice")
    gbx_zarr_write(opaque, table, mode="create", root=str(tmp_path / "twice"))
    assert log.read_text().count("evaluated") == 2
    log.unlink()
    gbx_zarr_write(opaque, table, mode="update", variables=["t2m"])
    assert log.read_text().count("evaluated") == 1


# --- update, overwrite and new variables -------------------------------------------------


def test_overwrite_replaces_a_variable_and_older_versions_stay_readable(
    spark, cube, truth, new_table, tmp_path
):
    table, _ = first_day(spark, cube, new_table, tmp_path, "over")
    version = ex.overwrite_a_variable(spark, table)
    assert version == 2
    np.testing.assert_allclose(
        assemble(spark, table), truth["t2m"].values[:12] - 0.5, atol=1e-4
    )
    np.testing.assert_array_equal(  # version 1 is still the uncorrected store
        assemble(spark, table, version_as_of=1), truth["t2m"].values[:12]
    )
    np.testing.assert_array_equal(  # the other variable is untouched
        assemble(spark, table, "msl"), truth["msl"].values[:12]
    )


def test_a_new_variable_is_named_in_variables(
    spark, cube, truth, new_table, tmp_path, capsys
):
    table, _ = first_day(spark, cube, new_table, tmp_path, "newvar")
    version = ex.add_a_variable(spark, table)
    assert version == 2
    assert shown(capsys) == ex.ADD_A_VARIABLE_output.strip()
    np.testing.assert_allclose(
        assemble(spark, table, "t2m_c"), truth["t2m"].values[:12] - 273.15, atol=1e-3
    )
    # without variables= a block of a variable the store lacks has nowhere to go
    rows = gbx_zarr_read(spark, table, variables=["t2m"]).collect()
    moved = [{**r.asDict(), "variable": "t2m_k2"} for r in rows]
    with pytest.raises(Exception) as err:
        gbx_zarr_write(
            spark.createDataFrame(moved, BLOCKS_SCHEMA), table, mode="update"
        )
    assert "Fix:" in str(err.value) or "t2m_k2" in str(err.value)


# --- tiles ------------------------------------------------------------------------------


def test_tiles_changed_by_an_rst_function_go_back_with_their_provenance(
    spark, cube, truth, new_table, tmp_path
):
    table, _ = first_day(spark, cube, new_table, tmp_path, "tiles_prov")
    version = ex.write_tiles_with_provenance(spark, table)
    assert version == 2
    want = truth["t2m"].values[:12].copy()
    want[:4] -= 1.0
    np.testing.assert_allclose(assemble(spark, table), want, atol=1e-4)


def test_tiles_changed_by_an_rst_function_go_back_with_dim_cols(
    spark, cube, truth, new_table, tmp_path
):
    table, _ = first_day(spark, cube, new_table, tmp_path, "tiles_dim")
    version = ex.write_tiles_with_dim_cols(spark, table)
    assert version == 2
    want = truth["t2m"].values[:12].copy()
    want[:4] -= 1.0
    np.testing.assert_allclose(assemble(spark, table), want, atol=1e-4)


def test_a_store_is_created_from_tiles_on_their_grid(
    spark, cube, truth, new_table, tmp_path
):
    table = new_table("tiles_new")
    assert ex.create_from_tiles(spark, cube, table, str(tmp_path / "tiles_new")) == 1
    got = gbx_zarr_read(spark, table, variables=["t2m"], output="table")
    assert got.count() == 3 * 181 * 360
    ds = xr.open_zarr(
        gbx_zarr_export(spark, table, str(tmp_path / "tiles_new.zarr")),
        consolidated=False,
    )
    np.testing.assert_allclose(
        ds["t2m"].values, truth["t2m"].values[:3] + 1.0, atol=1e-4
    )
    assert ds["time"].values[0] == np.datetime64("2023-01-01T00:00")


# --- append and fold ---------------------------------------------------------------------


def test_optimize_true_folds_what_the_write_touched_after_it_commits(
    spark, cube, truth, new_table, tmp_path, capsys
):
    table, _ = first_day(spark, cube, new_table, tmp_path, "fold")
    version = ex.append_and_fold(spark, cube, table)
    assert version == 2  # the write's own version; the fold is the next one
    assert shown(capsys) == ex.APPEND_AND_FOLD_output.strip()
    np.testing.assert_array_equal(assemble(spark, table), truth["t2m"].values[:24])


def test_the_retry_bound_the_page_quotes_is_the_one_in_the_package():
    from databricks.labs.gbx.zarr import _write

    text = PAGE.read_text()
    assert _write._ATTEMPTS == 20 and "20 attempts" in text
    assert _write._MAX_WAIT_SECONDS == 16 and "16 seconds" in text


# --- registered stores: tombstones and publish -------------------------------------------


def test_a_chunk_written_all_fill_is_deleted_not_stored(
    spark, copy_of_cube, truth, new_table, tmp_path, capsys
):
    table = new_table("tomb")
    gbx_zarr_register(
        spark, copy_of_cube, table, writable=True, overlay_root=str(tmp_path / "ov")
    )
    capsys.readouterr()
    before = tree_digest(copy_of_cube)
    ex.delete_a_chunk(spark, table)
    assert shown(capsys) == ex.DELETE_A_CHUNK_output.strip()
    assert tree_digest(copy_of_cube) == before  # the store itself is never written
    got = assemble(spark, table)
    assert np.isnan(got[:12, :91, :180]).all()  # reads as the fill value
    np.testing.assert_array_equal(got[12:], truth["t2m"].values[12:])
    np.testing.assert_array_equal(
        got[:12, 91:], truth["t2m"].values[:12, 91:]
    )  # the neighbours are untouched


def _writable_copy(spark, copy_of_cube, tmp_path, name):
    table = name  # a fixed name: the Fix lines quote it
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    gbx_zarr_register(
        spark, copy_of_cube, table, writable=True, overlay_root=str(tmp_path / "ov")
    )
    return table


def test_a_killed_publish_leaves_a_lock_that_a_sync_clears_once_it_is_old(
    spark, copy_of_cube, new_table, tmp_path, monkeypatch, capsys
):
    from databricks.labs.gbx.zarr import _sync

    table = _writable_copy(spark, copy_of_cube, tmp_path, "weather_lock")
    try:
        update_one_chunk(spark, table)
        # a publish that dies after it took the lock never releases it
        monkeypatch.setattr(_sync, "_release", lambda *a, **k: None)
        monkeypatch.setattr(
            _sync, "_AFTER_LOCK", lambda: (_ for _ in ()).throw(KeyboardInterrupt())
        )
        with pytest.raises(KeyboardInterrupt):
            gbx_zarr_export(spark, table, method="publish")
        monkeypatch.undo()
        # the lock is minutes old here and can be broken after an hour: move the clock on
        monkeypatch.setattr(_sync, "_lock_age", lambda lock: 2 * 3600.0)
        capsys.readouterr()
        ex.clear_a_stale_publish_lock(spark, table)
        assert shown(capsys) == ex.CLEAR_A_STALE_LOCK_output.strip()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_a_write_during_a_publish_conflicts_and_sync_then_publish_recovers(
    spark, copy_of_cube, truth, new_table, tmp_path, monkeypatch, capsys
):
    from databricks.labs.gbx.zarr import _sync

    table = _writable_copy(spark, copy_of_cube, tmp_path, "weather_conflict")
    try:
        update_one_chunk(spark, table)
        later = chunk_patch(spark, table, delta=5.0)  # a writer that read before the publish
        original, fired = _sync._restamp, []

        def write_lands_first(rows, base):
            if not fired:  # the publish's files are in the store; its commit is next
                fired.append(1)
                gbx_zarr_write(later, table, mode="update")
            return original(rows, base)

        monkeypatch.setattr(_sync, "_restamp", write_lands_first)
        capsys.readouterr()
        ex.recover_a_publish(spark, table)
        assert shown(capsys) == ex.RECOVER_A_PUBLISH_output.strip()
        plain = xr.open_zarr(copy_of_cube, consolidated=False)["t2m"].values
        np.testing.assert_array_equal(plain, assemble(spark, table))  # one version again
        np.testing.assert_array_equal(
            plain[:12, :91, :180], truth["t2m"].values[:12, :91, :180] + 4.0
        )  # -1 then +5: the later write won
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


# --- time units ---------------------------------------------------------------------------


def test_times_between_the_steps_of_an_integer_axis_are_refused(
    spark, cube, new_table, tmp_path, capsys
):
    table, _ = first_day(spark, cube, new_table, tmp_path, "units")
    capsys.readouterr()
    before = gbx_zarr_history(spark, table).count()
    ex.refuse_fractional_times(spark, table)
    assert shown(capsys) == ex.REFUSE_FRACTIONAL_TIMES_output.strip()
    assert gbx_zarr_history(spark, table).count() == before  # nothing was committed


def test_unit_abbreviations_are_refused_on_read(spark, tmp_path):
    """Unit names are days, hours, minutes, seconds, milliseconds, microseconds and nanoseconds
    (any case, singular or plural); UDUNITS spellings such as ``h`` and ``hrs`` are not."""
    outcome = {}
    for unit in ("Hour", "hours", "HOURS", "h", "hrs"):
        path = str(tmp_path / f"t_{unit}.zarr")
        group = zarr.open_group(path, mode="w", zarr_format=2)
        v = group.create_array("v", shape=(3,), chunks=(3,), dtype="float32")
        v[:] = np.arange(3, dtype="float32")
        v.attrs["_ARRAY_DIMENSIONS"] = ["time"]
        t = group.create_array("time", shape=(3,), chunks=(3,), dtype="int64")
        t[:] = np.arange(3)
        t.attrs["_ARRAY_DIMENSIONS"] = ["time"]
        t.attrs["units"] = f"{unit} since 2023-01-01"
        try:
            gbx_zarr_read(spark, path, variables=["v"], output="table").collect()
            outcome[unit] = "read"
        except ZarrUnsupported as err:
            assert "not a CF time unit" in str(err) and "Fix:" in str(err)
            outcome[unit] = "refused"
    assert outcome == {
        "Hour": "read",
        "hours": "read",
        "HOURS": "read",
        "h": "refused",
        "hrs": "refused",
    }


# --- the page (no Spark) ------------------------------------------------------------------


@pytest.mark.structure
def test_every_snippet_the_page_shows_exists():
    text = PAGE.read_text()
    names = set(re.findall(r'functionName="([A-Za-z_0-9]+)"', text))
    names |= set(re.findall(r'outputConstant="([A-Za-z_0-9]+)"', text))
    assert names, "the page shows no code from the examples"
    missing = sorted(n for n in names if not hasattr(ex, n))
    assert not missing, f"not in zarr_write_examples.py: {missing}"


@pytest.mark.structure
def test_the_page_is_wired_in_and_speaks_to_users():
    sidebar = (DOCS.parent / "sidebars.js").read_text()
    assert "'writers/zarr'" in sidebar
    pattern = re.compile(r"\bwave[ -]?\d+|\btask \d+|gate g\d|ruling|\bZT\d+", re.I)
    assert not pattern.findall(PAGE.read_text())
    for page in ("zarr.mdx", "overview.mdx"):
        assert "../writers/zarr" in (DOCS / "formats" / page).read_text()
