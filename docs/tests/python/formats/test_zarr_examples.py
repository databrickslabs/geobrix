"""Executes the Formats / Zarr doc examples on the sample cube (Docker only).

Every code block on ``docs/docs/formats/zarr.mdx`` is a function of ``zarr_examples.py``;
these tests run each one against the real sample cube on a local Delta session and assert
what the page says it does. The manifest of a Zarr store is a Delta table, so the session is
the Delta one from ``conftest.py``.

Run the whole file: the store tests tell one story and each starts from the store the one
before it left (create, append, tag, update, ..., vacuum). The registered-store tests at the
end are independent of it. They register a copy of the cube in a temporary directory: a
local session lists a ``/Volumes`` path through Spark, which only Databricks can do.

"Concurrent" writers: two threads really append at once. A conflict needs one write to land
between another's files and its commit, which two Spark jobs hit only by luck, so that test
holds the second write at ``_write._BEFORE_COMMIT`` (a hook the Zarr unit tests use too) until
the first has committed. The code under test is the code on the page either way.
"""

import hashlib
import os
import re
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import zarr_examples as ex  # noqa: E402

xr = pytest.importorskip("xarray")
pytest.importorskip("zarr")

from databricks.labs.gbx.zarr import (  # noqa: E402
    BLOCKS_SCHEMA,
    ZarrVersionUnavailable,
    block_to_numpy,
    gbx_zarr_history,
    gbx_zarr_read,
    gbx_zarr_write,
)

DOCS = Path(__file__).resolve().parents[3] / "docs"
PAGES = [DOCS / "formats" / "overview.mdx", DOCS / "formats" / "zarr.mdx"]
IMAGES = (
    Path(__file__).resolve().parents[4] / "resources" / "images" / "diagrams" / "zarr"
)
VARS = ["t2m", "msl", "time", "latitude", "longitude"]


# --- helpers ---------------------------------------------------------------------------


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


def patch_frame(spark, source, delta, first_hours=12, variable="t2m"):
    """Blocks of the first ``first_hours`` of ``variable``, every cell moved by ``delta``."""
    stop = f"2023-01-01T{first_hours - 1:02d}:00"
    rows = gbx_zarr_read(
        spark,
        source,
        variables=[variable],
        sel={"time": slice("2023-01-01T00:00", stop)},
    ).collect()
    moved = [
        {**r.asDict(), "data": (block_to_numpy(r) + delta).astype("float32").tobytes()}
        for r in rows
    ]
    return spark.createDataFrame(moved, BLOCKS_SCHEMA)


def latest_version(spark, table):
    return max(r["version"] for r in gbx_zarr_history(spark, table).collect())


def tree_digest(path):
    h = hashlib.sha256()
    for f in sorted(p for p in Path(path).rglob("*") if p.is_file()):
        h.update(f.relative_to(path).as_posix().encode())
        h.update(f.read_bytes())
    return h.hexdigest()


def shown(capsys):
    """What an example printed, without the progress lines register and sync print."""
    lines = capsys.readouterr().out.splitlines()
    return "\n".join(ln for ln in lines if not ln.startswith("[register] ")).strip()


@pytest.fixture(scope="module")
def truth(cube):
    """The sample cube, loaded: what every store made from it must hold."""
    return xr.open_zarr(cube, consolidated=False).load()


@pytest.fixture(scope="module")
def story(spark, truth, tmp_path_factory):
    """One managed store the tests below build up in order. ``t2m`` is the model of it."""
    s = SimpleNamespace(
        table=f"fmt_weather_{uuid.uuid4().hex[:8]}",
        root=str(tmp_path_factory.mktemp("weather") / "era5"),
        t2m=truth["t2m"].values.copy(),
        tagged=None,
        version=0,
    )
    yield s
    spark.sql(f"DROP TABLE IF EXISTS {s.table}")


# --- sample data ----------------------------------------------------------------------


def test_setup_paths_match_the_sample_data_layout():
    assert ex.CUBE_SUBPATH in ex.SETUP
    assert ex.SETUP.count("cube = ") == 1


def test_the_sample_cube_can_be_generated(spark, tmp_path):
    written = ex.make_the_sample_cube(str(tmp_path / "era5-weather"))
    assert sorted(written) == ["era5_like_v2.zarr", "era5_like_v3.zarr"]
    rows = gbx_zarr_read(
        spark,
        str(written["era5_like_v3.zarr"]),
        variables=["t2m"],
        sel={"time": "2023-01-01T00:00", "latitude": 48.0, "longitude": 2.0},
        output="table",
    ).collect()
    assert len(rows) == 1


def test_a_selection_reads_as_a_table(spark, cube, truth, capsys):
    df = ex.read_a_selection(spark, cube)
    assert df.count() == 3 * 21 * 31
    expected = truth["t2m"].sel(
        time="2023-01-01T00:00", latitude=slice(60, 40), longitude=slice(0, 30)
    )
    got = df.where("time = timestamp'2023-01-01 00:00:00'").toPandas()
    assert len(got) == expected.size
    assert np.isclose(
        got.sort_values(["latitude", "longitude"])["t2m"].sum(),
        float(expected.sum()),
        rtol=1e-5,
    )
    assert shown(capsys) == ex.READ_A_SELECTION_output.strip()


# --- store kinds: managed, ownership, file modes --------------------------------------


def test_create_a_managed_store(spark, cube, story):
    story.version = ex.create_a_store(spark, cube, story.table, story.root)
    assert story.version == 1
    assert sorted(os.listdir(story.root)) == ["_gbx_owner", "_gbx_store.json", "data"]
    arrays = {
        r["array"]
        for r in spark.sql(
            f"SELECT array FROM {story.table} WHERE kind = 'array'"
        ).collect()
    }
    assert arrays == set(VARS)
    np.testing.assert_array_equal(assemble(spark, story.table), story.t2m[:12])


def test_a_root_has_one_owner(spark, cube, story, new_table, capsys):
    other = new_table("other")
    ex.refuse_a_second_owner(spark, cube, other, story.root)
    text = shown(capsys)
    assert "already owned by table" in text and story.table in text and "Fix:" in text
    assert not spark.catalog.tableExists(other)


def test_file_modes(spark, cube, new_table, tmp_path, capsys):
    table = new_table("fuse")
    ex.create_with_a_file_mode(spark, cube, table, str(tmp_path / "fuse_store"))
    props = dict(spark.sql(f"SHOW TBLPROPERTIES {table}").collect())
    assert props["geobrix.zarr.file_mode"] == "fuse"
    capsys.readouterr()
    ex.managed_is_not_available(
        spark, cube, new_table("managed"), str(tmp_path / "managed_store")
    )
    assert shown(capsys) == ex.MANAGED_IS_NOT_AVAILABLE_output.strip()


# --- transactions ---------------------------------------------------------------------


def test_append_hours(spark, cube, story):
    story.version = ex.append_hours(spark, cube, story.table)
    assert story.version == 2
    np.testing.assert_array_equal(assemble(spark, story.table), story.t2m[:24])


def test_two_writers_append_at_once(spark, cube, story):
    versions = ex.append_from_two_writers(spark, cube, story.table)
    assert sorted(versions) == [3, 4]  # each landed once, whichever went first
    story.version = 4
    np.testing.assert_array_equal(
        assemble(spark, story.table), story.t2m
    )  # all 48 hours, in order


def test_tag_a_version(spark, story):
    assert (
        ex.tag_a_version(spark, story.table) == 48
    )  # the tagged version reads in full
    story.version = latest_version(spark, story.table)
    assert story.version == 5  # the tag is a commit of its own
    story.tagged = story.t2m.copy()
    live = spark.sql(
        f"SELECT count(*) FROM {story.table} WHERE tag IS NULL AND kind <> 'commit'"
    ).first()[0]
    tagged = spark.sql(
        f"SELECT count(*) FROM {story.table} WHERE tag = 'two-days'"
    ).first()[0]
    assert tagged == live


def test_update_a_region(spark, story):
    story.version = ex.update_a_region(spark, story.table)
    assert story.version == 6
    story.t2m[:12] -= 1.0
    np.testing.assert_array_equal(assemble(spark, story.table), story.t2m)


def test_a_conflict_names_the_chunks_and_the_winner(spark, story, monkeypatch, capsys):
    from databricks.labs.gbx.zarr import _write

    loser = patch_frame(spark, story.table, +10.0)
    winner = patch_frame(spark, story.table, +5.0)
    winner_id, landed, fired = uuid.uuid4().hex, [], []

    def winner_lands_first(write_id):
        if not fired:  # the loser is the first write to reach its commit
            fired.append(write_id)
            landed.append(
                gbx_zarr_write(winner, story.table, mode="update", write_id=winner_id)
            )

    monkeypatch.setattr(_write, "_BEFORE_COMMIT", winner_lands_first)
    ex.handle_a_conflict(spark, story.table, loser)
    monkeypatch.setattr(_write, "_BEFORE_COMMIT", None)
    out = shown(capsys)
    assert f"lost to {winner_id}" in out and "t2m/c/" in out
    story.version = landed[0]
    assert story.version == 7
    story.t2m[:12] += 5.0  # the winner's write is the store; nothing of the loser's is
    np.testing.assert_array_equal(assemble(spark, story.table), story.t2m)
    committed = {r["write_id"] for r in gbx_zarr_history(spark, story.table).collect()}
    assert winner_id in committed and fired[0] not in committed


def test_a_retried_write_lands_once(spark, story):
    before = gbx_zarr_history(spark, story.table).count()
    version, again = ex.retry_with_a_write_id(
        spark, story.table, patch_frame(spark, story.table, -4.0)
    )
    assert version == 8 and again == version
    assert gbx_zarr_history(spark, story.table).count() == before + 1
    story.version = version
    story.t2m[:12] -= 4.0
    np.testing.assert_array_equal(assemble(spark, story.table), story.t2m)


# --- snapshots ------------------------------------------------------------------------


def test_history(spark, story, capsys):
    ex.read_history(spark, story.table)
    assert shown(capsys) == ex.READ_HISTORY_output.strip()


def test_time_travel(spark, story, capsys):
    ex.time_travel(spark, story.table)
    assert shown(capsys) == ex.TIME_TRAVEL_output.strip()


def test_restore_a_tagged_version(spark, story):
    story.version = ex.restore_a_version(spark, story.table)
    assert story.version == 9
    story.t2m = story.tagged.copy()
    np.testing.assert_array_equal(assemble(spark, story.table), story.t2m)
    assert (
        gbx_zarr_history(spark, story.table).orderBy("version").collect()[-1]["mode"]
        == "restore"
    )


def test_export_a_version(spark, story, tmp_path):
    to = str(tmp_path / "export.zarr")
    ex.export_a_version(spark, story.table, to)
    ds = xr.open_zarr(to)
    np.testing.assert_array_equal(ds["t2m"].values, story.t2m)
    assert sorted(os.listdir(to)) == [
        "latitude",
        "longitude",
        "msl",
        "t2m",
        "time",
        "zarr.json",
    ]


# --- lifecycle and rows ---------------------------------------------------------------


def test_retention_properties(spark, story, capsys):
    ex.show_retention(spark, story.table)
    assert shown(capsys) == ex.SHOW_RETENTION_output.strip()


def test_chunk_rows_fold_into_shard_rows(spark, story, capsys):
    ex.count_manifest_rows(spark, story.table)
    assert shown(capsys) == ex.COUNT_MANIFEST_ROWS_before_output.strip()
    story.version = ex.optimize_the_store(spark, story.table)
    assert story.version == 10
    ex.count_manifest_rows(spark, story.table)
    assert shown(capsys) == ex.COUNT_MANIFEST_ROWS_after_output.strip()
    np.testing.assert_array_equal(assemble(spark, story.table), story.t2m)  # same cells


def test_vacuum_deletes_what_no_readable_version_needs(
    spark, story, monkeypatch, capsys
):
    from databricks.labs.gbx.zarr import _maint

    def pack_files():
        return {
            str(p.relative_to(story.root))
            for p in Path(story.root, "data").rglob("*.zpack")
        }

    before = pack_files()
    later = datetime.now(timezone.utc) + timedelta(days=8)  # a week has passed
    monkeypatch.setattr(_maint, "_utcnow", lambda: later)
    ex.vacuum_the_store(spark, story.table)
    assert shown(capsys) == ex.VACUUM_THE_STORE_output.strip()
    after = pack_files()
    assert after < before  # packs went, none was added
    with pytest.raises(ZarrVersionUnavailable):  # version 1 is past the horizon now
        gbx_zarr_read(spark, story.table, variables=["t2m"], version_as_of=1)
    np.testing.assert_array_equal(  # the tag, and the live store, still read
        assemble(spark, story.table, version_as_of="tag:two-days"), story.tagged
    )
    np.testing.assert_array_equal(assemble(spark, story.table), story.t2m)


# --- registered stores ----------------------------------------------------------------


def test_register_in_place_leaves_the_store_alone(
    spark, copy_of_cube, truth, new_table
):
    table, before = new_table("reg"), tree_digest(copy_of_cube)
    assert ex.register_in_place(spark, copy_of_cube, table) == 1
    assert tree_digest(copy_of_cube) == before
    np.testing.assert_array_equal(assemble(spark, table), truth["t2m"].values)


def test_sample_a_time_series(spark, copy_of_cube, truth, new_table, capsys):
    table = new_table("sample")
    ex.register_in_place(spark, copy_of_cube, table)
    capsys.readouterr()
    rows = ex.sample_a_time_series(spark, table).collect()
    assert shown(capsys) == ex.SAMPLE_A_TIME_SERIES_output.strip()
    assert len(rows) == 6
    for r in rows:
        lon, lat = (2.0, 48.0) if r["name"] == "paris" else (0.0, 51.0)
        want = truth["t2m"].sel(time=r["time"], latitude=lat, longitude=lon).item()
        assert r["t2m"] == pytest.approx(want)


def test_an_overlay_keeps_the_base_and_each_registration_its_own_writes(
    spark, copy_of_cube, truth, new_table, tmp_path, capsys
):
    base_digest = tree_digest(copy_of_cube)
    read_only, writable = new_table("ro"), new_table("rw")
    ex.register_in_place(spark, copy_of_cube, read_only)
    overlay = str(tmp_path / "overlay")
    assert ex.register_with_an_overlay(spark, copy_of_cube, writable, overlay) == 1
    assert sorted(os.listdir(overlay)) == ["_gbx_owner", "_gbx_store.json"]
    assert ex.update_a_region(spark, writable) == 2
    assert tree_digest(copy_of_cube) == base_digest  # the base is never written
    assert sorted(os.listdir(overlay)) == ["_gbx_owner", "_gbx_store.json", "data"]
    moved = truth["t2m"].values.copy()
    moved[:12] -= 1.0
    np.testing.assert_array_equal(assemble(spark, writable), moved)
    np.testing.assert_array_equal(assemble(spark, read_only), truth["t2m"].values)
    capsys.readouterr()
    ex.compare_branches(spark, read_only, writable)
    assert shown(capsys) == ex.COMPARE_BRANCHES_output.strip()


def test_a_change_outside_geobrix_is_detected_then_synced(
    spark, copy_of_cube, truth, new_table, capsys
):
    table = new_table("drift")
    ex.register_in_place(spark, copy_of_cube, table)
    shard = Path(copy_of_cube) / "t2m" / "c" / "0" / "0" / "0"
    stat = shard.stat()
    os.utime(shard, (stat.st_atime, stat.st_mtime + 30))  # someone rewrote the object
    ex.detect_outside_changes(spark, table)
    assert shown(capsys) == ex.DETECT_OUTSIDE_CHANGES_output.strip()
    np.testing.assert_array_equal(assemble(spark, table), truth["t2m"].values)


def test_publish_writes_the_overlay_into_the_store(
    spark, copy_of_cube, truth, new_table, tmp_path
):
    table = new_table("pub")
    ex.register_with_an_overlay(spark, copy_of_cube, table, str(tmp_path / "overlay"))
    ex.update_a_region(spark, table)
    before = tree_digest(copy_of_cube)
    version = ex.publish_the_overlay(spark, table)
    assert isinstance(version, int) and tree_digest(copy_of_cube) != before
    plain = xr.open_zarr(copy_of_cube, consolidated=False)["t2m"].values
    moved = truth["t2m"].values.copy()
    moved[:12] -= 1.0
    np.testing.assert_array_equal(plain, moved)  # a plain Zarr reader sees it
    np.testing.assert_array_equal(assemble(spark, table), moved)


# --- the pages (no Spark) ---------------------------------------------------------------


@pytest.mark.structure
def test_every_snippet_a_page_shows_exists():
    names = set()
    for page in PAGES:
        text = page.read_text()
        names |= set(re.findall(r'functionName="([A-Za-z_0-9]+)"', text))
        names |= set(re.findall(r'outputConstant="([A-Za-z_0-9]+)"', text))
    assert names, "the pages show no code from the examples"
    missing = sorted(n for n in names if not hasattr(ex, n))
    assert not missing, f"not in zarr_examples.py: {missing}"


@pytest.mark.structure
def test_diagrams_are_linked_relatively_with_descriptive_alt_text():
    text = (DOCS / "formats" / "zarr.mdx").read_text()
    images = re.findall(r"!\[([^\]]*)\]\(([^)]+)\)", text)
    found = {Path(p).stem: (alt, p) for alt, p in images}
    for name in (
        "store-kinds",
        "write-commit",
        "concurrent-writers",
        "retention-timeline",
        "chunk-vs-shard-rows",
    ):
        assert name in found, f"zarr.mdx does not show the {name} diagram"
        alt, link = found[name]
        assert (
            len(alt) >= 120
        ), f"alt text of {name} is too short to describe the diagram"
        assert link.startswith(
            "../../../resources/images/diagrams/zarr/"
        ) and link.endswith(".png")
        assert not link.startswith(("http", "data:"))
        assert (IMAGES / f"{name}.png").is_file() and (IMAGES / f"{name}.svg").is_file()


@pytest.mark.structure
def test_zarr_page_sections_come_in_the_planned_order():
    text = (DOCS / "formats" / "zarr.mdx").read_text()
    titles = [h.strip() for h in re.findall(r"^## (.+)$", text, flags=re.M)]
    wanted = [
        "Why Zarr",
        "Store kinds",
        "File modes",
        "Transactions",
        "Snapshots",
        "Lifecycle",
        "Chunk rows",
        "Limits",
    ]
    positions = []
    for want in wanted:
        hits = [i for i, t in enumerate(titles) if t.startswith(want)]
        assert hits, f"no '## {want}...' section; the page has {titles}"
        positions.append(hits[0])
    assert positions == sorted(positions), f"sections out of order: {titles}"


@pytest.mark.structure
def test_the_pages_are_wired_in_and_speak_to_users():
    sidebar = (DOCS.parent / "sidebars.js").read_text()
    assert "'formats/overview'" in sidebar and "'formats/zarr'" in sidebar
    assert sidebar.index("'formats/zarr'") < sidebar.index("label: 'Readers & Writers'")
    raster = (DOCS / "readers" / "raster.mdx").read_text()
    assert "formats/overview" in raster and "### Format Selection Guide" not in raster
    pattern = re.compile(r"\bwave[ -]?\d+|\btask \d+|gate g\d|ruling|\bZT\d+", re.I)
    for page in PAGES:
        leaks = pattern.findall(page.read_text())
        assert not leaks, f"{page.name} leaks internal vocabulary: {leaks}"
