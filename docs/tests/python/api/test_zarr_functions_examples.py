"""Executes the Zarr function reference examples on the sample cube (Docker only).

Every code block on ``docs/docs/api/zarr-functions.mdx`` is a function of
``zarr_functions_examples.py``; these tests run each one against the real sample cube on a
local Delta session and assert what the page says it does. The manifest of a Zarr store is
a Delta table, so the session is the Delta one from ``zarr_delta.py``.

Run the whole file: the managed-store tests tell one story and each starts from the store the
one before it left (create, append, tag, optimize, restore, ...). The registered-store tests
at the end are independent of it. They register a copy of the cube in a temporary directory:
a local session lists a ``/Volumes`` path through Spark, which only Databricks can do.
"""

import ast
import os
import re
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import zarr_functions_examples as ex  # noqa: E402
from zarr_delta import copy_of_cube, cube, new_table, spark  # noqa: E402,F401

xr = pytest.importorskip("xarray")
pytest.importorskip("zarr")

pytestmark = pytest.mark.delta_session  # runs after the tests that use the shared session

import databricks.labs.gbx.zarr as zarr_pkg  # noqa: E402
from databricks.labs.gbx.zarr import (  # noqa: E402
    BLOCKS_SCHEMA,
    block_to_numpy,
    gbx_zarr_history,
    gbx_zarr_read,
    gbx_zarr_register,
    gbx_zarr_write,
)

DOCS = Path(__file__).resolve().parents[3] / "docs"
PAGE = DOCS / "api" / "zarr-functions.mdx"
PKG = Path(zarr_pkg.__file__).parent
NAMES = ["t2m", "msl", "time", "latitude", "longitude"]


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


def hours(spark, cube, start, stop):
    return gbx_zarr_read(spark, cube, variables=NAMES, sel={"time": slice(start, stop)})


@pytest.fixture(scope="module")
def truth(cube):
    return xr.open_zarr(cube, consolidated=False)[["t2m", "msl"]].load()


@pytest.fixture(scope="module")
def story(spark, tmp_path_factory):
    """One managed store the tests below build up in order."""
    s = SimpleNamespace(
        table=f"api_weather_{uuid.uuid4().hex[:8]}",
        root=str(tmp_path_factory.mktemp("api") / "era5"),
        tmp=tmp_path_factory.mktemp("api_out"),
    )
    yield s
    spark.sql(f"DROP TABLE IF EXISTS {s.table}")


# --- the managed store, in order ---------------------------------------------------------


def test_setup_paths_match_the_sample_data_layout():
    assert ex.CUBE_SUBPATH in ex.SETUP
    assert ex.SETUP.count("cube = ") == 1


def test_write_a_store(spark, cube, truth, story):
    assert ex.write_a_store(spark, cube, story.table, story.root) == 1
    np.testing.assert_array_equal(assemble(spark, story.table), truth["t2m"].values[:12])


def test_read_a_store(spark, story, capsys):
    ex.read_a_store(spark, story.table)
    assert shown(capsys) == ex.READ_A_STORE_output.strip()


def test_blocks_become_arrays(spark, story, truth, capsys):
    ex.convert_blocks(spark, story.table)
    assert shown(capsys) == ex.CONVERT_BLOCKS_output.strip()
    assert BLOCKS_SCHEMA.startswith("variable STRING")


def test_sample_a_point(spark, story, truth, capsys):
    row = ex.sample_a_point(spark, story.table).first()
    assert shown(capsys) == ex.SAMPLE_A_POINT_output.strip()
    want = truth["t2m"].sel(time="2023-01-01T06:00", latitude=48.0, longitude=2.0)
    assert row["t2m"] == pytest.approx(float(want))


def test_append_then_the_history_lists_every_commit(spark, cube, story, truth, capsys):
    assert (
        gbx_zarr_write(
            hours(spark, cube, "2023-01-01T12:00", "2023-01-01T23:00"),
            story.table,
            mode="append",
        )
        == 2
    )
    ex.show_the_history(spark, story.table)
    assert shown(capsys) == ex.SHOW_THE_HISTORY_output.strip()
    np.testing.assert_array_equal(
        assemble(spark, story.table), truth["t2m"].values[:24]
    )


def test_tag_a_version(spark, story, capsys):
    assert ex.tag_a_version(spark, story.table) == 24
    assert shown(capsys) == ex.TAG_A_VERSION_output.strip()
    assert gbx_zarr_history(spark, story.table).count() == 3


def test_optimize_folds_the_chunk_rows(spark, story, truth):
    before = spark.sql(
        f"SELECT count(*) FROM {story.table} WHERE kind = 'chunk' AND array = 't2m' "
        "AND tag IS NULL"
    ).first()[0]
    assert before > 1
    assert ex.fold_the_store(spark, story.table) == 4
    after = spark.sql(
        f"SELECT count(*) FROM {story.table} WHERE kind = 'chunk' AND array = 't2m' "
        "AND tag IS NULL"
    ).first()[0]
    assert after == 0
    np.testing.assert_array_equal(assemble(spark, story.table), truth["t2m"].values[:24])


def test_restore_makes_the_store_a_tagged_version_again(spark, story, truth):
    assert ex.restore_a_tag(spark, story.table) == 5
    np.testing.assert_array_equal(assemble(spark, story.table), truth["t2m"].values[:24])
    assert gbx_zarr_history(spark, story.table).orderBy("version").collect()[-1][
        "mode"
    ] == "restore"


def test_vacuum_lists_before_it_deletes(spark, story, capsys):
    ex.vacuum_a_store(spark, story.table)
    assert shown(capsys) == ex.VACUUM_A_STORE_output.strip()


def test_untag_a_version(spark, story):
    assert ex.untag_a_version(spark, story.table) is None
    tags = spark.sql(f"SELECT count(*) FROM {story.table} WHERE tag = 'day-one'").first()[0]
    assert tags == 0


def test_export_a_copy(spark, story, truth):
    to = str(story.tmp / "copy.zarr")
    assert ex.export_a_copy(spark, story.table, to) == to
    ds = xr.open_zarr(to)
    np.testing.assert_array_equal(ds["t2m"].values, truth["t2m"].values[:24])


def test_export_a_reference(spark, story, truth):
    to = str(story.tmp / "reference.json")
    assert ex.export_a_reference(spark, story.table, to) == to
    ds = xr.open_zarr("reference://", storage_options={"fo": to}, consolidated=False)
    np.testing.assert_array_equal(ds["t2m"].values, truth["t2m"].values[:24])


def test_every_zarr_error_is_a_zarr_error_with_a_fix(spark, story, capsys):
    ex.catch_a_zarr_error(spark, story.table)
    assert shown(capsys) == ex.CATCH_A_ZARR_ERROR_output.strip()


# --- registered stores ------------------------------------------------------------------


def test_register_in_place(spark, copy_of_cube, new_table, truth):
    table = new_table("reg")
    assert ex.register_a_store(spark, copy_of_cube, table) == 1
    np.testing.assert_array_equal(assemble(spark, table), truth["t2m"].values)


def test_sync_records_what_another_tool_changed(spark, copy_of_cube, new_table, capsys):
    table = new_table("sync")
    gbx_zarr_register(spark, copy_of_cube, table)
    shard = Path(copy_of_cube) / "t2m" / "c" / "0" / "0" / "0"
    stat = shard.stat()
    os.utime(shard, (stat.st_atime, stat.st_mtime + 30))  # someone rewrote the object
    capsys.readouterr()
    ex.sync_a_store(spark, table)
    assert shown(capsys) == ex.SYNC_A_STORE_output.strip()


def test_publish_writes_the_overlay_into_the_store(
    spark, copy_of_cube, new_table, truth, tmp_path
):
    table = new_table("pub")
    gbx_zarr_register(
        spark, copy_of_cube, table, writable=True, overlay_root=str(tmp_path / "ov")
    )
    rows = gbx_zarr_read(
        spark,
        table,
        variables=["t2m"],
        isel={"time": slice(0, 12), "latitude": slice(0, 91), "longitude": slice(0, 180)},
    ).collect()
    patch = spark.createDataFrame(
        [
            {**r.asDict(), "data": (block_to_numpy(r) - 1).astype("float32").tobytes()}
            for r in rows
        ],
        BLOCKS_SCHEMA,
    )
    gbx_zarr_write(patch, table, mode="update")
    version = ex.publish_an_overlay(spark, table)
    assert isinstance(version, int)
    plain = xr.open_zarr(copy_of_cube, consolidated=False)["t2m"].values
    want = truth["t2m"].values.copy()
    want[:12, :91, :180] -= 1.0
    np.testing.assert_array_equal(plain, want)  # a plain Zarr reader sees it


# --- the page (no Spark) ------------------------------------------------------------------


def _text(node, named):
    """A string expression of the package as text, with ``{…}`` where an f-string inserts a
    value; ``None`` for anything that is not built from literals and module-level names."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            v.value if isinstance(v, ast.Constant) else "{…}" for v in node.values
        )
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _text(node.left, named), _text(node.right, named)
        return None if left is None or right is None else left + right
    if isinstance(node, ast.Name):
        return named.get(node.id)
    return None


def _fix_texts():
    """Every fix the package gives: the ``fix=`` of a ``ZarrError``, and what follows ``Fix: ``
    in the message of a ``ValueError``."""
    out = set()
    for path in PKG.glob("*.py"):
        tree = ast.parse(path.read_text())
        named = {}
        for node in tree.body:
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
            ):
                value = _text(node.value, named)
                if value is not None:
                    named[node.targets[0].id] = value
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == "fix" and _text(kw.value, named) is not None:
                        out.add(_text(kw.value, named))
            if isinstance(node, (ast.Constant, ast.JoinedStr)):
                value = _text(node, named)
                if value and "Fix: " in value:
                    out.add(value.split("Fix: ", 1)[1])
    return out


_WILDCARD = re.compile(r"'?<[^>]*>'?")  # <name> in a cell: a value the message fills in
_FILLED = r"'?(?:\{…\}|<[^>]*>)'?"  # what <name> matches in the source: {…}, or text in <>


def _ends_at_a_boundary(cell, rest):
    """Whether a cell may stop where ``rest`` (the rest of the fix) goes on: at the end, after a
    sentence (or the cell's own full stop), or before a clause in parentheses."""
    if rest in ("", "."):
        return True
    if rest in ("; {…}", ": {…}"):  # the instruction itself is what follows: spell it out
        return False
    if cell.endswith((".", ";")):
        return rest[:1].isspace()
    return rest.startswith(("; ", ". ", " ("))


def _starts_a_fix(cell, fixes):
    """Whether ``cell`` is the start of one of ``fixes``, ending at a boundary of the text.

    ``<name>`` in a cell stands for a value the message fills in (``{…}`` in the source). A cell
    that covers a whole fix may also spell out what a ``{…}`` of the source stands for.
    """
    literal = _WILDCARD.split(cell)
    start = re.compile(_FILLED.join(re.escape(p) for p in literal), re.S)
    covered = _WILDCARD.sub("X", cell).rstrip(".")
    for fix in fixes:
        if fix.startswith("{…}"):  # a fix that is only a filled-in value says nothing
            continue
        m = start.match(fix)
        if m and _ends_at_a_boundary(cell, fix[m.end() :]):
            return True
        whole = ".+?".join(re.escape(p) for p in fix.rstrip(".").split("{…}"))
        if "{…}" in fix and re.fullmatch(whole, covered, re.S):
            return True
    return False


@pytest.mark.structure
def test_every_snippet_the_page_shows_exists():
    text = PAGE.read_text()
    names = set(re.findall(r'functionName="([A-Za-z_0-9]+)"', text))
    names |= set(re.findall(r'outputConstant="([A-Za-z_0-9]+)"', text))
    assert names, "the page shows no code from the examples"
    missing = sorted(n for n in names if not hasattr(ex, n))
    assert not missing, f"not in zarr_functions_examples.py: {missing}"


@pytest.mark.structure
def test_every_public_name_has_a_section_with_its_signature():
    import inspect

    text = PAGE.read_text()
    for name in zarr_pkg.__all__:
        if name.startswith("Zarr"):
            continue
        assert re.search(rf"^## `?{name}`?\b|^### `?{name}`?\b", text, re.M), (
            f"{name} has no section on the page"
        )
    for name in (n for n in zarr_pkg.__all__ if n.startswith("gbx_zarr_")):
        fn = getattr(zarr_pkg, name)
        for param in inspect.signature(fn).parameters:
            if param == "spark":
                continue
            assert f"`{param}`" in text, f"{name}: parameter {param} is not documented"
    for cls in (n for n in zarr_pkg.__all__ if n.startswith("Zarr")):
        assert f"`{cls}`" in text, f"{cls} is not documented"


@pytest.mark.structure
def test_signatures_on_the_page_are_the_signatures_in_the_package():
    import inspect

    text = PAGE.read_text()
    for name in (n for n in zarr_pkg.__all__ if n.startswith("gbx_zarr_")):
        fn = getattr(zarr_pkg, name)
        shown_sig = re.search(rf"```python\n{name}\((.*?)\)(?: -> [^\n]+)?\n```", text, re.S)
        assert shown_sig, f"{name} shows no signature"
        got = [
            p.strip().split("=")[0].strip().lstrip("*").strip()
            for p in re.split(r",\s*(?![^\[\(]*[\]\)])", shown_sig.group(1))
            if p.strip() and p.strip() != "*"
        ]
        want = list(inspect.signature(fn).parameters)
        assert got == want, f"{name}: page {got} != package {want}"


@pytest.mark.structure
def test_every_fix_the_page_quotes_is_the_start_of_a_fix_in_the_package():
    """A "The fix says" cell is the start of the real fix, not a piece from its middle or its
    end, and stops at a boundary of it (the end, a sentence or a parenthesis), never mid-clause."""
    text = PAGE.read_text()
    fixes = _fix_texts()
    cells = []
    for table in re.findall(
        r"^\| Error \| Raised when \| The fix says \|\n\|[-| ]+\|\n((?:\|.*\n)+)",
        text,
        re.M,
    ):
        for row in table.strip().splitlines():
            cols = [c.strip() for c in row.strip().strip("|").split("|")]
            assert len(cols) == 3 and cols[2].startswith("`"), row
            cells.append(cols[2].strip("`"))
    assert len(cells) >= 40, f"expected the error tables, found {len(cells)} fixes"
    wrong = [c for c in cells if not _starts_a_fix(c, fixes)]
    assert not wrong, f"not the start of a fix of the package: {wrong}"


@pytest.mark.structure
def test_the_fix_guard_rejects_a_tail_a_middle_and_a_cut_in_a_clause():
    fixes = {
        "append {…} labels on the store's step",
        "run gbx_zarr_sync({…}), then publish again",
        "wait for the publish to finish. If it was killed, clear the lock with x",
        "the files were vacuumed; export a later version or a tag",
    }
    assert _starts_a_fix("append '<dimension>' labels on the store's step", fixes)
    assert _starts_a_fix("run gbx_zarr_sync('<table>'), then publish again", fixes)
    assert _starts_a_fix("wait for the publish to finish.", fixes)  # a sentence
    assert _starts_a_fix("the files were vacuumed; export a later version or a tag", {"the files were vacuumed; {…}"})
    assert not _starts_a_fix("labels on the store's step", fixes)  # a tail
    assert not _starts_a_fix("then publish again", fixes)  # a tail
    assert not _starts_a_fix("wait for the publish to finish. If it was killed, clear", fixes)


@pytest.mark.structure
def test_the_page_is_wired_in_and_speaks_to_users():
    sidebar = (DOCS.parent / "sidebars.js").read_text()
    assert "'api/zarr-functions'" in sidebar
    pattern = re.compile(r"\bwave[ -]?\d+|\btask \d+|gate g\d|ruling|\bZT\d+", re.I)
    assert not pattern.findall(PAGE.read_text())
    for page in ("zarr.mdx", "overview.mdx"):
        assert "../api/zarr-functions" in (DOCS / "formats" / page).read_text()
    for page in (DOCS / "readers" / "zarr.mdx", DOCS / "writers" / "zarr.mdx"):
        assert "../api/zarr-functions" in page.read_text()
