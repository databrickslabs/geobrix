"""Run a notebook in pieces: a cell range, the setup cells it needs, and a check that nothing else is needed.

A piece is ``--cells A:B``: the cells numbered A to B, both included, counted from 1 over ALL the notebook's cells
(markdown too), as Databricks numbers them. The uploaded copy keeps the piece's cells and the PRELUDE: the cells
before A that set the session up. The state a later piece needs from an earlier one is carried the way the example
notebooks already carry it between sessions: their outputs are persisted tables and files, and the ``FORCE_*`` flags
(default False) make a cell reuse what exists instead of recomputing it. A piece therefore re-runs only cheap setup,
never the earlier piece's compute.

The automatic prelude is every code cell before A that is setup: a ``%pip``, ``%restart_python`` or ``%run`` cell, or
a cell of definitions only (imports, ``def``/``class``, and assignments with no call in them, such as
``CATALOG = "main"`` or ``FORCE_RELOAD = False``). A cell that computes (a call, a read) is not setup. When the
piece then uses a name that only a skipped cell defines, the piece is refused before anything is submitted, with the
cell to add through ``--prelude``.

Stdlib only.
"""

from __future__ import annotations

import ast
import builtins
import re
from typing import Iterable, Optional

_MAGIC_SETUP = re.compile(r"^\s*(%pip|%restart_python|%run)\b", re.M)
_RESTART = re.compile(r"restartPython\(\)")
# Names a Databricks notebook session has before any cell runs.
_SESSION_GLOBALS = frozenset(dir(builtins)) | {"spark", "sc", "sqlContext", "dbutils", "display", "displayHTML",
                                               "sql", "table", "getArgument"}


def cell_source(cell) -> str:
    src = cell.get("source", "")
    return src if isinstance(src, str) else "".join(src)


def parse_range(spec: str, n_cells: int) -> range:
    """``"A:B"`` (1-based, both ends included; ``A:`` and ``:B`` are open ends) as a 0-based ``range`` of cell indices."""
    m = re.fullmatch(r"\s*(\d*)\s*:\s*(\d*)\s*", spec or "")
    if not m or not (m.group(1) or m.group(2)):
        raise ValueError(f"--cells {spec!r}: expected A:B (cell numbers from 1, both included; A: and :B are open)")
    start = int(m.group(1)) if m.group(1) else 1
    end = int(m.group(2)) if m.group(2) else n_cells
    if start < 1 or end < start or end > n_cells:
        raise ValueError(f"--cells {spec!r}: the notebook has cells 1 to {n_cells}; A must be >= 1 and A <= B <= {n_cells}")
    return range(start - 1, end)


def parse_cell_list(spec: str, n_cells: int) -> list[int]:
    """``"3,5:7"`` (1-based) as sorted 0-based cell indices; the ``--prelude`` form."""
    out: set[int] = set()
    for part in (p.strip() for p in (spec or "").split(",")):
        if not part:
            continue
        out.update(parse_range(part if ":" in part else f"{part}:{part}", n_cells))
    return sorted(out)


def range_label(rng: range) -> str:
    return f"{rng.start + 1}:{rng.stop}"


def _python_of(src: str) -> str:
    """The Python of a cell, its magic and shell lines blanked (so line numbers stay)."""
    return "\n".join("" if line.lstrip().startswith(("%", "!")) else line for line in src.splitlines())


def _parse(src: str) -> Optional[ast.Module]:
    try:
        return ast.parse(_python_of(src))
    except SyntaxError:
        return None


def _no_call(node) -> bool:
    return not any(isinstance(n, (ast.Call, ast.Await, ast.Yield, ast.YieldFrom)) for n in ast.walk(node))


def _setup_stmt(stmt) -> bool:
    """A statement a later piece may re-run: an import, a ``def``/``class``, an assignment with no call, a string."""
    if isinstance(stmt, (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return True
    if isinstance(stmt, (ast.Assign, ast.AnnAssign)) and stmt.value is not None and _no_call(stmt.value):
        return True
    return isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant)


def is_setup_cell(cell) -> bool:
    """True for a cell a later piece re-runs: an install, restart or ``%run`` cell, or definitions only."""
    if cell.get("cell_type") != "code":
        return False
    src = cell_source(cell)
    if _MAGIC_SETUP.search(src) or _RESTART.search(src):
        return True
    if src.lstrip().startswith("%"):  # %sql, %sh, %md ...: not Python setup
        return False
    tree = _parse(src)
    if tree is None or not tree.body:
        return False
    return all(_setup_stmt(stmt) for stmt in tree.body)


def auto_prelude(cells: list, rng: range) -> list[int]:
    """The setup cells before the range (0-based indices)."""
    return [i for i in range(rng.start) if is_setup_cell(cells[i])]


def _bound(nodes) -> set[str]:
    names: set[str] = set()
    for n in nodes:
        if isinstance(n, ast.Name) and isinstance(n.ctx, (ast.Store, ast.Del)):
            names.add(n.id)
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(n.name)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            for a in n.names:
                names.add((a.asname or a.name).split(".")[0])
        elif isinstance(n, ast.arg):
            names.add(n.arg)
    return names


def _defined(tree: ast.AST) -> set[str]:
    """Every name the cell binds anywhere (function arguments and locals too): what a kept cell provides."""
    return _bound(ast.walk(tree))


def _module_scope(tree: ast.AST):
    """The nodes of the cell's module scope: function and class bodies left out (their locals are not the session's)."""
    stack = list(ast.iter_child_nodes(tree))
    while stack:
        n = stack.pop()
        yield n
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        if isinstance(n, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
            continue
        stack.extend(ast.iter_child_nodes(n))


def _session_defined(tree: ast.AST) -> set[str]:
    """The names a skipped cell would have left in the session (module scope only)."""
    return _bound(_module_scope(tree))


def _loaded(tree: ast.AST) -> set[str]:
    return {n.id for n in ast.walk(tree) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}


def missing_names(cells: list, kept: Iterable[int], rng: range) -> dict[str, list[int]]:
    """Names the kept cells use that only a skipped code cell defines: {name: [0-based indices of the cells defining it]}.

    The piece's cells and the prelude cells outside the automatic setup (``--prelude``) count: both re-run in the
    piece. A setup cell's reads are left out (its names usually come from a ``%run`` notebook)."""
    kept = set(kept)
    defined_kept: set[str] = set()
    used: set[str] = set()
    for i in sorted(kept):
        tree = _parse(cell_source(cells[i])) if cells[i].get("cell_type") == "code" else None
        if tree is not None:
            defined_kept |= _defined(tree)
            if i in rng or not is_setup_cell(cells[i]):
                used |= _loaded(tree)
    out: dict[str, list[int]] = {}
    for i, cell in enumerate(cells):
        if i in kept or cell.get("cell_type") != "code":
            continue
        tree = _parse(cell_source(cell))
        if tree is None:
            continue
        for name in (_session_defined(tree) & used) - defined_kept:
            out.setdefault(name, []).append(i)
    return out


CARRIED_PREFIX = "# --- piece setup carried from skipped cells"


def _inputs(stmt) -> set[str]:
    """The names a call-free assignment reads from the session (its own lambda arguments and comprehension targets
    left out). Imports and ``def``/``class`` read nothing when they run."""
    if not isinstance(stmt, (ast.Assign, ast.AnnAssign)) or stmt.value is None:
        return set()
    return _loaded(stmt.value) - _defined(stmt.value)


def carried_statements(cells: list, missing: dict[str, list[int]],
                       available: Iterable[str] = ()) -> tuple[Optional[dict[int, dict]], set[str]]:
    """Cells of the setup STATEMENTS (imports, defs, call-free assignments) of skipped cells that bind a missing name,
    and the names they bind: {0-based source cell: its carried cell}, so each runs where its cell was (before a later
    prelude cell that reads it). A compute cell often starts with its imports or a flag; those lines carry, the compute
    does not. ``available``: the names the kept cells bind. A statement that reads a name neither those cells, an
    earlier carried statement nor the session provides (``wkb = geom.wkb`` after ``geom = make()``) is not carried,
    so its name stays missing and the piece is refused rather than failing with a NameError on the compute."""
    wanted = set(missing)
    have = set(available) | _SESSION_GLOBALS
    candidates = []  # (1-based cell, source segment, names bound, names read), in notebook order
    for i in sorted({i for where in missing.values() for i in where}):
        src = _python_of(cell_source(cells[i]))
        tree = _parse(cell_source(cells[i]))
        if tree is None:
            continue
        for stmt in tree.body:
            if not _setup_stmt(stmt):
                continue
            names = _bound(_module_scope(ast.Module(body=[stmt], type_ignores=[]))) | (
                {stmt.name} if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else set())
            seg = ast.get_source_segment(src, stmt)
            if seg:
                candidates.append((i + 1, seg, names, _inputs(stmt)))
    grown = True
    while grown:  # a carried line also carries the setup lines of those cells it reads (``SEP = os.sep`` -> ``import os``)
        before = len(wanted)
        for _, _, names, needs in candidates:
            if names & wanted:
                wanted |= needs - have
        grown = len(wanted) != before
    by_cell: dict[int, list[str]] = {}  # 0-based source cell -> its carried lines
    covered: set[str] = set()
    for cell_no, seg, names, needs in candidates:
        if names & wanted and needs <= have:
            by_cell.setdefault(cell_no - 1, []).append(seg)
            covered |= names
            have |= names
    covered &= set(missing)
    if not by_cell:
        return None, set()
    return {i: {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [],
                "source": (f"{CARRIED_PREFIX} {i + 1} (the runner's; not in the notebook) ---\n"
                           + "\n".join(lines) + "\n").splitlines(keepends=True)}
            for i, lines in by_cell.items()}, covered


def select_cells(nb: dict, spec: str, prelude: Optional[str] = None) -> tuple[dict, range, list[int]]:
    """The notebook with only the piece ``spec`` and its prelude; returns (notebook, range, prelude indices).

    ``prelude``: the ``--prelude`` cells (1-based list) added to the automatic ones. A name the piece needs from a
    skipped cell is carried when a setup statement of that cell binds it (an import, a flag); ValueError when only
    the skipped cell's compute binds it."""
    cells = nb.get("cells", [])
    rng = parse_range(spec, len(cells))
    pre = sorted(set(auto_prelude(cells, rng)) | set(parse_cell_list(prelude, len(cells)) if prelude else []))
    pre = [i for i in pre if i not in rng]
    kept = sorted(set(pre) | set(rng))
    missing = missing_names(cells, kept, rng)
    available = set().union(*(_defined(t) for i in kept if cells[i].get("cell_type") == "code"
                              for t in [_parse(cell_source(cells[i]))] if t is not None))
    carried, covered = carried_statements(cells, missing, available) if missing else (None, set())
    missing = {k: v for k, v in missing.items() if k not in covered}
    if missing:
        detail = "; ".join(f"{name} (cell {', '.join(str(i + 1) for i in where)})" for name, where in sorted(missing.items()))
        add = ",".join(sorted({str(i + 1) for where in missing.values() for i in where}, key=int))
        raise ValueError(
            f"--cells {spec}: the piece uses names that only skipped cells define: {detail}. "
            f"Fix: --prelude {add} (re-runs those cells first), or start the piece earlier"
        )
    out = dict(nb)
    carried = carried or {}  # keyed by skipped cells only, so a carried cell never replaces a kept one
    out["cells"] = [carried.get(i, cells[i]) for i in sorted(set(pre) | set(carried) | set(rng))]
    return out, rng, pre
