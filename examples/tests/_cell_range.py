"""Run a notebook in pieces: a cell range, the setup cells it needs, and a check that nothing else is needed.

A piece is ``--cells A:B``: the cells numbered A to B, both included, counted from 1 over ALL the notebook's cells
(markdown too), as Databricks numbers them. The uploaded copy keeps the piece's cells and the PRELUDE: the cells
before A that set the session up. The state a later piece needs from an earlier one is carried the way the example
notebooks already carry it between sessions: their outputs are persisted tables and files, and the ``FORCE_*`` flags
(default False) make a cell reuse what exists instead of recomputing it. A piece therefore re-runs only cheap setup,
never the earlier piece's compute.

The automatic prelude is every code cell before A that is setup: a ``%pip``, ``%restart_python`` or ``%run`` cell, or
a cell of side-effect-free statements only: imports, ``def``/``class``, assignments of literals or simple expressions
(``CATALOG = "main"``, ``FORCE_RELOAD = False``, ``N = int(os.environ.get("N", 3))``), an ``if`` over those, and
``print``/logging calls of them (``print(f"DEMO={DEMO}")``). A cell that computes (a Spark action, a read, a write, any
other call) is not setup, and neither is a setup cell that reads a name only a skipped cell defines. When the piece then
uses a name that only a skipped cell defines, the piece is refused before anything is submitted, with the cell to add
through ``--prelude`` (``--prelude`` stays the manual override), unless the cell's setup statements bind it: those are
carried. A name counts as used when read as a variable, through ``globals().get("X")`` / ``globals()["X"]``, or by a
``%run`` notebook (parsed next to the notebook); the names a ``%run`` notebook defines count as defined.

Stdlib only.
"""

from __future__ import annotations

import ast
import builtins
import json
import re
from pathlib import Path
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


_PURE_CALLS = frozenset({
    "str", "repr", "int", "float", "bool", "len", "dict", "list", "tuple", "set", "frozenset", "range", "sorted",
    "min", "max", "abs", "round", "Path", "PurePath", "os.getenv", "os.environ.get", "os.path.join",
    "logging.getLogger", "os.path.basename", "os.path.dirname", "os.path.abspath", "os.path.splitext", "globals", "globals().get",
    "dbutils.widgets.get", "dbutils.widgets.text", "dbutils.widgets.dropdown", "dbutils.widgets.combobox",
    "dbutils.widgets.multiselect"})  # reads and flag definitions: no Spark job, no write
_STR_METHODS = frozenset({"format", "join", "upper", "lower", "strip", "replace", "split", "title"})
_LOG_ROOTS = frozenset({"logging", "logger", "log", "LOG", "LOGGER"})
_LOG_METHODS = frozenset({"debug", "info", "warning", "warn", "error", "critical", "exception"})


def _dotted(func) -> Optional[str]:
    """``os.path.join`` for a Name/Attribute chain; ``globals().get`` for that call form; else None."""
    parts: list[str] = []
    while isinstance(func, ast.Attribute):
        parts.append(func.attr)
        func = func.value
    if isinstance(func, ast.Name):
        parts.append(func.id)
    elif (isinstance(func, ast.Call) and isinstance(func.func, ast.Name) and func.func.id == "globals"
          and not func.args and parts):
        parts.append("globals()")
    else:
        return None
    return ".".join(reversed(parts))


def _pure_call(call: ast.Call) -> bool:
    name = _dotted(call.func)
    if name in _PURE_CALLS:
        return True
    f = call.func  # "...{}".format(x), ", ".join(names)
    return (isinstance(f, ast.Attribute) and f.attr in _STR_METHODS
            and (isinstance(f.value, ast.JoinedStr) or (isinstance(f.value, ast.Constant) and isinstance(f.value.value, str))))


def _pure(node) -> bool:
    """No side effect: no call but the reads and conversions in ``_PURE_CALLS``, no await or yield."""
    for n in ast.walk(node):
        if isinstance(n, (ast.Await, ast.Yield, ast.YieldFrom)):
            return False
        if isinstance(n, ast.Call) and not _pure_call(n):
            return False
    return True


def _log_stmt(stmt) -> bool:
    """``print(...)`` / ``logger.info(...)`` / ``warnings.warn(...)`` of side-effect-free arguments."""
    if not (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)):
        return False
    call = stmt.value
    name = _dotted(call.func) or ""
    root, _, attr = name.rpartition(".")
    if name != "print" and name != "warnings.warn" and not (root in _LOG_ROOTS and attr in _LOG_METHODS):
        return False
    return all(_pure(a) for a in call.args) and all(_pure(k.value) for k in call.keywords)


def _setup_stmt(stmt) -> bool:
    """A statement a later piece may re-run: an import, a ``def``/``class``, an assignment of a side-effect-free value,
    a string, a ``print``/logging call of those, or an ``if`` over those."""
    if isinstance(stmt, (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Pass)):
        return True
    if isinstance(stmt, (ast.Assign, ast.AnnAssign)) and stmt.value is not None and _pure(stmt.value):
        return True
    if isinstance(stmt, ast.If):
        return _pure(stmt.test) and all(_setup_stmt(s) for s in stmt.body + stmt.orelse)
    return _log_stmt(stmt) or (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant))


def is_setup_cell(cell) -> bool:
    """True for a cell a later piece re-runs: an install, restart or ``%run`` cell, or side-effect-free statements only."""
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


def auto_prelude(cells: list, rng: range, base_dir: Optional[Path] = None) -> list[int]:
    """The setup cells before the range (0-based indices). A setup cell that reads a name only a skipped cell defines
    is not kept (it would raise NameError in the piece): its names are then carried or refused like any skipped cell's."""
    avail = set(_SESSION_GLOBALS)
    skipped: set[str] = set()
    out: list[int] = []
    for i in range(rng.start):
        cell = cells[i]
        if cell.get("cell_type") != "code":
            continue
        src = cell_source(cell)
        tree = _parse(src)
        bound = _session_defined(tree) if tree is not None else set()
        if not is_setup_cell(cell):
            skipped |= bound
            continue
        if _MAGIC_SETUP.search(src) or _RESTART.search(src):
            out.append(i)
            avail |= bound | _run_notebooks(src, base_dir)[0]
            continue
        if tree is not None and ((_eager_loads(tree) - bound) & skipped) - avail:
            skipped |= bound
            continue
        out.append(i)
        avail |= bound
    return out


def _is_globals_call(n) -> bool:
    return isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "globals" and not n.args


def _const_str(n) -> Optional[str]:
    return n.value if isinstance(n, ast.Constant) and isinstance(n.value, str) else None


def _globals_key(n) -> Optional[str]:
    """``X`` for ``globals().get("X", ...)``, ``globals()["X"]``, ``"X" in globals()``; else None."""
    if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in ("get", "pop", "setdefault") \
            and _is_globals_call(n.func.value) and n.args:
        return _const_str(n.args[0])
    if isinstance(n, ast.Subscript) and _is_globals_call(n.value):
        return _const_str(n.slice)
    if isinstance(n, ast.Compare) and len(n.ops) == 1 and isinstance(n.ops[0], (ast.In, ast.NotIn)) \
            and _is_globals_call(n.comparators[0]):
        return _const_str(n.left)
    return None


def _bound(nodes) -> set[str]:
    names: set[str] = set()
    for n in nodes:
        if isinstance(n, ast.Name) and isinstance(n.ctx, (ast.Store, ast.Del)):
            names.add(n.id)
        elif isinstance(n, ast.Subscript) and isinstance(n.ctx, ast.Store) and _globals_key(n) is not None:
            names.add(_globals_key(n))  # globals()["X"] = ...
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
    """The names read as variables or through ``globals().get("X")`` / ``globals()["X"]`` / ``"X" in globals()``."""
    out = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
    for n in ast.walk(tree):
        if not (isinstance(n, ast.Subscript) and isinstance(n.ctx, ast.Store)) and _globals_key(n) is not None:
            out.add(_globals_key(n))
    return out


def _eager_loads(tree: ast.AST) -> set[str]:
    """The names read when the cell runs: ``def``/``class``/lambda bodies left out (read later, if called)."""
    out: set[str] = set()
    stack = [tree]
    while stack:
        n = stack.pop()
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            stack.extend(n.decorator_list)
            stack.extend(d for d in n.args.defaults + n.args.kw_defaults if d is not None)
            continue
        if isinstance(n, (ast.ClassDef, ast.Lambda)):
            continue
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load):
            out.add(n.id)
        elif not (isinstance(n, ast.Subscript) and isinstance(n.ctx, ast.Store)) and _globals_key(n) is not None:
            out.add(_globals_key(n))
        stack.extend(ast.iter_child_nodes(n))
    return out


_RUN_LINE = re.compile(r"^\s*%run\s+(\S+)", re.M)


def _run_notebooks(src: str, base_dir: Optional[Path], _seen: Optional[set] = None) -> tuple[set[str], set[str]]:
    """(names defined, names read) by the notebooks a cell ``%run``s, parsed relative to ``base_dir`` (the parent
    notebook's folder). A notebook that cannot be found or parsed adds nothing."""
    defined: set[str] = set()
    read: set[str] = set()
    if base_dir is None:
        return defined, read
    seen = _seen if _seen is not None else set()
    for target in _RUN_LINE.findall(src):
        ref = Path(target.strip("\"'"))
        names = [ref.name] if ref.is_absolute() else [str(ref)]
        found = next((c for n in names for c in (base_dir / n, base_dir / (n + ".ipynb"), base_dir / (n + ".py"))
                      if c.is_file()), None)
        if found is None or found in seen:
            continue
        seen.add(found)
        try:
            if found.suffix == ".ipynb":
                srcs = [cell_source(c) for c in json.loads(found.read_text()).get("cells", []) if c.get("cell_type") == "code"]
            else:
                srcs = [found.read_text()]
        except (OSError, ValueError):
            continue
        nb_def: set[str] = set()
        nb_read: set[str] = set()
        for text in srcs:
            tree = _parse(text)
            if tree is not None:
                nb_def |= _session_defined(tree)
                nb_read |= _loaded(tree) - _defined(tree)
            d, r = _run_notebooks(text, found.parent, seen)  # a %run inside the %run notebook
            nb_def |= d
            nb_read |= r
        defined |= nb_def
        read |= nb_read - nb_def
    return defined, read


def missing_names(cells: list, kept: Iterable[int], rng: range,
                  base_dir: Optional[Path] = None) -> dict[str, list[int]]:
    """Names the kept cells use that only a skipped code cell defines: {name: [0-based indices of the cells defining it]}.

    The piece's cells and the prelude cells outside the automatic setup (``--prelude``) count: both re-run in the
    piece. A setup cell's reads are left out (its names usually come from a ``%run`` notebook). A kept ``%run``
    notebook (found under ``base_dir``) counts both ways, for the skipped cells BEFORE it only: the names it reads
    (``globals().get("X")`` included) are used by the piece, and the names it defines are defined (it runs after
    them). A skipped cell AFTER the ``%run`` overrides what it defined, so that cell still counts."""
    kept = set(kept)
    defined_kept: set[str] = set()
    used: set[str] = set()
    run_reads: list[tuple[int, set[str]]] = []  # (the %run cell, names its notebooks read)
    run_defs: list[tuple[int, set[str]]] = []  # (the %run cell, names its notebooks define)
    for i in sorted(kept):
        if cells[i].get("cell_type") != "code":
            continue
        src = cell_source(cells[i])
        run_def, run_read = _run_notebooks(src, base_dir)
        if run_def:
            run_defs.append((i, run_def))
        if run_read:
            run_reads.append((i, run_read))
        tree = _parse(src)
        if tree is not None:
            defined_kept |= _defined(tree)
            if i in rng or not is_setup_cell(cells[i]):
                used |= _loaded(tree)
    out: dict[str, list[int]] = {}
    for i, cell in enumerate(cells[: rng.stop]):  # a cell after the piece never ran before it in the notebook
        if i in kept or cell.get("cell_type") != "code":
            continue
        tree = _parse(cell_source(cell))
        if tree is None:
            continue
        used_here = used | {n for run_i, names in run_reads if i < run_i for n in names}
        run_defined = {n for run_i, names in run_defs if i < run_i for n in names}
        for name in (_session_defined(tree) & used_here) - defined_kept - run_defined:
            out.setdefault(name, []).append(i)
    return out


CARRIED_PREFIX = "# --- piece setup carried from skipped cells"


def _inputs(stmt) -> set[str]:
    """The names a side-effect-free assignment or ``if`` reads from the session (its own lambda arguments, comprehension
    targets and names it assigns left out). Imports and ``def``/``class`` read nothing when they run."""
    if isinstance(stmt, (ast.Assign, ast.AnnAssign)) and stmt.value is not None:
        node = stmt.value
    elif isinstance(stmt, ast.If):
        node = stmt
    else:
        return set()
    return _loaded(node) - _defined(node)


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


def select_cells(nb: dict, spec: str, prelude: Optional[str] = None,
                 base_dir: Optional[Path] = None) -> tuple[dict, range, list[int]]:
    """The notebook with only the piece ``spec`` and its prelude; returns (notebook, range, prelude indices).

    ``prelude``: the ``--prelude`` cells (1-based list) added to the automatic ones. A name the piece needs from a
    skipped cell is carried when a setup statement of that cell binds it (an import, a flag); ValueError when only
    the skipped cell's compute binds it."""
    cells = nb.get("cells", [])
    rng = parse_range(spec, len(cells))
    pre = sorted(set(auto_prelude(cells, rng, base_dir)) | set(parse_cell_list(prelude, len(cells)) if prelude else []))
    pre = [i for i in pre if i not in rng]
    kept = sorted(set(pre) | set(rng))
    missing = missing_names(cells, kept, rng, base_dir)
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
