"""What a change touches, for gbx:test:databricks' ``affected`` pass: the test files and the family oracle tables a diff reaches
through the import graph of the commit under test. Pure Python (``ast`` and git only): it reads the blobs of the commit, never
the working tree, and imports nothing of the package.

The graph: every module of ``python/geobrix/src`` (``databricks.labs.gbx.*``) and of ``python/geobrix/test`` (``test.*``), with
its imports (module level and inside functions), the top-level symbols it defines and the names each one refers to. A test file
also depends on the ``conftest.py`` of its folder and of each folder above it. The graph is cached per commit.

From a changed file to the tests:

* a test file selects itself; a ``conftest.py`` the tests of its folder; a helper of the test tree the tests that import it;
* a source module selects the test files that import it, directly or through other modules. A module most of the bank reaches (a
  package ``__init__``, a loader, a module such as ``pyvx/functions.py`` that every vector test imports) is followed by symbol: only
  the importers that use the symbols that changed, or that use the names it took from a changed module, go on. A changed module of
  that kind whose change is not in a symbol (module-level code) selects the tests that import it directly;
* a Rust kernel (``rust/gbxrs/src/*.rs``) maps to the functions and classes the extension exports whose code, or code they call,
  changed; then to the Python modules that call those exports (the ``_rs`` call sites), followed as above;
* an oracle table module (``bench/oracle_tables/*``) maps to its tables, run as ``GBX_ORACLE_TABLES`` parts of the family harness;
* the manifest (``functions.json``) maps the rows that changed to their tables, and to the tests that name those rows; a function
  whose light code a change reaches maps the same way (its row's ``module:symbol`` references);
* ``scripts/**`` and ``examples/tests/**`` map to the ``scripts/ci`` tests that name the file, or a file that names it;
* the CI workflow and actions, the package's ``pyproject.toml`` and its requirements select the whole bank (and trip the guard).

Each selected file and table keeps the reason it was selected: the changed file and the import chain.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

import _test_shards as ts

GRAPH_VERSION = 1
SRC_ROOT = "python/geobrix/src"
TEST_ROOT = "python/geobrix/test"
GBX = "databricks.labs.gbx"
ORACLE_PKG = f"{GBX}.bench.oracle_tables"
ORACLES = f"{GBX}.bench.oracles"
MANIFEST_JSON = f"{SRC_ROOT}/databricks/labs/gbx/manifest/functions.json"
FAMILY_FILE = "test/manager/test_family_cross_tier.py"
RUST_CRATE = "rust/gbxrs"
SCRIPT_DIRS = ("scripts/", "examples/tests/")
SCRIPTS_TESTS = "scripts/ci/"
# Changes that reach every test: the CI files, the package's build and requirements.
EVERYTHING = (ts.WORKFLOW, ts.LIGHT_ACTION, ts.HEAVY_ACTION, f"{ts.PKG}/pyproject.toml")
EVERYTHING_PREFIX = (".github/actions/", f"{ts.PKG}/requirements", f"{ts.PKG}/envs/")
# A module whose importers reach more than this share of the test files is followed by symbol, not as a whole.
HUB_SHARE = 0.10
# ``affected`` refuses (without --allow-broad) a selection over this share of the light bank's units (files and oracle tables).
BROAD_SHARE = 0.25
REF_REF = re.compile(r"(databricks\.labs\.gbx(?:\.\w+)+):(\w+)")
_MODULE = "<module>"  # the pseudo-symbol of module-level code that defines no name
ALL = None  # a state's filter that takes the whole module


# ---------------------------------------------------------------------------------------------- git


def _git(root: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, check=True).stdout


def blobs(root: Path, rev: str, paths: Iterable[str]) -> Dict[str, Optional[str]]:
    """The text of each path at ``rev`` (None when absent there), in one ``git cat-file --batch``."""
    paths = list(paths)
    if not paths:
        return {}
    res = subprocess.run(["git", "cat-file", "--batch"], cwd=root, input="".join(f"{rev}:{p}\n" for p in paths).encode(),
                         capture_output=True, check=True)
    out, data, i = {}, res.stdout, 0
    for p in paths:
        nl = data.index(b"\n", i)
        head = data[i:nl].decode()
        i = nl + 1
        if head.endswith(" missing"):
            out[p] = None
            continue
        size = int(head.rsplit(" ", 1)[1])
        out[p] = data[i:i + size].decode("utf-8", "replace")
        i += size + 1
    return out


def tracked(root: Path, rev: str, *prefixes: str) -> List[str]:
    return [ln for ln in _git(root, "ls-tree", "-r", "--name-only", rev, "--", *prefixes).splitlines() if ln]


# ---------------------------------------------------------------------------------------------- one module


def module_of(path: str) -> Optional[str]:
    """The dotted name of a module file of the graph: ``databricks.labs.gbx.pyvx._tin``, ``test.pyvx.test_tin_core``."""
    if not path.endswith(".py"):
        return None
    if path.startswith(SRC_ROOT + "/"):
        rel = path[len(SRC_ROOT) + 1:-3]
    elif path.startswith(TEST_ROOT + "/"):
        rel = path[len(ts.PKG) + 1:-3]
    else:
        return None
    parts = rel.split("/")
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _words(node) -> Set[str]:
    """The names a piece of code refers to: names, attributes, arguments, aliases and short strings."""
    out = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Name):
            out.add(n.id)
        elif isinstance(n, ast.Attribute):
            out.add(n.attr)
        elif isinstance(n, ast.arg):
            out.add(n.arg)
        elif isinstance(n, ast.alias):
            out.add((n.asname or n.name).split(".")[0])
        elif isinstance(n, ast.Constant) and isinstance(n.value, str) and len(n.value) <= 200:
            out.add(n.value)
    return out


def _defined(stmt) -> Optional[Set[str]]:
    """The top-level names a statement defines; None for module-level code that defines none (a call, a loop)."""
    if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return {stmt.name}
    if isinstance(stmt, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
        targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
        names = {n.id for t in targets for n in ast.walk(t) if isinstance(n, ast.Name)}
        return names or None
    if isinstance(stmt, (ast.Import, ast.ImportFrom)):
        return {(a.asname or a.name).split(".")[0] for a in stmt.names if a.name != "*"} or None
    if isinstance(stmt, (ast.If, ast.Try, ast.With)) or type(stmt).__name__ == "TryStar":
        names: Set[str] = set()
        bodies = [stmt.body, getattr(stmt, "orelse", []), getattr(stmt, "finalbody", [])]
        bodies += [h.body for h in getattr(stmt, "handlers", [])]
        for b in bodies:
            for s in b:
                names |= _defined(s) or set()
        return names or None
    if isinstance(stmt, ast.Expr) and isinstance(getattr(stmt, "value", None), ast.Constant):
        return set()  # a docstring
    return None


def parse_module(name: str, path: str, text: str) -> dict:
    """A module of the graph: its raw imports (resolved against the graph later), its symbols and their references."""
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError) as exc:
        return {"name": name, "path": path, "pkg": path.endswith("__init__.py"), "imports": [], "defs": {}, "words": [], "strings": [],
                "error": f"{type(exc).__name__}: {exc}"}
    defs: Dict[str, Set[str]] = {}
    imports = []
    for stmt in tree.body:
        names = _defined(stmt)
        refs = _words(stmt)
        if names is None:
            defs.setdefault(_MODULE, set()).update(refs)
            scope = [_MODULE]
        else:
            for n in names:
                defs.setdefault(n, set()).update(refs - {n} if not isinstance(stmt, (ast.Import, ast.ImportFrom)) else refs)
            scope = sorted(names)
        for n in ast.walk(stmt):
            if isinstance(n, ast.Import):
                imports.append({"level": 0, "module": "", "names": [[a.name, a.asname] for a in n.names], "scope": scope, "from": False})
            elif isinstance(n, ast.ImportFrom):
                imports.append({"level": n.level or 0, "module": n.module or "", "names": [[a.name, a.asname] for a in n.names],
                                "scope": scope, "from": True})
    strings = sorted({n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str) and 3 <= len(n.value) <= 200})
    return {"name": name, "path": path, "pkg": path.endswith("__init__.py"), "imports": imports,
            "defs": {k: sorted(v) for k, v in defs.items()}, "words": sorted(_words(tree)), "strings": strings}


def symbol_dumps(text: Optional[str]) -> Optional[Dict[str, str]]:
    """Each top-level symbol's code (an ``ast.dump``, positions left out); module-level code that defines no name under
    ``<module>``. None when the text does not parse."""
    if text is None:
        return {}
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return None
    out: Dict[str, List[str]] = {}
    for stmt in tree.body:
        names = _defined(stmt)
        d = ast.dump(stmt)
        for n in ([_MODULE] if names is None else sorted(names)):
            out.setdefault(n, []).append(d)
    return {k: "\n".join(v) for k, v in out.items()}


def changed_symbols(old: Optional[str], new: Optional[str]) -> Optional[Set[str]]:
    """The top-level symbols whose code differs between two versions of a module (added and removed ones too); None when the
    change cannot be put on symbols (module-level code changed, or a side does not parse)."""
    a, b = symbol_dumps(old), symbol_dumps(new)
    if a is None or b is None:
        return None
    out = {k for k in set(a) | set(b) if a.get(k) != b.get(k)}
    if _MODULE in out:
        return None
    return out


# ---------------------------------------------------------------------------------------------- the graph


def _resolve(mod: dict, imp: dict, known: Set[str]) -> List[dict]:
    """The edges of one import statement to modules of the graph: ``{"to", "names"}`` (names imported from it, [] = the module
    as a whole) and the symbols of the importer that hold the import (``scope``)."""
    out = []
    if not imp["from"]:
        for n, _ in imp["names"]:
            parts = n.split(".")
            for k in range(len(parts), 0, -1):
                cand = ".".join(parts[:k])
                if cand in known:
                    out.append({"to": cand, "names": [], "scope": imp["scope"]})
                    break
        return out
    if imp["level"]:
        pkg = mod["name"].split(".") if mod["pkg"] else mod["name"].split(".")[:-1]
        if imp["level"] > 1:
            pkg = pkg[: len(pkg) - (imp["level"] - 1)]
        base = ".".join(pkg + ([imp["module"]] if imp["module"] else []))
    else:
        base = imp["module"]
    rest = []
    for n, _ in imp["names"]:
        sub = f"{base}.{n}" if base else n
        if n != "*" and sub in known:
            out.append({"to": sub, "names": [], "scope": imp["scope"]})
        else:
            rest.append(n)
    if rest:
        target = base
        while target and target not in known:  # `from a.b import c` where a.b is not ours: nothing; a sub-package: the nearest module
            target = target.rpartition(".")[0]
        if target == base and target in known:
            out.append({"to": target, "names": [] if "*" in rest else rest, "scope": imp["scope"]})
    return out


def _conftests(name: str, known: Set[str]) -> List[str]:
    """The conftest modules pytest loads for a test module: of its folder and of each folder above it, under ``test``."""
    parts = name.split(".")[:-1]
    out = []
    for k in range(len(parts), 0, -1):
        cand = ".".join(parts[:k] + ["conftest"])
        if cand in known and cand != name:
            out.append(cand)
    return out


def is_test(path: str) -> bool:
    return path.startswith(TEST_ROOT + "/") and ts._is_test_file(path)


def build_graph(root: Path, sha: str, cache_dir: Optional[Path] = None) -> dict:
    """The import graph of ``sha`` (cached in ``cache_dir/<sha>.json``)."""
    if cache_dir is not None:
        f = Path(cache_dir) / f"{sha}-v{GRAPH_VERSION}.json"
        try:
            g = json.loads(f.read_text())
            if g.get("version") == GRAPH_VERSION and g.get("sha") == sha:
                return g
        except (OSError, ValueError):
            pass
    paths = [p for p in tracked(root, sha, SRC_ROOT, TEST_ROOT) if p.endswith(".py")]
    texts = blobs(root, sha, paths)
    mods = {}
    for p in paths:
        name = module_of(p)
        if name and texts.get(p) is not None:
            mods[name] = parse_module(name, p, texts[p])
    known = set(mods)
    for m in mods.values():
        edges = []
        for imp in m.pop("imports"):
            edges += [e for e in _resolve(m, imp, known) if e["to"] != m["name"]]
        if m["path"].startswith(TEST_ROOT + "/") and not m["name"].endswith(".conftest"):
            edges += [{"to": c, "names": [], "scope": [_MODULE], "conftest": True} for c in _conftests(m["name"], known)]
        m["edges"] = edges
    scripts = {p: t for p, t in blobs(root, sha, [p for p in tracked(root, sha, *SCRIPT_DIRS) if not p.endswith((".png", ".ipynb"))]).items() if t is not None}
    g = {"version": GRAPH_VERSION, "sha": sha, "modules": mods, "scripts": scripts}
    if cache_dir is not None:
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        (Path(cache_dir) / f"{sha}-v{GRAPH_VERSION}.json").write_text(json.dumps(g))
    return g


class Graph:
    """Lookups over a built graph: who imports a module, how many test files reach it, a module's symbol closure."""

    def __init__(self, g: dict):
        self.g = g
        self.mods: Dict[str, dict] = g["modules"]
        self.by_path = {m["path"]: n for n, m in self.mods.items()}
        self.rev: Dict[str, List[Tuple[str, dict]]] = {}
        for n, m in self.mods.items():
            for e in m["edges"]:
                self.rev.setdefault(e["to"], []).append((n, e))
        self.words = {n: set(m["words"]) for n, m in self.mods.items()}
        self.tests = sorted(n for n, m in self.mods.items() if is_test(m["path"]))
        self._reach: Dict[str, int] = {}

    def is_test(self, name: str) -> bool:
        return is_test(self.mods[name]["path"])

    def reach(self, name: str) -> int:
        """How many test files import ``name``, directly or through any module (the whole-module closure)."""
        if name not in self._reach:
            seen, todo = {name}, [name]
            while todo:
                x = todo.pop()
                for i, _ in self.rev.get(x, ()):
                    if i not in seen:
                        seen.add(i)
                        todo.append(i)
            self._reach[name] = sum(1 for x in seen if self.is_test(x))
        return self._reach[name]

    def is_hub(self, name: str) -> bool:
        m = self.mods[name]
        return m["pkg"] or name.endswith(".conftest") or self.reach(name) > HUB_SHARE * max(1, len(self.tests))

    def closure(self, name: str, seeds: Iterable[str]) -> Set[str]:
        """``seeds`` and every top-level symbol of the module that refers to one of them, to a fixed point."""
        defs = self.mods[name]["defs"]
        out = set(seeds)
        grew = True
        while grew:
            grew = False
            for sym, refs in defs.items():
                if sym != _MODULE and sym not in out and out.intersection(refs):
                    out.add(sym)
                    grew = True
        return out

    def users(self, name: str, needles: Iterable[str], substring: bool = False) -> Optional[Set[str]]:
        """The symbols of ``name`` whose code names one of ``needles`` (as a word, or inside a string when ``substring``);
        None (the whole module) when module-level code does."""
        needles = set(needles)
        hit = set()
        for sym, refs in self.mods[name]["defs"].items():
            if needles.intersection(refs) or (substring and any(n in r for r in refs for n in needles)):
                hit.add(sym)
        if _MODULE in hit:
            return None
        return self.closure(name, hit)


# ---------------------------------------------------------------------------------------------- the walk


@dataclass
class Selection:
    files: Dict[str, List[str]] = field(default_factory=dict)  # test file (from python/geobrix, or scripts/ci/... from the root) -> reasons
    tables: Dict[str, List[str]] = field(default_factory=dict)  # oracle table -> reasons
    notes: List[str] = field(default_factory=list)
    everything: List[str] = field(default_factory=list)  # changed files that select the whole bank

    def add_file(self, f: str, why: str) -> None:
        self.files.setdefault(f, [])
        if why not in self.files[f]:
            self.files[f].append(why)

    def add_table(self, t: str, why: str) -> None:
        self.tables.setdefault(t, [])
        if why not in self.tables[t]:
            self.tables[t].append(why)


def _short(name: str) -> str:
    return name[len(GBX) + 1:] if name.startswith(GBX + ".") else name


def _chain(chain: List[str]) -> str:
    """An import chain from the test to the changed module (``a -> b -> c``; a long one keeps its ends)."""
    names = [_short(x) for x in chain[::-1]]
    if len(names) > 6:
        names = names[:3] + [f"... {len(names) - 5} more"] + names[-2:]
    return " -> ".join(names)


def walk(G: Graph, seeds: List[Tuple[str, Optional[Set[str]], str]]) -> Tuple[Dict[str, Tuple[str, List[str]]], Dict[str, Optional[Set[str]]]]:
    """Follow the importers from ``(module, symbols or None for all, why)`` seeds. Returns the test modules reached (with the
    reason and the chain from the test to the seed) and the modules reached with the symbols reached (None: all of it)."""
    states: Dict[str, Optional[Set[str]]] = {}
    tests: Dict[str, Tuple[str, List[str]]] = {}
    todo: List[Tuple[str, Optional[Set[str]], str, List[str]]] = [(m, f, why, [m]) for m, f, why in seeds]
    while todo:
        mod, filt, why, chain = todo.pop(0)
        if mod in states:
            have = states[mod]
            if have is ALL or (filt is not ALL and filt <= have):
                continue
            new = ALL if filt is ALL else filt - have
            states[mod] = ALL if filt is ALL else have | filt
            filt = new
        else:
            states[mod] = None if filt is ALL else set(filt)
        if G.is_test(mod) and mod not in tests:
            tests[mod] = (why, chain)
        for imp, e in G.rev.get(mod, ()):
            if e.get("conftest"):
                used = ALL if filt is ALL else (G.words[imp] & filt)  # a test uses a conftest's fixtures by name
                if used is not ALL and not used:
                    continue
            elif filt is ALL:
                used = ALL
            else:
                used = (set(e["names"]) & filt) if e["names"] else (G.words[imp] & filt)
                if not used:
                    continue
            if G.is_test(imp) or not G.is_hub(imp):
                nxt: Optional[Set[str]] = ALL
            elif used is ALL:
                scope = set(e["scope"])
                nxt = ALL if _MODULE in scope else G.closure(imp, scope)
            else:
                hit = G.users(imp, used)
                nxt = ALL if hit is None else G.closure(imp, hit | (set(e["names"]) & used))
            todo.append((imp, nxt, why, chain + [imp]))
    return tests, states


# ---------------------------------------------------------------------------------------------- Rust


_RS_HEAD = re.compile(
    r"^(?:pub(?:\([^)]*\))?\s+)?(?:(?:async|unsafe|const|extern(?:\s+\"[^\"]*\")?)\s+)*"
    r"(?:(fn)\s+(\w+)|(struct|enum|trait|union)\s+(\w+)|(impl)(?:<[^{]*?>)?\s+(?:[\w:<>, ]+?\s+for\s+)?(\w+)|(mod)\s+(\w+))"
)


def _rs_clean(line: str) -> str:
    line = re.sub(r'"(?:\\.|[^"\\])*"', '""', line)
    line = re.sub(r"'(?:\\.|[^'\\])'", "''", line)
    return line.split("//", 1)[0]


def rust_items(text: str) -> List[dict]:
    """The top-level items of a Rust file: ``{"kind": fn|type|mod, "name", "start", "end"}`` (1-based lines; a struct and its
    ``impl`` blocks are one type). ``#[cfg(test)]`` modules are left out."""
    lines = text.splitlines()
    out, depth, i, cfg_test = [], 0, 0, False
    while i < len(lines):
        raw = _rs_clean(lines[i])
        s = raw.strip()
        if depth == 0 and s.startswith("#[cfg(test)]"):
            cfg_test = True
        m = _RS_HEAD.match(s) if depth == 0 else None
        if m:
            kind = "fn" if m.group(1) else "type" if (m.group(3) or m.group(5)) else "mod"
            name = m.group(2) or m.group(4) or m.group(6) or m.group(8)
            start, d, j, opened = i, 0, i, False
            while j < len(lines):
                c = _rs_clean(lines[j])
                d += c.count("{") - c.count("}")
                opened = opened or "{" in c
                if (opened and d <= 0) or (not opened and c.rstrip().endswith(";")):
                    break
                j += 1
            if not (kind == "mod" and cfg_test):
                out.append({"kind": kind, "name": name, "start": start + 1, "end": j + 1})
            cfg_test = False
            i = j + 1
            continue
        if s and not s.startswith("#"):
            cfg_test = False
        depth += raw.count("{") - raw.count("}")
        i += 1
    return out


def rust_exports(text: str) -> Dict[str, str]:
    """Python name -> Rust item of what the ``#[pymodule]`` adds (``wrap_pyfunction!``, ``add_class``, ``m.add("X", ...)``)."""
    out = {}
    for m in re.finditer(r"wrap_pyfunction!\(\s*(\w+)", text):
        out[m.group(1)] = m.group(1)
    for m in re.finditer(r"add_class::<\s*(\w+)\s*>", text):
        out[m.group(1)] = m.group(1)
    for m in re.finditer(r"\.add\(\s*\"(\w+)\"\s*,\s*[^;]*?<(\w+)>", text):
        out[m.group(1)] = m.group(2)
    return out


def _refers(body: str, item: dict) -> bool:
    n = re.escape(item["name"])
    if item["kind"] == "fn":
        return bool(re.search(rf"(?:\b{n}\s*\(|::{n}\b|\(\s*{n}\b)", body))
    return bool(re.search(rf"\b{n}\b", body))


def rust_changed_exports(files: Dict[str, Tuple[Optional[str], Set[int]]]) -> Tuple[Set[str], List[str]]:
    """The exported Python names a change to a crate reaches. ``files``: path -> (text at the commit, changed lines there; an
    empty set with a text = a new file). An item is reached when its lines changed or its code calls a reached item."""
    items = []
    for path, (text, lines) in files.items():
        if text is None:
            continue
        src = text.splitlines()
        for it in rust_items(text):
            it = dict(it, path=path, body="\n".join(src[it["start"] - 1:it["end"]]))
            it["changed"] = not lines or any(it["start"] <= ln <= it["end"] for ln in lines)
            items.append(it)
    reached = {id(it) for it in items if it["changed"]}
    grew = True
    while grew:
        grew = False
        for it in items:
            if id(it) in reached:
                continue
            if any(_refers(it["body"], o) for o in items if id(o) in reached and o is not it):
                reached.add(id(it))
                grew = True
    names = {it["name"] for it in items if id(it) in reached}
    exports: Dict[str, str] = {}
    for path, (text, _) in files.items():
        if text and "#[pymodule]" in text:
            exports.update(rust_exports(text))
    notes = [f"{p}: no item of it changed (its tests or comments only: cargo's tests cover them)" for p, (t, ln) in files.items()
             if t is not None and ln != {-1} and not any(it["path"] == p and it["changed"] for it in items)]
    return {py for py, rs in exports.items() if rs in names}, notes


def _rust_files(root: Path, base: str, sha: str, changed: List[str]) -> Dict[str, Tuple[Optional[str], Set[int]]]:
    """The crate's source files at ``sha`` with their changed lines (every file of the crate when its build files changed)."""
    crate_files = [p for p in tracked(root, sha, f"{RUST_CRATE}/src") if p.endswith(".rs")]
    texts = blobs(root, sha, crate_files)
    old = blobs(root, base, crate_files)
    build = any(f.startswith(RUST_CRATE + "/") and not f.startswith((f"{RUST_CRATE}/src/", f"{RUST_CRATE}/testdata/")) for f in changed)
    out = {}
    for p in crate_files:
        if build:
            out[p] = (texts[p], set())
        elif p in changed:
            if old.get(p) is None:
                out[p] = (texts[p], set())
                continue
            lines: Set[int] = set()
            for m in re.finditer(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", _git(root, "diff", "-U0", f"{base}", sha, "--", p), re.M):
                c, n = int(m.group(1)), int(m.group(2) if m.group(2) is not None else 1)
                lines |= set(range(c, c + n)) if n else {c, c + 1}
            out[p] = (texts[p], lines)
        else:
            out[p] = (texts[p], {-1})  # unchanged: reached only through the items it calls
    return out


# ---------------------------------------------------------------------------------------------- manifest and oracle tables


def _rows(text: Optional[str]) -> Dict[str, dict]:
    try:
        return {f["sql_name"]: f for f in (json.loads(text or "{}").get("functions") or []) if f.get("sql_name")}
    except ValueError:
        return {}


def row_tables(row: dict) -> List[str]:
    """The oracle tables a manifest row runs under in the family harness (its own; a row with a ``gdal`` block, SPEC_SE too)."""
    out = [row["oracle"]] if row.get("oracle") else []
    if row.get("gdal") is not None and "SPEC_SE" not in out:
        out.append("SPEC_SE")
    return out


def row_refs(row: dict) -> List[Tuple[str, str]]:
    return REF_REF.findall(json.dumps(row.get("levels") or {}))


def family_tables(shards: List[ts.Shard]) -> Dict[str, ts.Shard]:
    """Oracle table -> the CI family shard that runs it."""
    return {t: s for s in shards if s.oracle_tables for t in s.oracle_tables.split(",") if t}


# ---------------------------------------------------------------------------------------------- selection


def _script_mentions(G: Graph, changed: str) -> Tuple[Dict[str, str], List[str]]:
    """The ``scripts/ci`` tests that name ``changed`` or a script that names it (a file names another by its file name, or a
    Python module by its stem as a word). Returns test -> the chain, and notes."""
    scripts = G.g["scripts"]

    def names_it(text: str, f: str) -> bool:
        base = f.rsplit("/", 1)[-1]
        if base in text:
            return True
        stem = base[:-3] if base.endswith(".py") else ""
        return bool(stem) and re.search(rf"\b{re.escape(stem)}\b", text) is not None

    reached = {changed: [changed]}
    todo = [changed]
    while todo:
        f = todo.pop(0)
        for p, t in scripts.items():
            if p not in reached and not (p.startswith(SCRIPTS_TESTS) and ts._is_test_file(p)) and names_it(t, f):
                reached[p] = reached[f] + [p]
                todo.append(p)
    out = {}
    for p, t in scripts.items():
        if p.startswith(SCRIPTS_TESTS) and ts._is_test_file(p) and "/" not in p[len(SCRIPTS_TESTS):]:
            if p == changed:
                out[p] = [p]
                continue
            for f, chain in reached.items():
                if names_it(t, f):
                    out[p] = [p] + chain[::-1]
                    break
    return out, []


def select(G: Graph, changed: List[str], shards: List[ts.Shard], *, old_text: Callable[[str], Optional[str]],
           new_text: Callable[[str], Optional[str]], rust: Optional[Callable[[List[str]], Dict[str, Tuple[Optional[str], Set[int]]]]] = None) -> Selection:
    """The test files and oracle tables ``changed`` reaches (see the module docstring), each with its reasons."""
    sel = Selection()
    tables_of = family_tables(shards)
    seeds: List[Tuple[str, Optional[Set[str]], str]] = []
    rows_changed: Dict[str, str] = {}
    rust_files: List[str] = []
    for f in changed:
        if f in EVERYTHING or f.startswith(EVERYTHING_PREFIX):
            sel.everything.append(f)
            continue
        if f.startswith(RUST_CRATE + "/"):
            rust_files.append(f)
            continue
        if f.startswith(("rust/",)):
            sel.notes.append(f"{f}: a crate the Python tests do not load (its tests are cargo's)")
            continue
        if f == MANIFEST_JSON:
            old, new = _rows(old_text(f)), _rows(new_text(f))
            for n in sorted(set(old) | set(new)):
                if old.get(n) != new.get(n):
                    rows_changed[n] = f"{f}: row {n} changed"
            continue
        if f.startswith(SCRIPT_DIRS):
            hits, notes = _script_mentions(G, f)
            for t, chain in hits.items():
                sel.add_file(t, f"{f}: " + (" <- ".join(chain) if len(chain) > 1 else "changed"))
            if not hits:
                sel.notes.append(f"{f}: no scripts/ci test names it")
            continue
        name = module_of(f)
        if name is not None and name in G.mods:
            path = G.mods[name]["path"]
            if is_test(path):
                seeds.append((name, ALL, f))  # itself, and the tests that import it
                continue
            if name.endswith(".conftest"):
                prefix = path.rsplit("/", 1)[0] + "/"
                for t in G.tests:
                    if G.mods[t]["path"].startswith(prefix):
                        _add_test(sel, G, t, f"{f}: the conftest of its folder", tables_of)
                continue
            if G.is_hub(name) or G.mods[name]["pkg"]:
                syms = changed_symbols(old_text(f), new_text(f))
                if syms is None:
                    direct = sorted({i for i, _ in G.rev.get(name, ()) if G.is_test(i)})
                    sel.notes.append(f"{f}: module-level code changed in a module {G.reach(name)} test file(s) reach; "
                                     f"selected the {len(direct)} that import it directly")
                    for t in direct:
                        _add_test(sel, G, t, f"{f}: imports it directly (module-level code changed)", tables_of)
                    continue
                if not syms:
                    sel.notes.append(f"{f}: no symbol changed (a comment or formatting)")
                    continue
                seeds.append((name, G.closure(name, syms), f"{f} ({', '.join(sorted(syms)[:6])}{' ...' if len(syms) > 6 else ''})"))
            else:
                seeds.append((name, ALL, f))
            continue
        if name is not None:
            sel.notes.append(f"{f}: not in the commit (deleted); its importers would fail to import, and are not selected for it")
            continue
        if f.startswith(ts.PKG + "/"):  # a data file of the package or of the tests: the modules that name it
            base = f.rsplit("/", 1)[-1]
            hit = 0
            for n in G.mods:
                if any(base in s for s in G.mods[n]["strings"]):
                    users = G.users(n, [base], substring=True)
                    seeds.append((n, ALL if users is None else users, f"{f} (named in {_short(n)})"))
                    hit += 1
            if not hit:
                sel.notes.append(f"{f}: no module names it")
            continue
        sel.notes.append(f"{f}: outside the Python test bank")
    if rust_files:
        files = rust(rust_files) if rust else {}
        exports, notes = rust_changed_exports(files)
        sel.notes += notes
        if exports:
            for n in G.mods:
                if G.words[n] & exports:
                    users = G.users(n, exports)
                    seeds.append((n, ALL if users is None else users, f"{', '.join(sorted(rust_files))} (exports {', '.join(sorted(exports))}; called in {_short(n)})"))
    tests, states = walk(G, seeds)
    for t, (why, chain) in tests.items():
        _add_test(sel, G, t, f"{why}: {_chain(chain)}" if len(chain) > 1 else f"{why}: changed", tables_of)
    # oracle tables reached through their modules, and the rows whose light code was reached
    for mod, filt in states.items():
        if mod.startswith(ORACLE_PKG + ".") or mod == ORACLES:
            for t in [x for x in G.mods[mod]["defs"] if x in tables_of and (filt is ALL or x in filt)]:
                sel.add_table(t, f"{_short(mod)} defines it")
    rows = _rows(new_text(MANIFEST_JSON))
    for n, row in rows.items():
        for mod, sym in row_refs(row):
            if mod in states and (states[mod] is ALL or sym in states[mod]):
                rows_changed.setdefault(n, f"its light code {_short(mod)}:{sym} is reached")
                break
    if rows_changed:
        names = set(rows_changed)
        for n, why in sorted(rows_changed.items()):
            for t in row_tables(rows.get(n) or {}):
                if t in tables_of:
                    sel.add_table(t, f"row {n}: {why}")
        for t in G.tests:
            m = G.mods[t]
            hit = names & set(m["words"]) or {n for n in names if any(n in s for s in m["strings"])}
            if hit:
                _add_test(sel, G, t, f"names row {sorted(hit)[0]} ({rows_changed[sorted(hit)[0]]})", tables_of)
    return sel


def _add_test(sel: Selection, G: Graph, t: str, why: str, tables_of: Dict[str, ts.Shard]) -> None:
    rel = G.mods[t]["path"][len(ts.PKG) + 1:]
    if rel == FAMILY_FILE:
        for tb in tables_of:
            sel.add_table(tb, why)
    else:
        sel.add_file(rel, why)


# ---------------------------------------------------------------------------------------------- shards


def light_units(shards: List[ts.Shard], root: Path) -> int:
    """The units of the light bank (its files, and the family file as its oracle tables)."""
    n = 0
    for s in shards:
        if s.kind not in ("light", "scripts") or s.local_only:
            continue
        n += len([t for t in s.oracle_tables.split(",") if t]) if s.oracle_tables else len(ts.files_of(s, root) or [])
    return n


def to_shards(sel: Selection, shards: List[ts.Shard], root: Path) -> Tuple[List[ts.Shard], List[str]]:
    """The selection as shards of the selected files only, one per CI shard group that runs them (its marker, pool and options),
    and one per family shard holding its selected tables. Returns the shards and the files no runnable shard covers."""
    runnable = [s for s in shards if not s.local_only]
    if sel.everything:
        return [s for s in runnable if s.kind in ("light", "heavy", "scripts")], []
    fn = ts.shard_of(root)
    groups: Dict[str, Tuple[ts.Shard, List[str]]] = {}
    lost = []
    for f in sorted(sel.files):
        if f.startswith(SCRIPTS_TESTS):
            home = next((s for s in runnable if s.kind == "scripts"), None)
            rel = f
        else:
            home = next((s for s in runnable if s.kind in ("light", "heavy") and not s.oracle_tables and ts._covers(s, f, fn)), None)
            rel = f
        if home is None:
            lost.append(f)
            continue
        groups.setdefault(home.group, (home, []))[1].append(rel)
    out = []
    for group, (home, files) in groups.items():
        dirs = sorted({f.split("/")[1] if f.startswith("test/") else f.rsplit("/", 1)[0] for f in files})
        label = group if dirs in ([group.split()[-1]], [group]) or group.startswith("heavy") else "+".join(dirs)  # bench 1/3 holds test/pyvx
        out.append(ts.Shard(name=f"{label} affected", kind=home.kind, paths=sorted(files), delta=home.delta, pool=home.pool,
                            marker=home.marker, subset=True))
    tables_of = family_tables(runnable)
    for s in runnable:
        if not s.oracle_tables:
            continue
        mine = [t for t in s.oracle_tables.split(",") if t and t in sel.tables]
        if mine:
            out.append(ts.Shard(name=f"{s.name} affected", kind=s.kind, paths=list(s.paths), ignore=list(s.ignore), part=s.part,
                                oracle_tables=",".join(mine), delta=s.delta, pool=s.pool, marker=s.marker, unsplittable=True, subset=True))
    unknown = sorted(t for t in sel.tables if t not in tables_of)
    lost += [f"table:{t}" for t in unknown]
    return out, lost


def affected(root: Path, base: str, sha: str, shards: List[ts.Shard], cache_dir: Optional[Path] = None) -> Tuple[Selection, Graph]:
    """The selection for the files ``sha`` changed since it left ``base`` (their merge base)."""
    mb = _git(root, "merge-base", base, sha).strip()
    changed = ts.changed_files(root, base, sha)
    G = Graph(build_graph(root, sha, cache_dir))
    py = [f for f in changed if f.endswith((".py", ".json"))]
    old, new = blobs(root, mb, py), blobs(root, sha, py)
    sel = select(G, changed, shards, old_text=lambda f: old.get(f) if f in old else blobs(root, mb, [f])[f],
                 new_text=lambda f: new.get(f) if f in new else blobs(root, sha, [f])[f],
                 rust=lambda fs: _rust_files(root, mb, sha, fs))
    sel.notes.insert(0, f"{len(changed)} file(s) changed between {base} (merge base {mb[:10]}) and {sha[:10]}")
    return sel, G


def explain_lines(sel: Selection, n_units: int, n_light_selected: int) -> List[str]:
    out = [f"affected: {len(sel.files)} test file(s) and {len(sel.tables)} oracle table(s); {n_light_selected} of the light bank's "
           f"{n_units} units ({100.0 * n_light_selected / max(1, n_units):.0f}%)"]
    for n in sel.notes:
        out.append(f"  note: {n}")
    for f in sel.everything:
        out.append(f"  {f}: selects the whole bank")
    for f, why in sorted(sel.files.items()):
        out.append(f"  {f}: {why[0]}" + (f" (and {len(why) - 1} more reason(s))" if len(why) > 1 else ""))
    for t, why in sorted(sel.tables.items()):
        out.append(f"  table {t}: {why[0]}" + (f" (and {len(why) - 1} more reason(s))" if len(why) > 1 else ""))
    return out


def light_selected(sel: Selection, shards: List[ts.Shard], root: Path) -> int:
    """How many units of the light bank the selection holds."""
    if sel.everything:
        return light_units(shards, root)
    fn = ts.shard_of(root)
    light = [s for s in shards if s.kind == "light" and not s.oracle_tables and not s.local_only]
    n = sum(1 for f in sel.files if f.startswith(SCRIPTS_TESTS) or any(ts._covers(s, f, fn) for s in light))
    return n + len(sel.tables)
