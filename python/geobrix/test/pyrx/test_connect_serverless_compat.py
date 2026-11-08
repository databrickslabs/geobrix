"""Spark Connect / Serverless compatibility guard for every shipped GeoBrix package.

Spark Connect (Serverless, standard access mode) has no JVM bridge (`spark._jvm`, `spark._jsc`,
`spark._jsparkSession`), no `sparkContext` or RDD API, no `SparkConf`, no runtime `spark.conf.set(...)`, and it loads
a JAR only as a session artifact. The light tier is a Serverless target, so shipped code must not reach for any of
those unless it is gated and written down.

The register `gbx/_compat/classic_only.toml` lists every shipped site that is allowed to:

* ``[[site]]`` entries: one per (file, enclosing function, API). The scan finds them in the syntax tree, so prose
  (docstrings, comments, error messages) never counts and a call with a string argument cannot hide.
* ``[[feature]]`` entries: classic-only behaviour no API pattern can see (a refusal on Serverless, a level that needs
  the JAR). Each must resolve to a real definition.

A forbidden API outside the register fails with file:line, and a register entry that matches no code fails as stale.
`bench/` ships in the wheel as a developer harness; product code never imports it, so it is out of scope.
"""

import ast
import tomllib
from dataclasses import dataclass
from pathlib import Path

import databricks.labs.gbx as gbx

_PKG = Path(gbx.__file__).resolve().parent
_REGISTER = _PKG / "_compat" / "classic_only.toml"
_DEV_ONLY = ("bench",)  # ships in the wheel as a developer harness; product code never imports it
# The Serverless product core: no entry of the register may sit here, whatever its gate.
_CORE = ("pyrx", "pyvx", "ds", "engine")

# Attribute names that Spark Connect does not have, by the label the register uses.
_ATTRS = {
    "sparkContext": "sparkContext",
    "rdd": ".rdd",
    "_jvm": "_jvm",
    "_jsc": "_jsc",
    "_jsparkSession": "_jsparkSession",
    "_jdf": "_jdf",
    "_sc": "_sc",
    "setConf": "setConf",
    "setSystemProperty": "setSystemProperty",
    # a JAR loaded or a Java UDF registered by the client: Connect-only entry points, and the JAR uses to track
    "addArtifact": "addArtifact",
    "registerJavaFunction": "registerJavaFunction",
    "registerJavaUDAF": "registerJavaUDAF",
}
_NAMES = ("SparkConf", "SparkContext")  # imported or referenced by name
_CONF_MUTATORS = ("set", "unset")  # spark.conf.set(...) / spark.conf.unset(...)
_JAR_TEXT = (
    "java_dependencies"  # the clause that puts a JAR behind a Unity Catalog function
)

_GATES = ("capability", "refusal", "guarded", "classic-only")
_COMPUTES = ("dedicated", "standard", "serverless", "warehouse")

_FIX = (
    "Fix: gate it behind a capability (a Connect check such as capabilities.is_connect) and add it to "
    "gbx/_compat/classic_only.toml, or use a Connect-safe API."
)


@dataclass(frozen=True)
class Finding:
    path: str  # relative to the gbx package, posix
    line: int
    api: str
    symbol: str  # dotted enclosing class/function names, "<module>" at module level
    text: str


class _Scanner(ast.NodeVisitor):
    def __init__(self, path: str, lines: list[str]):
        self.path, self.lines = path, lines
        self.stack: list[str] = []
        self.found: list[Finding] = []
        self.prose: set[int] = set()  # ids of docstring-position constants

    def _add(self, node: ast.AST, api: str) -> None:
        self.found.append(
            Finding(
                self.path,
                node.lineno,
                api,
                ".".join(self.stack) or "<module>",
                self.lines[node.lineno - 1].strip(),
            )
        )

    def _scoped(self, node) -> None:
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_FunctionDef = visit_AsyncFunctionDef = visit_ClassDef = _scoped

    def visit_Expr(self, node: ast.Expr) -> None:
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            self.prose.add(id(node.value))  # a docstring or a bare string statement
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in _ATTRS:
            self._add(node, _ATTRS[node.attr])
        elif (
            node.attr in _CONF_MUTATORS
            and isinstance(node.value, ast.Attribute)
            and node.value.attr == "conf"
        ):
            self._add(node, f"conf.{node.attr}")
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id in _NAMES:
            self._add(node, node.id)

    def visit_alias(self, node: ast.alias) -> None:
        if node.name in _NAMES:
            self._add(node, node.name)

    def visit_Call(self, node: ast.Call) -> None:
        f = node.func
        if (
            isinstance(f, ast.Name)
            and f.id in ("getattr", "hasattr")
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _ATTRS
        ):
            self._add(node, _ATTRS[node.args[1].value])
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        if (
            isinstance(node.value, str)
            and _JAR_TEXT in node.value
            and id(node) not in self.prose
        ):
            self._add(node, _JAR_TEXT)


def scan_source(source: str, path: str = "<test>") -> list[Finding]:
    """Every forbidden API in ``source``, found in its syntax tree (a string or a comment is never a hit)."""
    scanner = _Scanner(path, source.splitlines())
    scanner.visit(ast.parse(source))
    return scanner.found


def shipped_files():
    for p in sorted(_PKG.rglob("*.py")):
        rel = p.relative_to(_PKG)
        if "__pycache__" in rel.parts or rel.parts[0] in _DEV_ONLY:
            continue
        yield rel, p


def scan_package() -> list[Finding]:
    out: list[Finding] = []
    for rel, p in shipped_files():
        out += scan_source(p.read_text(), rel.as_posix())
    return out


def load_register(path: Path = _REGISTER) -> dict:
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def _covers(entry: dict, f: Finding) -> bool:
    """An entry covers a finding in its file, in its function (or a function nested in it), for its API."""
    sym = entry["symbol"]
    return (
        entry["path"] == f.path
        and f.api in entry["api"]
        and (f.symbol == sym or f.symbol.startswith(sym + "."))
    )


def check(findings: list[Finding], sites: list[dict]) -> tuple[list[str], list[str]]:
    """``(unregistered, stale)``: findings no entry covers, and (entry, api) pairs no finding matches."""
    unregistered = [
        f"{f.path}:{f.line} [{f.api}] in {f.symbol} -> {f.text}"
        for f in findings
        if not any(_covers(e, f) for e in sites)
    ]
    stale = [
        f"{e['path']}::{e['symbol']} [{api}]"
        for e in sites
        for api in e["api"]
        if not any(_covers({**e, "api": [api]}, f) for f in findings)
    ]
    return unregistered, stale


def _sites() -> list[dict]:
    return load_register()["site"]


# ---------------------------------------------------------------------------------------------------------------
# The product


def test_every_connect_incompatible_site_is_registered():
    unregistered, _ = check(scan_package(), _sites())
    assert not unregistered, (
        "shipped code reaches an API Spark Connect / Serverless does not have, and it is not in the register:\n  "
        + "\n  ".join(unregistered)
        + f"\n{_FIX}"
    )


def test_register_has_no_stale_entries():
    _, stale = check(scan_package(), _sites())
    assert not stale, (
        "register entries that match no code (the site moved, changed or was removed):\n  "
        + "\n  ".join(stale)
        + "\nFix: remove the entry from gbx/_compat/classic_only.toml, or correct its path / symbol / api."
    )


def _defined_names(tree: ast.AST) -> set[str]:
    """Dotted names of every class, function and module-level assignment in a file."""
    names: set[str] = set()

    def walk(node, prefix):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                dotted = prefix + child.name
                names.add(dotted)
                walk(child, dotted + ".")
            elif isinstance(child, (ast.Assign, ast.AnnAssign)) and not prefix:
                targets = (
                    child.targets if isinstance(child, ast.Assign) else [child.target]
                )
                names.update(t.id for t in targets if isinstance(t, ast.Name))

    walk(tree, "")
    return names


def test_register_entries_are_well_formed():
    reg = load_register()
    problems = []
    for kind in ("site", "feature"):
        for e in reg.get(kind, []):
            label = f"[{kind}] {e.get('path')}::{e.get('symbol')}"
            for key in ("path", "symbol", "computes", "gate", "why"):
                if not e.get(key):
                    problems.append(f"{label}: missing '{key}'")
            if kind == "site" and not e.get("api"):
                problems.append(f"{label}: missing 'api'")
            if e.get("gate") not in _GATES:
                problems.append(
                    f"{label}: gate must be one of {_GATES}, got {e.get('gate')!r}"
                )
            bad = [c for c in e.get("computes", []) if c not in _COMPUTES]
            if bad:
                problems.append(f"{label}: unknown computes {bad}; known {_COMPUTES}")
            if e.get("gate") in ("capability", "refusal") and not e.get("gate_detail"):
                problems.append(
                    f"{label}: a '{e['gate']}' gate must name it in 'gate_detail'"
                )
            if kind == "site" and e.get("path", "").split("/")[0] in _CORE:
                problems.append(
                    f"{label}: the Serverless product core takes no site entry"
                )
            file = _PKG / e.get("path", "")
            if not file.is_file():
                problems.append(f"{label}: no such file")
            elif kind == "feature" and e["symbol"] not in _defined_names(
                ast.parse(file.read_text())
            ):
                problems.append(f"{label}: symbol is not defined in the file")
    assert not problems, "malformed register:\n  " + "\n  ".join(problems)


def test_serverless_core_is_free_of_forbidden_apis():
    """The pyrx, pyvx, ds and engine packages (the light product) take no entry: any hit there is a failure."""
    hits = [
        f"{f.path}:{f.line} [{f.api}] -> {f.text}"
        for f in scan_package()
        if f.path.split("/")[0] in _CORE
    ]
    assert not hits, (
        "the Serverless product core must use no JVM bridge, Spark config mutation or JAR load:\n  "
        + "\n  ".join(hits)
        + "\nFix: use a Connect-safe API; this part of the package takes no register entry."
    )


def test_scan_covers_the_shipped_packages_and_leaves_the_bench_out():
    files = {rel.as_posix() for rel, _ in shipped_files()}
    for required in (
        "ds/raster.py",
        "ds/gtiff.py",
        "pyrx/functions.py",
        "pyvx/__init__.py",
        "engine/run.py",
        "manager/jvm.py",
        "manager/capabilities.py",
        "zarr/_write.py",
        "vectorx/file_read.py",
        "rasterx/helpers.py",
        "stac/client.py",
        "pygx/_h3.py",
        "sample/overture.py",
        "uc/ddl.py",
        "_native/_selfcheck.py",
    ):
        assert (
            required in files
        ), f"{required} not covered by the Connect / Serverless scan"
    assert not any(f.startswith("bench/") for f in files)


def test_no_shipped_module_imports_the_bench():
    """The bench is dev-only because nothing shipped needs it: a product import of it would make it product code."""
    importers = []
    for rel, p in shipped_files():
        for node in ast.walk(ast.parse(p.read_text())):
            names = []
            if isinstance(node, ast.ImportFrom):
                names = [node.module or ""] + [
                    f"{node.module}.{a.name}" for a in node.names
                ]
            elif isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            if any(n.startswith("databricks.labs.gbx.bench") for n in names):
                importers.append(f"{rel.as_posix()}:{node.lineno}")
    assert not importers, "shipped code imports the dev-only bench: " + ", ".join(
        importers
    )


# ---------------------------------------------------------------------------------------------------------------
# The guard itself


def test_scan_flags_each_forbidden_api():
    src = (
        "def f(spark, df):\n"
        "    spark.conf.set('k', 'v')\n"  # the call with string arguments the old line-based guard let through
        "    spark.conf.unset('k')\n"
        "    spark.sparkContext\n"
        "    df.rdd\n"
        "    spark._jvm\n"
        "    spark._jsc\n"
        "    spark._jsparkSession\n"
        "    getattr(spark, '_jvm', None)\n"
        "    spark.addArtifact('/x.jar')\n"
        "    spark.udf.registerJavaFunction('n', 'c', 'int')\n"
        "    from pyspark import SparkConf\n"
        "    return \"ENVIRONMENT (java_dependencies = '[]')\"\n"
    )
    assert {x.api for x in scan_source(src)} == {
        "conf.set",
        "conf.unset",
        "sparkContext",
        ".rdd",
        "_jvm",
        "_jsc",
        "_jsparkSession",
        "addArtifact",
        "registerJavaFunction",
        "SparkConf",
        "java_dependencies",
    }


def test_scan_ignores_prose():
    src = (
        '"""Module doc: no spark.conf.set, no sparkContext, no java_dependencies."""\n'
        "def f():\n"
        '    """Connect-safe: no .rdd / _jvm / conf.set / addArtifact."""\n'
        "    # spark._jvm would break on Serverless\n"
        '    raise ValueError("do not call spark.sparkContext or df.rdd")\n'
    )
    assert scan_source(src) == []


def test_a_planted_unregistered_call_fails_with_file_line_and_fix():
    planted = scan_source("def f(spark):\n    spark._jvm.x()\n", "zarr/planted.py")
    unregistered, stale = check(planted, [])
    assert unregistered == ["zarr/planted.py:2 [_jvm] in f -> spark._jvm.x()"]
    assert stale == []
    assert "Fix:" in _FIX and "classic_only.toml" in _FIX


def test_a_register_entry_that_matches_no_code_is_stale():
    planted = scan_source("def f(spark):\n    spark._jvm.x()\n", "a.py")
    entries = [
        {"path": "a.py", "symbol": "f", "api": ["_jvm", "_jsc"]},  # _jsc is not used
        {"path": "a.py", "symbol": "gone", "api": ["_jvm"]},  # no such function
    ]
    unregistered, stale = check(planted, entries)
    assert unregistered == []
    assert stale == ["a.py::f [_jsc]", "a.py::gone [_jvm]"]


def test_an_entry_covers_a_nested_function_of_its_symbol():
    src = "def install(spark):\n    def inner():\n        return spark._jvm\n"
    entries = [{"path": "m.py", "symbol": "install", "api": ["_jvm"]}]
    assert check(scan_source(src, "m.py"), entries) == ([], [])
