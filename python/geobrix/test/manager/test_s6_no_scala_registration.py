"""No code path registers SQL from Scala: session registration has one owner, the Python manager.

L1Installer is the manager's primitive (the manager calls it over py4j and decides every name); nothing
else in the JAR may put a function into a registry or run a registrar, and no product Python code may
call a Scala registrar. Code only: the docs and examples get the same check once they are rewritten.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
_SCALA = REPO / "src/main/scala"
_PYTHON = REPO / "python/geobrix/src"
_ALLOWED = {"com/databricks/labs/gbx/kernels/L1Installer.scala"}
_SCALA_PATTERNS = (
    r"functionRegistry\.(registerFunction|createOrReplaceTempFunction)",
    r"\bdef\s+register\s*\(\s*spark\b",
    r"\budf\.register(JavaFunction)?\(",
    r"\bRegistryDelegate\b",
    r'"register_ds"',
)
# A Python call into a Scala registrar: the register_ds data source, or a JVM `functions.register` through py4j.
# The light Python registrations (spark.udf.register / spark.udtf.register in the manager) are not Scala.
_PYTHON_PATTERNS = (
    r"""format\(\s*["']register_ds["']""",
    r"_jvm\.[\w.]*functions\.register\(",
)


def _hits(root, glob, patterns, allowed=frozenset()):
    out = []
    for path in root.rglob(glob):
        rel = path.relative_to(root).as_posix()
        if rel in allowed:
            continue
        text = path.read_text()
        out += [f"{rel}: {m.group(0)}" for p in patterns for m in re.finditer(p, text)]
    return out


def test_no_scala_sql_registration():
    assert _hits(_SCALA, "*.scala", _SCALA_PATTERNS, _ALLOWED) == []


def test_no_product_python_runs_a_scala_registrar():
    assert _hits(_PYTHON, "*.py", _PYTHON_PATTERNS) == []
