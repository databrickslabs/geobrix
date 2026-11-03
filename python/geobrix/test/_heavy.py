"""Heavy legs of parity tests for functions that have no L1 block in the manifest.

The JAR still holds the Catalyst companion of such a function (the decision sweep deletes it). A parity test that
compares the light function with that companion installs it here, under its own SQL name, through the manager's
primitive ``L1Installer.install`` (the deleted Scala ``register`` did the same for every companion), and then lets
the manager choose its level for every name, as ``databricks.labs.gbx.register`` does. The companions are the lines
of ``TestL1.scala``: when the sweep deletes a companion, its line goes and this helper follows.
"""

import functools
import re
from pathlib import Path

_TEST_L1 = (
    Path(__file__).resolve().parents[3]
    / "src/test/scala/com/databricks/labs/gbx/kernels/TestL1.scala"
)


def _companions():
    return re.findall(r'"([\w.]+)"', _TEST_L1.read_text().split("installAll")[0])


_SCALA = _TEST_L1.parents[7] / "main" / "scala"
_NAME = re.compile(r'override\s+def\s+name\s*:\s*String\s*=\s*"(gbx_[a-z0-9_]+)"')


@functools.lru_cache(maxsize=None)
def _sql_name(fqn):
    """The SQL name a companion declares, read from its Scala source (the JAR's classes are not visible to py4j's
    ``Class.forName``)."""
    package, _, simple = fqn.rpartition(".")
    for path in _SCALA.rglob("*.scala"):
        text = path.read_text()
        if f"package {package}" not in text:
            continue
        m = re.search(rf"^\s*object\s+{simple}\b", text, re.M)
        if m:
            found = _NAME.search(text, m.end())
            if found:
                return found.group(1)
    raise AssertionError(f"{fqn}: no companion with a SQL name in src/main/scala")


def install_jvm_scalars(spark, prefixes):
    """Install the JAR's companions whose SQL name starts with one of ``prefixes`` (the scalar function registry)."""
    installer = spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
    for fqn in _companions():
        name = _sql_name(fqn)
        if not name.startswith(tuple(prefixes)):
            continue
        result = installer.install(spark._jsparkSession, name, fqn)
        assert result == "ok", f"{fqn}: {result}"


def heavy_register(spark, prefixes):
    """The JAR's scalars first, then the manager's choice for every name under ``prefixes``."""
    from databricks.labs.gbx import manager

    install_jvm_scalars(spark, prefixes)
    return manager.register_family(spark, prefixes)
