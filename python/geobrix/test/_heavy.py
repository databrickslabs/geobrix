"""Heavy legs of parity tests for functions that have no L1 block in the manifest.

The JAR still holds the Catalyst companion of such a function (the decision sweep deletes it). A parity test that
compares the light function with that companion installs it here, under its own SQL name, through the manager's
primitive ``L1Installer.install`` (the deleted Scala ``register`` did the same for every companion), and then pins
every other name under the prefixes that has an L1 row to L1. The companions are the lines
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


_TOP_LEVEL = re.compile(
    r"^(?:object|class|case class|trait|sealed|abstract|final)\b", re.M
)


@functools.lru_cache(maxsize=None)
def _sql_name(fqn):
    """The SQL name a companion declares, read from its Scala source (the JAR's classes are not visible to py4j's
    ``Class.forName``). The file must declare exactly ``package <package>`` (not a sub-package) and the name is read
    from the body of exactly ``object <simple>``, up to the next top-level declaration.
    """
    package, _, simple = fqn.rpartition(".")
    pkg = re.compile(rf"^package\s+{re.escape(package)}\s*(?://.*)?$", re.M)
    for path in _SCALA.rglob("*.scala"):
        text = path.read_text()
        if not pkg.search(text):
            continue
        m = re.search(rf"^\s*(?:\w+\s+)*object\s+{re.escape(simple)}\b", text, re.M)
        if not m:
            continue
        nxt = _TOP_LEVEL.search(text, m.end())
        found = _NAME.search(text, m.end(), nxt.start() if nxt else len(text))
        if found:
            return found.group(1)
    raise AssertionError(f"{fqn}: no companion with a SQL name in src/main/scala")


def install_jvm_scalars(spark, prefixes):
    """Install the JAR's companions whose SQL name starts with one of ``prefixes`` (the scalar function registry);
    the SQL names installed."""
    installer = spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
    installed = []
    for fqn in _companions():
        name = _sql_name(fqn)
        if not name.startswith(tuple(prefixes)):
            continue
        result = installer.install(spark._jsparkSession, name, fqn)
        assert result == "ok", f"{fqn}: {result}"
        installed.append(name)
    return installed


def heavy_register(spark, prefixes):
    """The heavy leg of a parity test: the JAR's companions, then L1 for the other names under ``prefixes`` that have
    an L1 row. A name with no L1 row is served by its companion alone, and the manager never registers it again, so
    no later registration can put the light function over the companion just installed.
    """
    from databricks.labs.gbx import manager

    installed = set(install_jvm_scalars(spark, prefixes))
    names = [n for n in manager.names_at_level(prefixes, "L1") if n not in installed]
    return manager.register_family(spark, prefixes, only=names, level="L1")
