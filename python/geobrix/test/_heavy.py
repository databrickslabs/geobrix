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
    r"^(?:(?:private(?:\[[\w.]+\])?|protected|implicit|sealed|abstract|final|case)\s+)*"
    r"(?:package\s+object|object|class|trait)\b",
    re.M,
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


def _point_jvm_at_payload(spark):
    """Name the wheel payload's natives to the JVM, as the registrar does (``manager.jvm``, which this helper does not
    import: it loads rasterio, which the heavyweight env lacks). A node with no system GDAL JNI (a Databricks pool
    cluster) has no other GDAL; a JVM already on ``GBX_NATIVE_ROOT``'s payload is pointed at that same one.
    """
    from databricks.labs.gbx import _native

    d = _native.native_dir()
    key = "spark.databricks.labs.gbx.native.dir"  # GDALManager.NativeDirKey
    if d and spark.conf.get(key, None) != d:
        spark.conf.set(key, d)


def install_jvm_scalars(spark, prefixes, names=None):
    """Install the JAR's companions whose SQL name starts with one of ``prefixes`` (the scalar function registry),
    or, with ``names``, only those named; the SQL names installed."""
    _point_jvm_at_payload(spark)
    installer = spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
    installed = []
    for fqn in _companions():
        name = _sql_name(fqn)
        if not name.startswith(tuple(prefixes)) or (names and name not in names):
            continue
        result = installer.install(spark._jsparkSession, name, fqn)
        assert result == "ok", f"{fqn}: {result}"
        installed.append(name)
    if installed:
        # the session now holds the JVM companion under each name: record it as L1, so a later generated-wrapper
        # `ensure` and a grid-dir re-registration leave it where it is
        from databricks.labs.gbx import _api

        _api._done(spark).update(installed)
        _api.chosen(spark).update({n: ("L1", "-") for n in installed})
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


def light_stack_available():
    """Whether the light packages (pandas, rasterio) import here. The heavyweight CI env does not have them: a test of
    a function whose only form needs them (a function with no L1 row) skips there
    with ``pytest.mark.skipif(not light_stack_available(), ...)``.
    """
    try:
        import pandas  # noqa: F401
        import rasterio  # noqa: F401
    except ImportError:
        return False
    return True


def forget_generators(spark, prefix="gbx_rst_"):
    """Take the generators out of a session another module of the same pytest process registered at the heavy level.

    A module that calls ``heavy_register`` leaves each generator under ``prefix`` installed as a heavy table function
    (its row shape is the tile struct) and marked done, so a later module that calls the same generators for the light
    table function's columns gets the heavy one, or, for a companion installed as a scalar, finds
    ``UNRESOLVABLE_TABLE_VALUED_FUNCTION``. The names are dropped and unmarked here: the first call registers the level
    the manager picks.
    """
    from databricks.labs.gbx import _api, manifest

    for name, fn in manifest.load().functions.items():
        if fn.kind == "generator" and name.startswith(prefix):
            spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {name}")
            _api._done(spark).discard(name)
            _api.chosen(spark).pop(name, None)


def pin_l1(spark, names):
    """Register exactly ``names`` at the heavy (Catalyst) level, and nothing else of their families.

    A heavy-dir fixture uses this instead of registering a whole family: a family also holds names whose only form
    is the light one, and installing those imports the light packages (pandas, rasterio), which the heavyweight CI
    env does not have. A scalar's companion goes in as it is (it needs no light kernel behind it, which the
    manager's own L1 install of a virtual-tile row does); a generator, and a scalar with no companion, take the
    manager's L1 row. A name with neither belongs in a test that skips without the light packages.
    """
    from databricks.labs.gbx import manager, manifest

    fns = manifest.load().functions
    names = [n.lower() for n in names]
    scalars = {n for n in names if fns[n].kind != "generator"}
    installed = (
        set(install_jvm_scalars(spark, ("gbx_",), names=scalars)) if scalars else set()
    )
    rest = [n for n in names if n not in installed]
    if rest:
        manager.register_family(spark, ("gbx_",), only=rest, level="L1")
    return names
