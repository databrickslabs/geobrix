"""The heavy-CI-env plugin blocks what requirements-ci.txt lacks; this fails when the two drift apart.

The blocked set is derived from the locks (python/geobrix/test/_plugins/heavy_ci_env.py). The tracked list below
is the part of it a heavy-dir test is known to depend on being absent: if requirements-ci.txt gains one of these, or
the light lock drops one, the heavy tests must be re-checked on purpose, so the list changes with them.
"""

import importlib.util

from conftest import REPO_ROOT

PLUGIN = REPO_ROOT / "python" / "geobrix" / "test" / "_plugins" / "heavy_ci_env.py"

# The light packages the heavy tests must not see (distribution names).
ABSENT_FROM_HEAVY = {
    "pandas", "pyarrow", "rasterio", "shapely", "h3", "quadbin", "scipy", "pmtiles", "pyproj", "affine",
}  # fmt: skip
# What the heavy env runs on: blocking any of these would break pytest or Spark.
PRESENT_IN_HEAVY = {"numpy", "pyspark", "py4j", "pytest", "pluggy"}


def _plugin():
    spec = importlib.util.spec_from_file_location("heavy_ci_env", PLUGIN)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # importing blocks nothing: that happens in pytest_load_initial_conftests
    return module


def _locks():
    plugin = _plugin()
    return plugin, plugin.pinned(plugin.CI_LOCK.read_text()), plugin.pinned(plugin.LIGHT_LOCK.read_text())


def test_requirements_ci_still_lacks_the_light_packages_the_plugin_blocks():
    plugin, ci, light = _locks()
    assert ABSENT_FROM_HEAVY & ci == set(), (
        "requirements-ci.txt now has " + ", ".join(sorted(ABSENT_FROM_HEAVY & ci))
        + ": the heavy CI env no longer lacks it. Update ABSENT_FROM_HEAVY and re-check the heavy tests."
    )
    assert ABSENT_FROM_HEAVY <= light, "the light lock dropped " + ", ".join(sorted(ABSENT_FROM_HEAVY - light))
    assert ABSENT_FROM_HEAVY <= plugin.blocked_distributions(
        plugin.CI_LOCK.read_text(), plugin.LIGHT_LOCK.read_text()
    )


def test_the_plugin_never_blocks_what_the_heavy_env_runs_on():
    plugin, ci, light = _locks()
    blocked = plugin.blocked_distributions(plugin.CI_LOCK.read_text(), plugin.LIGHT_LOCK.read_text())
    assert PRESENT_IN_HEAVY <= ci, "requirements-ci.txt dropped " + ", ".join(sorted(PRESENT_IN_HEAVY - ci))
    assert blocked & ci == set()
    assert blocked & PRESENT_IN_HEAVY == set()
