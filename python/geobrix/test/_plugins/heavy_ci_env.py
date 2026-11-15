"""pytest plugin (``-p heavy_ci_env``): run the heavy-dir tests as the heavyweight CI job sees them.

WHY: the heavy CI job (``build python``, .github/actions/python_build/action.yml) installs only
``requirements-ci.txt``: no pandas, rasterio, shapely, quadbin and the rest of the light stack. The dev
container has all of them, so a heavy run there cannot see a failure caused by a missing package. This plugin makes
those packages unimportable (``sys.modules[name] = None``), so ``import pandas`` raises ``ImportError`` and
``test/conftest.py`` takes its own CI path (``find_spec("rasterio") is None``).

The blocked set is derived, not listed: the distributions in the light CI lock (``LIGHT_LOCK``, the primary light
lock the ``pyrx`` CI job installs) that ``requirements-ci.txt`` lacks, mapped to their installed top-level modules.
A module shared with a distribution that stays installed (a namespace package) is left alone, and so are the
standard library and ``osgeo`` (GDAL is installed apart from the lock).

``gbx:test:python --heavy-ci-env`` loads it. Limit: only the pytest process is affected; Spark worker
processes still import what is installed.
"""

import importlib.metadata
import re
import sys
from pathlib import Path

import pytest

LOCKS = Path(__file__).resolve().parents[2]  # python/geobrix
CI_LOCK = LOCKS / "requirements-ci.txt"
LIGHT_LOCK = LOCKS / "requirements-light-env6-ci.txt"

# Modules that must stay importable whatever the locks say: GDAL's binding is installed apart from the lock.
KEEP = frozenset({"osgeo"})

# Top-level names some wheels ship that are not importable packages (or, `test`, belong to the checkout's own tests).
NOT_MODULES = frozenset({"test", "tests", "include", "lib64", "docs"})

_PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==", re.M)


def _norm(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def pinned(text):
    """The normalised distribution names a hash-pinned lock pins."""
    return {_norm(n) for n in _PIN.findall(text)}


def blocked_distributions(ci_text, light_text):
    """Distributions the light lock has and the heavy CI lock lacks."""
    return pinned(light_text) - pinned(ci_text)


def blocked_modules(distributions):
    """Top-level modules to block: those installed, owned only by ``distributions``, not stdlib or ``KEEP``."""
    owners = importlib.metadata.packages_distributions()
    return sorted(
        mod
        for mod, dists in owners.items()
        if dists
        and all(_norm(d) in distributions for d in dists)
        and mod not in sys.stdlib_module_names
        and mod not in KEEP
        and mod.isidentifier()
        and "__mypyc" not in mod
        and mod not in NOT_MODULES
        and not mod.startswith("_")
    )


def block():
    """Make the blocked modules unimportable; the sorted names."""
    names = blocked_modules(
        blocked_distributions(CI_LOCK.read_text(), LIGHT_LOCK.read_text())
    )
    for name in names:
        sys.modules[name] = None
    return names


BLOCKED = []


@pytest.hookimpl(tryfirst=True)
def pytest_load_initial_conftests(early_config, parser, args):
    """Block before any conftest or test module is imported."""
    BLOCKED[:] = block()


def pytest_report_header(config):
    return f"heavy-ci-env: {len(BLOCKED)} modules blocked ({CI_LOCK.name} lacks them): {' '.join(BLOCKED)}"
