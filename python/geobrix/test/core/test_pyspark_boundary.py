"""Without PySpark, the light tier fails with one clear message -- and stays lazy.

PySpark is not a GeoBrix dependency (every Databricks runtime ships its own), so
a local install without it must say so instead of a bare
``ModuleNotFoundError: No module named 'pyspark'``. The check runs in a
subprocess whose import hook reports pyspark as not installed.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import databricks.labs.gbx as gbx

_HINT = "GeoBrix uses the PySpark that ships with your Databricks runtime"
_ROOT = Path(gbx.__file__).parent

_PROBE = """
import importlib, importlib.abc, json, sys

class _NoPyspark(importlib.abc.MetaPathFinder):
    # As if pyspark were not installed: the same error, raised first.
    def find_spec(self, name, path=None, target=None):
        if name == "pyspark" or name.startswith("pyspark."):
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)

sys.meta_path.insert(0, _NoPyspark())
out = {}
for name in sys.argv[1:]:
    try:
        importlib.import_module(name)
        out[name] = None
    except ModuleNotFoundError as e:
        out[name] = [e.name, str(e)]
    except Exception as e:  # an unrelated optional dependency, not under test
        out[name] = [None, repr(e)]
print(json.dumps(out))
"""


def _is_pyspark(missing):
    return missing == "pyspark" or (missing or "").startswith("pyspark.")


def _public_modules():
    """Every public module of the light packages (no ``_``-prefixed segment)."""
    names = ["databricks.labs.gbx", "databricks.labs.gbx.ds"]
    for pkg in ("pyrx", "pyvx", "pygx"):
        for f in sorted((_ROOT / pkg).rglob("*.py")):
            parts = f.relative_to(_ROOT).with_suffix("").parts
            if parts[-1] == "__init__":
                parts = parts[:-1]
            if any(p.startswith("_") for p in parts):
                continue
            names.append(".".join(("databricks", "labs", "gbx") + parts))
    return names


def _import_without_pyspark(names):
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)}
    r = subprocess.run(
        [sys.executable, "-c", _PROBE, *names],
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
    )
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout.strip().splitlines()[-1])


def test_missing_pyspark_gives_one_clear_error_and_packages_stay_lazy():
    got = _import_without_pyspark(_public_modules())
    bare = {
        name: err[1]
        for name, err in got.items()
        if err and _is_pyspark(err[0]) and _HINT not in err[1]
    }
    assert bare == {}, f"bare pyspark ModuleNotFoundError from: {sorted(bare)}"
    # The packages themselves never import pyspark ...
    for pkg in ("", ".pyrx", ".pyvx", ".pygx"):
        assert got["databricks.labs.gbx" + pkg] is None, pkg
    # ... while their Spark entry points need it, and say how to get it.
    for name in ("ds", "pyrx.functions", "pyvx.functions", "pygx.functions"):
        err = got["databricks.labs.gbx." + name]
        assert err and _is_pyspark(err[0]) and "pyspark>=4.0.0" in err[1], name
