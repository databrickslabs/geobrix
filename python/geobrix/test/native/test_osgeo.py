"""GDAL's own Python bindings reach the package through one module, ``databricks.labs.gbx._osgeo``.

- Where ``osgeo`` does not import (the macOS host, a PyPI install), ``bindings()`` and ``utils()``
  raise an ImportError that names the function, the cause and the fix.
- With the env6 payload (``build/env-wheel/env6/payload``, Linux), ``osgeo`` is the payload's build
  of the pinned GDAL, ``osgeo_utils`` imports, and a process that imports rasterio and ``osgeo``
  maps one libgdal: the payload's.
- No other module of the package imports ``osgeo`` or ``osgeo_utils``.
"""

import ast
import json
import os
import platform
import re
import subprocess
import sys
from pathlib import Path

import pytest

from databricks.labs.gbx import _osgeo

REPO = Path(__file__).resolve().parents[4]
SRC = REPO / "python" / "geobrix" / "src"
HELPER = SRC / "databricks" / "labs" / "gbx" / "_osgeo.py"
PAYLOAD = REPO / "build" / "env-wheel" / "env6" / "payload"
OSGEO = ("osgeo", "osgeo_utils")


def test_without_osgeo_the_error_names_the_env_wheel(monkeypatch):
    for name in [n for n in sys.modules if n.split(".")[0] in OSGEO]:
        monkeypatch.delitem(sys.modules, name)
    for name in OSGEO:
        monkeypatch.setitem(sys.modules, name, None)
    for call in (
        lambda: _osgeo.bindings("rst_demo"),
        lambda: _osgeo.utils("gdal_calc", "rst_demo"),
    ):
        with pytest.raises(
            ImportError, match=r"^rst_demo needs GDAL's Python bindings"
        ) as e:
            call()
        assert isinstance(e.value, _osgeo.OsgeoUnavailable)
        assert re.search(
            r"Fix: install .*\+env6-py3-none-any\.whl .*\+env5-py3-none-any\.whl",
            str(e.value),
        )


_PROBE = """
import json, re
import databricks.labs.gbx  # activates the payload named by GBX_NATIVE_ROOT
from databricks.labs.gbx import _osgeo
import rasterio
g = _osgeo.bindings()
calc = _osgeo.utils("gdal_calc")
with open("/proc/self/maps") as fh:
    maps = {ln.split(None, 5)[-1].strip() for ln in fh if "/" in ln}
print(json.dumps({
    "gdal": g.gdal.VersionInfo("RELEASE_NAME"),
    "rasterio_gdal": rasterio.__gdal_version__,
    "osgeo": g.gdal.__file__,
    "gdal_array": g.gdal_array is not None,
    "gdal_calc": calc.__file__,
    "libgdal": sorted(p for p in maps if re.search(r"/libgdal(-[0-9a-f]{8})?\\.so", p)),
}))
"""


def test_the_payload_osgeo_is_the_pinned_gdal_and_rasterio_libgdal():
    a = platform.machine()
    if (
        not sys.platform.startswith("linux")
        or not (PAYLOAD / "_vendor" / a / "osgeo").is_dir()
    ):
        pytest.skip(
            f"PRECONDITION MISSING: {PAYLOAD}/_vendor/{a}/osgeo on Linux (gbx:build:wheel --env 6; "
            "run in the container)"
        )
    pin = re.search(
        r"^GDAL_VERSION=(\S+)$", (REPO / "scripts/native/pins.conf").read_text(), re.M
    ).group(1)
    env = {**os.environ, "GBX_NATIVE_ROOT": str(PAYLOAD)}
    env.pop("GBX_NATIVE_DISABLE", None)
    res = subprocess.run(
        [sys.executable, "-c", _PROBE], env=env, capture_output=True, text=True
    )
    assert res.returncode == 0, res.stderr[-2000:]
    got = json.loads(res.stdout.splitlines()[-1])
    assert (got["gdal"], got["rasterio_gdal"], got["gdal_array"]) == (pin, pin, True)
    assert got["osgeo"].startswith(f"{PAYLOAD}/_vendor/{a}/osgeo/")
    assert got["gdal_calc"].startswith(f"{PAYLOAD}/_vendor/common/osgeo_utils/")
    assert len(got["libgdal"]) == 1 and got["libgdal"][0].startswith(
        f"{PAYLOAD}/_native/{a}/lib/"
    ), got


def _osgeo_imports(path):
    """(line, module) of every import of osgeo or osgeo_utils in a file, importlib included."""
    for node in ast.walk(ast.parse(path.read_text(), str(path))):
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and not node.level:
            names = [node.module or ""]
        elif (
            isinstance(node, ast.Call)
            and getattr(node.func, "attr", getattr(node.func, "id", None))
            in ("import_module", "__import__")
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            names = [node.args[0].value]
        else:
            continue
        yield from ((node.lineno, n) for n in names if n.split(".")[0] in OSGEO)


def test_only_the_helper_imports_osgeo():
    found = [
        f"{p.relative_to(SRC)}:{line} {name}"
        for p in sorted(SRC.rglob("*.py"))
        if p != HELPER
        for line, name in _osgeo_imports(p)
    ]
    assert (
        not found
    ), f"import osgeo through databricks.labs.gbx._osgeo, not directly: {found}"
