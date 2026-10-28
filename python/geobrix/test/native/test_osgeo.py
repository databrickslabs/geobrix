"""GDAL's own Python bindings reach the package through one module, ``databricks.labs.gbx._osgeo``.

- Where ``osgeo`` does not import (the macOS host, a PyPI install), ``bindings()`` and ``utils()``
  raise an ImportError that names the function, the cause and the fix.
- An ``osgeo`` that is not rasterio's GDAL (another version, or a second libgdal mapped) is
  refused the same way, unless the caller passes ``same_libgdal=False``; ``_selfcheck.stack()``
  reports that mismatch instead of raising.
- With the env6 payload (``build/env-wheel/env6/payload``, Linux), ``osgeo`` is the payload's build
  of the pinned GDAL, ``osgeo_utils`` imports, and a process that imports rasterio and ``osgeo``
  maps one libgdal: the payload's.
- No other module of the package imports ``osgeo`` or ``osgeo_utils``, and only the sample bundle
  and the test-only COG validator pass ``same_libgdal=False``.
"""

import ast
import json
import os
import platform
import re
import subprocess
import sys
import types
from pathlib import Path

import pytest

from databricks.labs.gbx import _osgeo

REPO = Path(__file__).resolve().parents[4]
SRC = REPO / "python" / "geobrix" / "src"
HELPER = SRC / "databricks" / "labs" / "gbx" / "_osgeo.py"
PAYLOAD = REPO / "build" / "env-wheel" / "env6" / "payload"
OSGEO = ("osgeo", "osgeo_utils")
# The callers that open and write files by path only, never sharing a dataset or a /vsimem file
# with rasterio, so another GDAL's osgeo serves them: the dev-only sample bundle and the
# test-only COG validator.
SAME_LIBGDAL_EXEMPT = {
    SRC / "databricks" / "labs" / "gbx" / "sample" / "_bundle.py",
    REPO / "python" / "geobrix" / "test" / "pyrx" / "conftest.py",
    REPO / "docs" / "tests" / "python" / "readers" / "cog_gbx_examples.py",
}


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


@pytest.fixture
def fake_osgeo(monkeypatch):
    """Install a stand-in ``osgeo`` whose ``gdal.VersionInfo()`` is the given version, with no
    libgdal behind it (``gdal_array`` and ``osgeo_utils`` absent); the cached verdict is cleared.
    """

    def install(version):
        for name in [n for n in sys.modules if n.split(".")[0] in OSGEO]:
            monkeypatch.delitem(sys.modules, name)
        pkg = types.ModuleType("osgeo")
        for sub in ("gdal", "ogr", "osr"):
            mod = types.ModuleType(f"osgeo.{sub}")
            mod.__file__ = f"/fake/osgeo/{sub}.py"
            mod.UseExceptions = lambda: None
            setattr(pkg, sub, mod)
            monkeypatch.setitem(sys.modules, f"osgeo.{sub}", mod)
        pkg.gdal.VersionInfo = lambda request="VERSION_NUM": version
        monkeypatch.setitem(
            sys.modules, "osgeo", pkg
        )  # no gdal_array: it does not import
        monkeypatch.setitem(sys.modules, "osgeo_utils", None)

    _osgeo._same_libgdal_report.cache_clear()
    yield install
    _osgeo._same_libgdal_report.cache_clear()


ONE, TWO = ["/x/libgdal.so.39"], ["/x/libgdal.so.39", "/y/libgdal-0a1b2c3d.so.38"]


@pytest.mark.parametrize(
    "version, mapped, same_libgdal, refused",
    [
        ("rasterio's", ONE, True, None),
        ("rasterio's", None, True, None),  # off Linux: the versions decide
        ("9.9.9", ONE, True, "osgeo GDAL 9.9.9 from /fake/osgeo/gdal.py"),
        ("rasterio's", TWO, True, "the process maps 2 libgdal files"),
        ("9.9.9", TWO, False, None),  # a by-path caller may use another GDAL
    ],
)
def test_osgeo_on_another_gdal_is_refused(
    fake_osgeo, monkeypatch, version, mapped, same_libgdal, refused
):
    rasterio = pytest.importorskip("rasterio")
    fake_osgeo(rasterio.__gdal_version__ if version == "rasterio's" else version)
    monkeypatch.setattr(_osgeo, "mapped_libgdal", lambda: mapped)
    if refused is None:
        assert _osgeo.bindings("rst_demo", same_libgdal=same_libgdal).gdal_array is None
        return
    with pytest.raises(_osgeo.OsgeoUnavailable) as e:
        _osgeo.bindings("rst_demo", same_libgdal=same_libgdal)
    msg = str(e.value)
    assert msg.startswith(
        "rst_demo needs GDAL's Python bindings (osgeo) on rasterio's GDAL"
    )
    assert refused in msg and re.search(r"Fix: install .*\+env6.*GBX_NATIVE_ROOT", msg)


def test_selfcheck_stack_reports_another_gdal_instead_of_raising(
    fake_osgeo, monkeypatch
):
    for mod in ("rasterio", "pyogrio", "pyproj", "shapely", "netCDF4"):
        pytest.importorskip(mod)
    from databricks.labs.gbx._native import _selfcheck

    fake_osgeo("9.9.9")
    monkeypatch.setattr(_osgeo, "mapped_libgdal", lambda: TWO)
    st = _selfcheck.stack()
    assert (st["osgeo_gdal"], st["shared_libgdal"], st["gdal_array"]) == (
        "9.9.9",
        False,
        False,
    )
    assert st["libgdal"] == TWO and st["from_payload"]["osgeo_utils"] is False
    assert "maps 2 libgdal files" in st["osgeo_error"] and "Fix:" in st["osgeo_error"]


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
    if not sys.platform.startswith("linux"):
        pytest.skip(
            "the env wheels' payload loads on Linux only (run in the container)"
        )
    a = platform.machine()
    if not (PAYLOAD / "_vendor" / a / "osgeo").is_dir():
        pytest.skip(
            f"PRECONDITION MISSING: {PAYLOAD}/_vendor/{a}/osgeo (gbx:build:wheel --env 6)"
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
    """(line, module) of every import of osgeo or osgeo_utils in a file, importlib included.

    A static check: an ``import_module`` of a variable or an f-string, or an ``exec``, is not
    seen (the helper's own ``utils()`` builds its name that way).
    """
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


def _exemptions(path):
    """Lines of every call in a file that passes ``same_libgdal`` other than a literal True."""
    for node in ast.walk(ast.parse(path.read_text(), str(path))):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "same_libgdal" and not (
                    isinstance(kw.value, ast.Constant) and kw.value.value is True
                ):
                    yield node.lineno


def test_only_the_by_path_callers_skip_the_same_libgdal_check():
    roots = (
        SRC,
        REPO / "python" / "geobrix" / "test",
        REPO / "docs" / "tests" / "python",
    )
    found = {
        p: list(_exemptions(p))
        for root in roots
        for p in sorted(root.rglob("*.py"))
        if p not in (HELPER, Path(__file__).resolve())
        and ".pytest_cache" not in p.parts
    }
    found = {p: lines for p, lines in found.items() if lines}
    assert set(found) == SAME_LIBGDAL_EXEMPT, {
        str(p.relative_to(REPO)): lines for p, lines in found.items()
    }
