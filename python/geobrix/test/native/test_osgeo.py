"""GDAL's own Python bindings reach the package through one module, ``databricks.labs.gbx._osgeo``.

- Where ``osgeo`` does not import (the macOS host, a PyPI install), ``bindings()`` and ``utils()``
  raise an ImportError that names the function, the cause and the fix.
- An ``osgeo`` that is not rasterio's GDAL (another version, or the same version that does not
  open a ``/vsimem`` file rasterio wrote) is refused the same way, unless the caller passes
  ``same_libgdal=False``; ``_selfcheck.stack()`` reports that mismatch instead of raising. A
  second libgdal in the process (a GDAL-bundling library) is no refusal on its own.
- With the env6 payload (``build/env-wheel/env6/payload``, Linux), ``osgeo`` is the payload's build
  of the pinned GDAL, ``osgeo_utils`` imports, and a process that imports rasterio and ``osgeo``
  maps one libgdal of the payload, also when a foreign libgdal was loaded before the first check.
- No other module of the package imports ``osgeo`` or ``osgeo_utils``, and only the sample bundle
  and the test-only COG validator pass ``same_libgdal=False``.
"""

import ast
import ctypes.util
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


def test_the_maps_parse_finds_each_libgdal_whatever_the_line(monkeypatch, tmp_path):
    """``/proc/self/maps`` lines: an auditwheel or payload libgdal, the system one, one replaced on disk since it was
    mapped (" (deleted)"), and a path that is not UTF-8, which must not fail the package's import.
    """
    maps = tmp_path / "maps"
    maps.write_bytes(
        b"7f00-7f01 r-xp 00000000 00:00 1 /opt/x/rasterio.libs/libgdal-c14cf0f0.so.38.3.12.2\n"
        b"7f01-7f02 r-xp 00000000 00:00 2 /usr/lib/libgdal.so.34.3.8.4 (deleted)\n"
        b"7f02-7f03 r-xp 00000000 00:00 3 /usr/lib/libgdalalljni.so\n"
        b"7f03-7f04 r-xp 00000000 00:00 4 /tmp/caf\xe9/libfoo.so\n"
        b"7f04-7f05 rw-p 00000000 00:00 0 [heap]\n"
    )
    real_open = open
    monkeypatch.setattr(
        _osgeo,
        "open",
        lambda f, *a, **k: real_open(maps if f == "/proc/self/maps" else f, *a, **k),
        raising=False,
    )
    assert _osgeo.mapped_libgdal() == [
        "/opt/x/rasterio.libs/libgdal-c14cf0f0.so.38.3.12.2",
        "/usr/lib/libgdal.so.34.3.8.4",
    ]


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


NOT_SEEN = "osgeo opening rasterio's /vsimem file failed (RuntimeError: No such file)"


@pytest.mark.parametrize(
    "version, mapped, round_trip, same_libgdal, refused",
    [
        ("rasterio's", ONE, None, True, None),
        # a GDAL-bundling library (fiona) loaded first maps a second libgdal; still one GDAL
        ("rasterio's", TWO, None, True, None),
        ("9.9.9", ONE, None, True, "osgeo GDAL 9.9.9 from /fake/osgeo/gdal.py"),
        # the same version, two libgdal (or two dylibs off Linux): /vsimem is not shared, and
        # the message says which step failed
        ("rasterio's", None, NOT_SEEN, True, NOT_SEEN),
        ("9.9.9", TWO, NOT_SEEN, False, None),  # a by-path caller may use another GDAL
    ],
)
def test_osgeo_on_another_gdal_is_refused(
    fake_osgeo, monkeypatch, version, mapped, round_trip, same_libgdal, refused
):
    rasterio = pytest.importorskip("rasterio")
    fake_osgeo(rasterio.__gdal_version__ if version == "rasterio's" else version)
    monkeypatch.setattr(_osgeo, "mapped_libgdal", lambda: mapped)
    monkeypatch.setattr(_osgeo, "vsimem_check", lambda gdal: round_trip)
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
    assert "osgeo GDAL 9.9.9" in st["osgeo_error"] and "Fix:" in st["osgeo_error"]


_PROBE = """
import ctypes, json, os, re
import databricks.labs.gbx  # activates the payload named by GBX_NATIVE_ROOT
from databricks.labs.gbx import _osgeo
import rasterio
if os.environ.get("FOREIGN_LIBGDAL"):  # a GDAL-bundling library loaded before the first check
    ctypes.CDLL(os.environ["FOREIGN_LIBGDAL"])
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


# the first osgeo use runs the /vsimem check: it must print no warning (rasterio's
# NotGeoreferencedWarning is a UserWarning; osgeo's exception-mode notice a FutureWarning)
STRICT = ["-W", "error::UserWarning", "-W", "error::FutureWarning"]


@pytest.mark.parametrize(
    "foreign_first, warn_args",
    [(False, []), (True, []), (False, STRICT)],
    ids=["alone", "foreign-first", "warnings-are-errors"],
)
def test_the_payload_osgeo_is_the_pinned_gdal_and_rasterio_libgdal(
    foreign_first, warn_args
):
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
    if foreign_first:  # the dev image's system GDAL stands in for a fiona-like wheel
        foreign = ctypes.util.find_library("gdal")
        if not foreign:
            pytest.skip("no system libgdal to load as the foreign GDAL")
        env["FOREIGN_LIBGDAL"] = foreign
    res = subprocess.run(
        [sys.executable, *warn_args, "-c", _PROBE],
        env=env,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr[-2000:]
    got = json.loads(res.stdout.splitlines()[-1])
    assert (got["gdal"], got["rasterio_gdal"], got["gdal_array"]) == (pin, pin, True)
    assert got["osgeo"].startswith(f"{PAYLOAD}/_vendor/{a}/osgeo/")
    assert got["gdal_calc"].startswith(f"{PAYLOAD}/_vendor/common/osgeo_utils/")
    ours = [p for p in got["libgdal"] if p.startswith(f"{PAYLOAD}/_native/{a}/lib/")]
    assert len(ours) == 1 and len(got["libgdal"]) == 1 + foreign_first, got


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


# The modules that import osgeo themselves, and why each is safe: each does so only once the helper's verdict says
# osgeo is rasterio's GDAL, or builds a test fixture.
OSGEO_IMPORT_EXEMPT = {
    # the driver policy's full mode: gdal is imported only after _same_libgdal_report()["shared"]; the helper's
    # bindings() would turn GDAL exceptions on for the whole process, which the policy's identify must not
    SRC / "databricks" / "labs" / "gbx" / "_gdal_policy.py",
    # osr.SetPROJSearchPaths, only after _same_libgdal_report()["shared"]
    SRC / "databricks" / "labs" / "gbx" / "pyrx" / "_env.py",
    # the bench oracle's GTI fixture builder (a GeoPackage tile index it writes for the policy rows)
    SRC / "databricks" / "labs" / "gbx" / "bench" / "oracles.py",
}


def test_only_the_helper_imports_osgeo():
    found = [
        f"{p.relative_to(SRC)}:{line} {name}"
        for p in sorted(SRC.rglob("*.py"))
        if p != HELPER and p not in OSGEO_IMPORT_EXEMPT
        for line, name in _osgeo_imports(p)
    ]
    assert (
        not found
    ), f"import osgeo through databricks.labs.gbx._osgeo, not directly: {found}"


def _exemptions_in(source, name="<source>"):
    """Lines of every call in ``source`` that passes ``same_libgdal`` other than a literal True, or that hides
    keywords in a ``**`` argument to ``bindings`` / ``utils`` (which could carry it)."""
    for node in ast.walk(ast.parse(source, name)):
        if isinstance(node, ast.Call):
            callee = getattr(node.func, "attr", getattr(node.func, "id", None))
            for kw in node.keywords:
                if kw.arg == "same_libgdal" and not (
                    isinstance(kw.value, ast.Constant) and kw.value.value is True
                ):
                    yield node.lineno
                elif kw.arg is None and callee in ("bindings", "utils"):
                    yield node.lineno


def _exemptions(path):
    yield from _exemptions_in(path.read_text(), str(path))


@pytest.mark.parametrize(
    "source, flagged",
    [
        ('bindings("x", same_libgdal=True)', False),
        ('bindings("x", same_libgdal=False)', True),
        ('bindings("x", same_libgdal=flag)', True),
        ('bindings("x", **{"same_libgdal": False})', True),  # a keyword hidden in **
        ('_osgeo.utils("x", **kw)', True),
        ("dict(**kw)", False),  # ** elsewhere is not this check's business
    ],
)
def test_the_exemption_scan_sees_a_flag_passed_in_a_double_star(source, flagged):
    assert bool(list(_exemptions_in(source))) is flagged


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
