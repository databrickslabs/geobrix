"""The activation rules (``_native/_rules.py``): the one copy both activators use.

The CA rows: a user's ``GDAL_CURL_CA_BUNDLE``/``PROJ_CURL_CA_BUNDLE`` is never touched; otherwise
both get the user's ``CURL_CA_BUNDLE``, else the user's ``SSL_CERT_FILE``, else the system store;
``CURL_CA_BUNDLE`` is never set. The data rows: ``GDAL_DATA``/``PROJ_DATA`` only when unset, no
``PROJ_DATA`` over a user's ``PROJ_LIB``, and a ``PROJ_DATA``/``PROJ_LIB`` with no readable ``proj.db`` counts as
unset (with a payload: a real unreadable directory, and ``import pyproj`` in a fresh interpreter). The plugin rows: the payload's HDF5 filter plugins are
registered with the payload's own netCDF-C/HDF5 on the first import of a binding that reaches them,
once, and ``HDF5_PLUGIN_PATH`` is never set. Then both activators (the startup module, loaded from
``src/`` by path as the ``.pth`` imports it, and the import-time ``_native.activate``) apply the
rules (and leave ``GDAL_HTTP_VERSION`` to GDAL's default), and do nothing at all on an interpreter
other than the payload's CPython 3.12.
"""

import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import types
from pathlib import Path

import pytest

from databricks.labs.gbx import _native
from databricks.labs.gbx._native import _rules

SRC = Path(__file__).resolve().parents[2] / "src"
STORE = "<the system store>"
_ENV = (
    "GDAL_DATA",
    "PROJ_DATA",
    "PROJ_LIB",
    "GDAL_CURL_CA_BUNDLE",
    "PROJ_CURL_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "SSL_CERT_FILE",
    "GBX_NATIVE_DISABLE",
    "GBX_NATIVE_PTH",
    "GBX_NATIVE_ROOT",
    "HDF5_PLUGIN_PATH",
    "GDAL_HTTP_VERSION",
)

CA_CASES = [  # the user's environment, a system store exists, GDAL's bundle after, PROJ's after
    ({"GDAL_CURL_CA_BUNDLE": "/u/gdal.pem"}, True, "/u/gdal.pem", STORE),
    (
        {"GDAL_CURL_CA_BUNDLE": "/u/gdal.pem", "PROJ_CURL_CA_BUNDLE": ""},
        True,
        "/u/gdal.pem",
        "",
    ),
    ({"CURL_CA_BUNDLE": "/u/curl.pem"}, True, "/u/curl.pem", "/u/curl.pem"),
    ({"SSL_CERT_FILE": "/u/ssl.pem"}, True, "/u/ssl.pem", "/u/ssl.pem"),
    (
        {"CURL_CA_BUNDLE": "/u/curl.pem", "SSL_CERT_FILE": "/u/ssl.pem"},
        True,
        "/u/curl.pem",
        "/u/curl.pem",
    ),
    ({}, True, STORE, STORE),
    ({}, False, None, None),  # no store and no fallback: nothing is set
]


@pytest.mark.parametrize("environ, store, gdal, proj", CA_CASES)
def test_the_ca_rule(tmp_path, environ, store, gdal, proj):
    bundle = tmp_path / "ca-certificates.crt"
    if store:
        bundle.write_text("-----BEGIN CERTIFICATE-----\n")
    env = dict(environ)
    _rules.set_ca(env, candidates=[str(bundle)])
    want = {None: None, STORE: str(bundle)}
    assert env.get("GDAL_CURL_CA_BUNDLE") == want.get(gdal, gdal)
    assert env.get("PROJ_CURL_CA_BUNDLE") == want.get(proj, proj)
    assert env.get("CURL_CA_BUNDLE") == environ.get("CURL_CA_BUNDLE")  # never ours


# The user's environment, GDAL_DATA after, PROJ_DATA after. "<ok>"/"<ok2>" are directories holding a proj.db this
# process can read; "<grids>" a readable grid directory (a .gsb, no proj.db); "<empty>" an empty one; "<missing>"
# does not exist; "<unr>" exists with a proj.db this process cannot read (EACCES; simulated, see _unreadable).
# DBR 19 exports PROJ_DATA=/databricks/native/proj-data, which a standard-access notebook user cannot read: an entry
# that exists but cannot be read goes, every other one stays in order, and PROJ must still find a proj.db.
DATA_CASES = [
    ({}, "<share>/gdal", "<share>/proj"),
    ({"PROJ_LIB": "<ok>"}, "<share>/gdal", None),
    ({"GDAL_DATA": "/u/gdal", "PROJ_DATA": "<ok>"}, "/u/gdal", "<ok>"),
    ({"PROJ_DATA": ""}, "<share>/gdal", "<share>/proj"),
    ({"PROJ_LIB": "<missing>"}, "<share>/gdal", "<share>/proj"),
    # unreadable: it goes, and the payload's data, or the user's readable PROJ_LIB, serves
    ({"PROJ_DATA": "<unr>"}, "<share>/gdal", "<share>/proj"),
    ({"PROJ_DATA": "<unr>", "PROJ_LIB": "<ok>"}, "<share>/gdal", None),
    ({"PROJ_DATA": f"<unr>{os.pathsep}<ok>"}, "<share>/gdal", "<ok>"),
    ({"PROJ_DATA": f"<ok>{os.pathsep}<unr>"}, "<share>/gdal", "<ok>"),
    # grid directories and missing ones stay (PROJ accepts them); with no proj.db left, a data directory is appended
    (
        {"PROJ_DATA": f"<grids>{os.pathsep}<ok>"},
        "<share>/gdal",
        f"<grids>{os.pathsep}<ok>",
    ),
    (
        {"PROJ_DATA": f"<ok>{os.pathsep}<empty>"},
        "<share>/gdal",
        f"<ok>{os.pathsep}<empty>",
    ),
    (
        {"PROJ_DATA": f"<missing>{os.pathsep}<ok>"},
        "<share>/gdal",
        f"<missing>{os.pathsep}<ok>",
    ),
    ({"PROJ_DATA": f"<ok>{os.pathsep}<ok2>"}, "<share>/gdal", f"<ok>{os.pathsep}<ok2>"),
    ({"PROJ_DATA": "<grids>"}, "<share>/gdal", f"<grids>{os.pathsep}<share>/proj"),
    ({"PROJ_DATA": "<missing>"}, "<share>/gdal", f"<missing>{os.pathsep}<share>/proj"),
    (
        {"PROJ_DATA": f"<grids>{os.pathsep}<unr>", "PROJ_LIB": "<ok>"},
        "<share>/gdal",
        f"<grids>{os.pathsep}<ok>",
    ),
    # PROJ_LIB: an all-unreadable one goes; a grid-only one keeps its grids (PROJ reads PROJ_DATA first), both when
    # PROJ_DATA is completed and when it is set
    ({"PROJ_LIB": "<unr>"}, "<share>/gdal", "<share>/proj", None),
    ({"PROJ_LIB": "<grids>"}, "<share>/gdal", f"<grids>{os.pathsep}<share>/proj"),
    (
        {"PROJ_DATA": "<grids2>", "PROJ_LIB": "<grids>"},
        "<share>/gdal",
        f"<grids2>{os.pathsep}<grids>{os.pathsep}<share>/proj",
    ),
    (
        {"PROJ_DATA": "<grids2>", "PROJ_LIB": f"<grids>{os.pathsep}<ok>"},
        "<share>/gdal",
        f"<grids2>{os.pathsep}<grids>{os.pathsep}<ok>",
    ),
    # another PROJ's proj.db (DBR 19's /databricks/native/proj-data on a dedicated cluster): it goes like an
    # unreadable one, so the payload's proj.db wins; a proj.db of the payload's version stays
    ({"PROJ_DATA": "<foreign>"}, "<share>/gdal", "<share>/proj"),
    (
        {"PROJ_DATA": "<foreign9>"},
        "<share>/gdal",
        "<share>/proj",
    ),  # the same layout, another PROJ.VERSION
    ({"PROJ_DATA": "<nometa>"}, "<share>/gdal", "<share>/proj"),  # not a PROJ database
    (
        {"PROJ_DATA": f"<foreign>{os.pathsep}<grids>"},
        "<share>/gdal",
        f"<grids>{os.pathsep}<share>/proj",
    ),
    ({"PROJ_DATA": f"<foreign>{os.pathsep}<ok>"}, "<share>/gdal", "<ok>"),
    (
        {"PROJ_DATA": "<same>"},
        "<share>/gdal",
        "<same>",
    ),  # the payload's own version: kept as it is
    ({"PROJ_LIB": "<foreign>"}, "<share>/gdal", "<share>/proj", None),
]
KEEP = object()  # PROJ_LIB after the rule: as the user set it
DATA_CASES = [c if len(c) == 4 else (*c, KEEP) for c in DATA_CASES]


def _proj_db(path, layout=("1", "5"), version="9.9.0"):
    """A proj.db with only the metadata table the version check reads (``layout`` None: no metadata at all)."""
    import sqlite3

    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE metadata (key TEXT, value TEXT)")
    if layout:
        rows = [
            ("DATABASE.LAYOUT.VERSION.MAJOR", layout[0]),
            ("DATABASE.LAYOUT.VERSION.MINOR", layout[1]),
        ]
        con.executemany(
            "INSERT INTO metadata VALUES (?, ?)",
            rows + ([("PROJ.VERSION", version)] if version else []),
        )
    con.commit()
    con.close()


def _proj_dirs(tmp_path):
    """The directories DATA_CASES names, and "<share>": a payload share/ whose proj/proj.db is PROJ 9.9.0's."""
    names = (
        "ok",
        "ok2",
        "grids",
        "grids2",
        "empty",
        "unr",
        "same",
        "foreign",
        "foreign9",
        "nometa",
    )
    for n in names:
        (tmp_path / n).mkdir()
    for n in ("ok", "ok2", "unr", "same"):
        _proj_db(tmp_path / n / "proj.db")
    _proj_db(tmp_path / "share" / "proj" / "proj.db")
    _proj_db(tmp_path / "foreign" / "proj.db", layout=("1", "4"), version="9.4.0")
    _proj_db(tmp_path / "foreign9" / "proj.db", version="9.6.1")
    _proj_db(tmp_path / "nometa" / "proj.db", layout=None)
    for n in ("grids", "grids2"):
        (tmp_path / n / "my_grid.gsb").write_bytes(b"NUM_OREC")
    dirs = {f"<{n}>": str(tmp_path / n) for n in names}
    dirs["<missing>"] = str(tmp_path / "missing")
    dirs["<share>"] = str(tmp_path / "share")
    return dirs


@pytest.fixture
def _unreadable(monkeypatch):
    """Make "<unr>" (and what is under it) unreadable to os.access, as a mode-000 directory is to a non-root user
    (the container runs as root, which reads anything; the subprocess row below does it for real, as nobody).
    """
    real = os.access
    holder = {}

    def access(path, mode, *a, **k):
        if holder.get("unr") and str(path).startswith(holder["unr"]):
            return False
        return real(path, mode, *a, **k)

    monkeypatch.setattr(_rules.os, "access", access)
    return holder


def _fill(value, dirs):
    for k, v in dirs.items():
        value = value.replace(k, v) if value else value
    return value


@pytest.mark.parametrize("environ, gdal_data, proj_data, proj_lib", DATA_CASES)
def test_the_data_rule(tmp_path, _unreadable, environ, gdal_data, proj_data, proj_lib):
    dirs = _proj_dirs(tmp_path)
    _unreadable["unr"] = dirs["<unr>"]
    env = {k: _fill(v, dirs) for k, v in environ.items()}
    _rules.set_data_dirs(env, dirs["<share>"])
    assert (env.get("GDAL_DATA"), env.get("PROJ_DATA")) == (
        _fill(gdal_data, dirs),
        _fill(proj_data, dirs),
    )
    want_lib = environ.get("PROJ_LIB") if proj_lib is KEEP else proj_lib
    assert env.get("PROJ_LIB") == _fill(want_lib, dirs)


def test_a_proj_lib_list_drops_only_its_unreadable_entries(tmp_path, _unreadable):
    dirs = _proj_dirs(tmp_path)
    _unreadable["unr"] = dirs["<unr>"]
    env = {"PROJ_LIB": os.pathsep.join([dirs["<unr>"], dirs["<grids>"], dirs["<ok>"]])}
    _rules.set_data_dirs(env, dirs["<share>"])
    assert env == {
        "GDAL_DATA": dirs["<share>"] + "/gdal",
        "PROJ_LIB": os.pathsep.join([dirs["<grids>"], dirs["<ok>"]]),
    }


def test_a_proj_db_probe_that_raises_is_unreadable_and_never_raises(
    tmp_path, monkeypatch
):
    # Python 3.12's Path.exists raises on EACCES; os.access and isfile must not take activation down either
    dirs = _proj_dirs(tmp_path)

    def denied(*a, **k):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(_rules.os, "access", denied)
    env = {"PROJ_DATA": dirs["<ok>"]}
    _rules.set_data_dirs(env, "<share>")
    assert env["PROJ_DATA"] == "<share>/proj"


@pytest.mark.parametrize(
    "environ, opened",
    [
        (
            {},
            False,
        ),  # neither variable: every interpreter start, so the payload proj.db stays closed
        (
            {"PROJ_DATA": "<grids>"},
            False,
        ),  # a grid directory holds no proj.db to compare with
        ({"PROJ_LIB": "<grids>", "PROJ_DATA": "<missing>"}, False),
        ({"PROJ_DATA": "<ok>"}, True),  # a proj.db to compare: the payload's is read
        ({"PROJ_LIB": "<ok>"}, True),
    ],
)
def test_the_payload_proj_db_is_opened_only_when_a_variable_names_a_proj_db(
    tmp_path, monkeypatch, environ, opened
):
    dirs = _proj_dirs(tmp_path)
    reads = []
    real = _rules.proj_db_version
    monkeypatch.setattr(
        _rules, "proj_db_version", lambda db: reads.append(db) or real(db)
    )
    env = {k: _fill(v, dirs) for k, v in environ.items()}
    _rules.set_data_dirs(env, dirs["<share>"])
    assert bool([d for d in reads if d.startswith(dirs["<share>"])]) is opened


# The DBR 19 standard-access driver, for real: the runtime's PROJ_DATA names a directory this process cannot read
# (EACCES), and the vendored pyproj, which has no data dir of its own, falls through to PROJ_DATA. A fresh
# interpreter activates a copy of the payload as the .pth does, then imports pyproj and resolves EPSG:4326. Root
# reads a mode-000 directory, so under root the child runs as nobody (and the copies live where nobody can reach).
PYPROJ_PROBE = r"""
import os, sys
sys.path.insert(0, sys.argv[1])
import _gbx_native_path  # what geobrix-native.pth runs at interpreter start
assert _gbx_native_path.ACTIVE, "the payload copy did not activate"
import pyproj
from pyproj import datadir
print("RESULT", pyproj.__file__, pyproj.CRS.from_epsg(4326).to_epsg(), os.environ.get("PROJ_DATA"), datadir.get_data_dir())
"""


def _drop_to_nobody():
    os.setgroups([])
    os.setgid(65534)
    os.setuid(65534)


# PROJ_DATA as the runtime sets it: "unr" a mode-000 directory with a proj.db (EACCES for the child), "ok" a readable
# one; a list keeps its readable entries in order. The PROJ_DATA expected after activation, "payload" = the payload's.
PROJ_DATA_ROWS = {
    "unr": "payload",
    "ok": "ok",
    f"unr{os.pathsep}ok": "ok",
    f"ok{os.pathsep}unr": "ok",
    f"grids{os.pathsep}ok": f"grids{os.pathsep}ok",  # a grid directory (no proj.db) stays
}


@pytest.mark.parametrize("runtime_proj_data", list(PROJ_DATA_ROWS))
def test_import_pyproj_beside_the_runtimes_proj_data(runtime_proj_data):
    root = os.environ.get("GBX_NATIVE_ROOT")
    arch = os.uname().machine if hasattr(os, "uname") else ""
    if not (
        sys.platform.startswith("linux")
        and root
        and Path(root, "_vendor", arch, "pyproj").is_dir()
    ):
        pytest.skip(
            "PRECONDITION MISSING: GBX_NATIVE_ROOT naming an env-wheel payload, on Linux; this row not run"
        )
    root = Path(root)
    base = Path(
        tempfile.mkdtemp(dir="/tmp")
    )  # pytest's tmp dirs are 0700: nobody could not reach them
    runtime = {
        "unr": base / "proj-unr",
        "ok": base / "proj-ok",
        "grids": base / "grids",
    }
    try:
        os.chmod(base, 0o755)
        payload = base / "payload"
        for rel in (f"_native/{arch}/lib", "_native/share", f"_vendor/{arch}/pyproj"):
            shutil.copytree(root / rel, payload / rel)
        shutil.copy2(
            root / "_native" / arch / "MANIFEST.json", payload / "_native" / arch
        )
        src = base / "src"
        (src / "databricks/labs/gbx/_native").mkdir(parents=True)
        shutil.copy2(SRC / "_gbx_native_path.py", src)
        shutil.copy2(
            SRC / "databricks/labs/gbx/_native/_rules.py",
            src / "databricks/labs/gbx/_native",
        )
        for (
            k,
            d,
        ) in (
            runtime.items()
        ):  # the runtime's own PROJ data: a real proj.db; and a grid directory
            d.mkdir()
            if k == "grids":
                (d / "my_grid.gsb").write_bytes(b"NUM_OREC")
            else:
                shutil.copy2(root / "_native/share/proj/proj.db", d)
        runtime["unr"].chmod(0o000)
        proj_data = os.pathsep.join(
            str(runtime[k]) for k in runtime_proj_data.split(os.pathsep)
        )
        env = {k: v for k, v in os.environ.items() if k not in _ENV}
        env.update(GBX_NATIVE_ROOT=str(payload), PROJ_DATA=proj_data, HOME=str(base))
        res = subprocess.run(
            [sys.executable, "-I", "-c", PYPROJ_PROBE, str(src)],
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(base),
            preexec_fn=_drop_to_nobody if os.geteuid() == 0 else None,
        )
    finally:
        if runtime["unr"].exists():
            runtime["unr"].chmod(0o755)
        shutil.rmtree(base, ignore_errors=True)
    out = res.stdout + res.stderr
    line = next(
        (ln for ln in res.stdout.splitlines() if ln.startswith("RESULT ")), None
    )
    assert line, out[-3000:]
    module, epsg, proj_data, data_dir = line.split()[1:]
    assert module.startswith(f"{payload}/_vendor/{arch}/") and epsg == "4326", line
    expect = PROJ_DATA_ROWS[runtime_proj_data]
    want = (
        f"{payload}/_native/share/proj"
        if expect == "payload"
        else os.pathsep.join(str(runtime[k]) for k in expect.split(os.pathsep))
    )
    # pyproj takes PROJ_DATA's first entry that holds a proj.db, or the list itself
    assert proj_data == want and data_dir in (want, str(runtime["ok"])), line


# modules imported after activation, modules imported before it, registrations
HOOK_CASES = [
    # the first binding that reaches the payload's HDF5, once
    (["json", "netCDF4", "rasterio"], [], 1),
    # none of them reaches it: nothing is loaded
    (["json", "numpy", "shapely"], [], 0),
    (["osgeo.gdal"], [], 1),
    # a Rust kernel links the payload's libgdal
    (["databricks.labs.gbx._rs"], [], 1),
    # imported before activation: registered at once, no hook
    ([], ["rasterio"], 1),
]


@pytest.mark.parametrize("imports, loaded, calls", HOOK_CASES)
def test_the_plugin_hook(monkeypatch, imports, loaded, calls):
    seen = []
    monkeypatch.setattr(
        _rules, "register_hdf5_plugins", lambda root, arch: seen.append(arch) or []
    )
    meta, modules = [], dict.fromkeys(loaded)
    for _ in range(2):  # both activators run: the second adds nothing
        _rules.install_hdf5_hook("/gbx", "x86_64", meta_path=meta, modules=modules)
    for name in imports:
        for finder in list(meta):
            assert finder.find_spec(name, None) is None  # it only watches the import
    assert seen == ["x86_64"] * calls
    assert len(meta) == 1 and meta[0].done is bool(calls)  # one marker per process


def test_the_plugin_registration_never_raises(tmp_path):
    # no payload, and a payload whose MANIFEST names a library that does not load: no registration
    assert _rules.register_hdf5_plugins(str(tmp_path / "none"), "x86_64") is None
    native = tmp_path / "_native" / "x86_64"
    (native / "lib").mkdir(parents=True)
    (native / "hdf5-plugins").mkdir()
    (native / "lib" / "libnetcdf-0a1b2c3d.so.22").write_bytes(b"not a library")
    (native / "MANIFEST.json").write_text(
        '{"sonames": {"libnetcdf.so.22": "libnetcdf-0a1b2c3d.so.22"}}'
    )
    assert _rules.register_hdf5_plugins(str(tmp_path), "x86_64") is None


def _startup_module(monkeypatch):
    """``_gbx_native_path`` from src/, as the .pth imports it (loading it runs it: disabled here)."""
    monkeypatch.setenv("GBX_NATIVE_DISABLE", "1")
    spec = importlib.util.spec_from_file_location(
        "_gbx_native_path_t", SRC / "_gbx_native_path.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.delenv("GBX_NATIVE_DISABLE")
    mod._rules = mod._load_rules(str(SRC))
    return mod


@pytest.mark.parametrize("activator", ["pth", "import"])
@pytest.mark.parametrize(
    "payload_tag, active", [(_rules.CACHE_TAG, True), ("cpython-311", False)]
)
def test_both_activators_apply_the_rules_only_on_cpython_312(
    tmp_path, monkeypatch, activator, payload_tag, active
):
    root = tmp_path / "gbx"
    for d in (
        "_native/x86_64/lib",
        "_native/x86_64/hdf5-plugins",
        "_native/share/gdal",
        "_native/share/proj",
        "_vendor/x86_64",
        "_vendor/common",
    ):
        (root / d).mkdir(parents=True)
    stdlib, site = "/py/lib/python3.12", "/venv/lib/python3.12/site-packages"
    monkeypatch.setattr(sys, "path", [stdlib, site])
    # drop the hook an already activated payload (GBX_NATIVE_ROOT in this process) installed
    monkeypatch.setattr(
        sys,
        "meta_path",
        [f for f in sys.meta_path if not getattr(f, "gbx_hdf5_hook", False)],
    )
    for k in _ENV:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("SSL_CERT_FILE", "/u/ssl.pem")
    monkeypatch.setattr(sys, "platform", "linux")
    if activator == "pth":
        mod = _startup_module(monkeypatch)
        rules = mod._rules
        monkeypatch.setenv("GBX_NATIVE_ROOT", str(root))
        monkeypatch.setattr(
            os, "uname", lambda: types.SimpleNamespace(machine="x86_64")
        )

        def run():
            mod._activate()
            return mod.ACTIVE

    else:
        rules = _rules
        monkeypatch.setattr(_native, "_activated_by", None)

        def run():
            return _native.activate(root=str(root), machine="x86_64")

    # A payload built for another interpreter is what a stray install on 3.11 looks like here.
    monkeypatch.setattr(rules, "CACHE_TAG", payload_tag)
    registered = []
    monkeypatch.setattr(
        rules, "register_hdf5_plugins", lambda r, a: registered.append(a) or []
    )
    assert run() is active
    hooked = any(getattr(f, "gbx_hdf5_hook", False) for f in sys.meta_path)
    # the plugins wait for the first binding import (or register at once when one is loaded already)
    assert (hooked or registered == ["x86_64"]) is active
    if active:
        assert sys.path == [
            stdlib,
            f"{root}/_vendor/x86_64",
            f"{root}/_vendor/common",
            site,
        ]
        assert (
            os.environ["GDAL_CURL_CA_BUNDLE"]
            == os.environ["PROJ_CURL_CA_BUNDLE"]
            == "/u/ssl.pem"
        )
        assert os.environ["PROJ_DATA"] == f"{root}/_native/share/proj"
    else:
        assert sys.path == [stdlib, site]
        assert not {
            "GDAL_DATA",
            "PROJ_DATA",
            "GDAL_CURL_CA_BUNDLE",
            "PROJ_CURL_CA_BUNDLE",
        } & set(os.environ)
    # never process-wide: HDF5_PLUGIN_PATH would reach every HDF5 in the process and every child
    assert "HDF5_PLUGIN_PATH" not in os.environ
    # the HTTP version is GDAL's and curl's choice (or the user's GDAL_HTTP_VERSION); the payload never decides it
    assert "GDAL_HTTP_VERSION" not in os.environ
