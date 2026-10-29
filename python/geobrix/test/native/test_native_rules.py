"""The activation rules (``_native/_rules.py``): the one copy both activators use.

The CA rows: a user's ``GDAL_CURL_CA_BUNDLE``/``PROJ_CURL_CA_BUNDLE`` is never touched; otherwise
both get the user's ``CURL_CA_BUNDLE``, else the user's ``SSL_CERT_FILE``, else the system store;
``CURL_CA_BUNDLE`` is never set. The data rows: ``GDAL_DATA``/``PROJ_DATA`` only when unset, and no
``PROJ_DATA`` over a user's ``PROJ_LIB``. The plugin rows: the payload's HDF5 filter plugins are
registered with the payload's own netCDF-C/HDF5 on the first import of a binding that reaches them,
once, and ``HDF5_PLUGIN_PATH`` is never set. Then both activators (the startup module, loaded from
``src/`` by path as the ``.pth`` imports it, and the import-time ``_native.activate``) apply the
rules (and leave ``GDAL_HTTP_VERSION`` to GDAL's default), and do nothing at all on an interpreter
other than the payload's CPython 3.12.
"""

import importlib.util
import os
import sys
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


DATA_CASES = [  # the user's environment, GDAL_DATA after, PROJ_DATA after
    ({}, "<share>/gdal", "<share>/proj"),
    ({"PROJ_LIB": "/u/proj"}, "<share>/gdal", None),
    ({"GDAL_DATA": "/u/gdal", "PROJ_DATA": "/u/proj"}, "/u/gdal", "/u/proj"),
]


@pytest.mark.parametrize("environ, gdal_data, proj_data", DATA_CASES)
def test_the_data_rule(environ, gdal_data, proj_data):
    env = dict(environ)
    _rules.set_data_dirs(env, "<share>")
    assert (env.get("GDAL_DATA"), env.get("PROJ_DATA")) == (gdal_data, proj_data)
    assert env.get("PROJ_LIB") == environ.get("PROJ_LIB")


HOOK_CASES = [  # modules imported after activation, modules imported before it, registrations
    (
        ["json", "netCDF4", "rasterio"],
        [],
        1,
    ),  # the first binding that reaches the payload's HDF5, once
    (["json", "numpy", "shapely"], [], 0),  # none of them reaches it: nothing is loaded
    (["osgeo.gdal"], [], 1),
    (["databricks.labs.gbx._rs"], [], 1),  # a Rust kernel links the payload's libgdal
    ([], ["rasterio"], 1),  # imported before activation: registered at once, no hook
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
    monkeypatch.setattr(sys, "meta_path", list(sys.meta_path))
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
