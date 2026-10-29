"""The payload's activation rules: the one copy both activators use.

Used by ``_gbx_native_path`` (interpreter start, through ``geobrix-native.pth``; it loads this file
by path so that no geobrix package is imported) and by ``databricks.labs.gbx._native.activate``
(``import databricks.labs.gbx``). Pure standard library (``os``, ``sys``); imports nothing from
geobrix; never raises for any input it is given.

The rules:

- **Where the payload can load.** Linux, ``x86_64`` or ``aarch64``, and CPython 3.12 (the
  vendored bindings are ``cpython-312`` extensions; on any other interpreter activation does
  nothing, so a stray install never puts them on ``sys.path``).
- **``sys.path``.** ``_vendor/<arch>`` and then ``_vendor/common`` go just before the first
  ``site-packages`` (or ``dist-packages``) entry: after the standard library, ahead of every
  installed package, so the payload's bindings (which must match its GDAL) win over a user's own
  copies. ``PYTHONPATH`` entries stay ahead of them. With no such entry they are appended.
- **Data.** ``GDAL_DATA`` and ``PROJ_DATA`` point at ``_native/share``, each only when unset;
  ``PROJ_DATA`` is not set when the user set ``PROJ_LIB`` (PROJ's older name, still read).
- **HDF5 filter plugins.** ``HDF5_PLUGIN_PATH`` points at ``_native/<arch>/hdf5-plugins`` (netCDF-C's
  zstd, blosc, bzip2, ... filters) only when unset, so a user's own plugin directory wins. HDF5 and
  netCDF-C read it when they first load a filter, so it is set before any binding is imported.
- **HTTP version.** ``GDAL_HTTP_VERSION`` is never set. Unset, GDAL leaves the version to the
  payload's curl, which offers HTTP/2 to an https server by ALPN (HTTP/1.1 where the server has
  no HTTP/2); a user's value (``1.1``, ``2TLS``, ...) decides it.
- **CA.** For each of ``GDAL_CURL_CA_BUNDLE`` and ``PROJ_CURL_CA_BUNDLE``: a value the user set is
  never touched (even an empty one); otherwise it gets the user's ``CURL_CA_BUNDLE``, else the
  user's ``SSL_CERT_FILE`` (a non-empty value), else the first system store that exists, else the
  caller's fallback (the import-time activation passes certifi's). Setting them before any binding
  import matters: rasterio and pyogrio set both to certifi's bundle at their own import when they
  are unset, and GDAL and PROJ read these names before ``CURL_CA_BUNDLE`` and ``SSL_CERT_FILE``.
  ``CURL_CA_BUNDLE`` itself is never set: every curl user in the process reads it.
"""

import os
import sys

ARCHES = ("x86_64", "aarch64")
CACHE_TAG = "cpython-312"  # the vendored bindings' extension modules: *.cpython-312-<arch>-linux-gnu.so
SYSTEM_CA = (
    "/etc/ssl/certs/ca-certificates.crt",  # Debian / Ubuntu (Databricks runtimes)
    "/etc/pki/tls/certs/ca-bundle.crt",  # RHEL / AlmaLinux
    "/etc/ssl/cert.pem",
)
CA_VARS = ("GDAL_CURL_CA_BUNDLE", "PROJ_CURL_CA_BUNDLE")
USER_CA = ("CURL_CA_BUNDLE", "SSL_CERT_FILE")  # in the order they are taken
SITE_DIRS = ("site-packages", "dist-packages")


def interpreter_ok(cache_tag=None):
    """True on the interpreter the vendored bindings were built for."""
    tag = (
        getattr(sys.implementation, "cache_tag", None)
        if cache_tag is None
        else cache_tag
    )
    return tag == CACHE_TAG


def insert_vendor(path, dirs):
    """Put ``dirs`` (in order, the existing ones not yet on ``path``) just before the first
    site-packages entry of the list ``path``, else at its end. Returns the dirs inserted.
    """
    new = [d for d in dirs if os.path.isdir(d) and d not in path]
    at = next(
        (
            i
            for i, p in enumerate(path)
            if os.path.basename(str(p).rstrip("/")) in SITE_DIRS
        ),
        len(path),
    )
    path[at:at] = new
    return new


def set_data_dirs(env, share):
    env.setdefault("GDAL_DATA", os.path.join(share, "gdal"))
    if "PROJ_LIB" not in env:
        env.setdefault("PROJ_DATA", os.path.join(share, "proj"))


def set_plugin_path(env, plugins):
    """``HDF5_PLUGIN_PATH`` at the payload's filter plugins, when unset and they exist."""
    if os.path.isdir(plugins):
        env.setdefault("HDF5_PLUGIN_PATH", plugins)


def ca_for(env, candidates=SYSTEM_CA, fallback=None):
    """The bundle our GDAL and PROJ get when their own variable is unset, or None."""
    for name in USER_CA:
        if env.get(name):
            return env[name]
    for p in candidates:
        if os.path.isfile(p):
            return p
    try:
        return fallback() if fallback else None
    except Exception:  # noqa: BLE001 - a fallback (certifi) that fails gives no bundle
        return None


def set_ca(env, candidates=SYSTEM_CA, fallback=None):
    unset = [v for v in CA_VARS if v not in env]
    ca = ca_for(env, candidates, fallback) if unset else None
    for v in unset if ca else ():
        env[v] = ca


def activate(root, arch, path=None, env=None, candidates=SYSTEM_CA, fallback=None):
    """Apply the rules for the payload at ``root`` on ``arch``; False (nothing changed) where the
    payload cannot load: another interpreter, another architecture, or no ``_native/<arch>/lib``.
    """
    path = sys.path if path is None else path
    env = os.environ if env is None else env
    if (
        env.get("GBX_NATIVE_DISABLE") == "1"
        or not interpreter_ok()
        or arch not in ARCHES
    ):
        return False
    if not os.path.isdir(os.path.join(root, "_native", arch, "lib")):
        return False
    insert_vendor(
        path,
        [os.path.join(root, "_vendor", arch), os.path.join(root, "_vendor", "common")],
    )
    set_data_dirs(env, os.path.join(root, "_native", "share"))
    set_plugin_path(env, os.path.join(root, "_native", arch, "hdf5-plugins"))
    set_ca(env, candidates, fallback)
    return True
