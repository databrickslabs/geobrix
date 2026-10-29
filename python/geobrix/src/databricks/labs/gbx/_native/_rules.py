"""The payload's activation rules: the one copy both activators use.

Used by ``_gbx_native_path`` (interpreter start, through ``geobrix-native.pth``; it loads this file
by path so that no geobrix package is imported) and by ``databricks.labs.gbx._native.activate``
(``import databricks.labs.gbx``). Pure standard library (``os``, ``sys``; ``json`` and ``ctypes``
only when the filter plugins are registered); imports nothing from geobrix; never raises for any
input it is given.

The rules:

- **Where the payload can load.** Linux, ``x86_64`` or ``aarch64``, and CPython 3.12 (the
  vendored bindings are ``cpython-312`` extensions; on any other interpreter activation does
  nothing, so a stray install never puts them on ``sys.path``).
- **``sys.path``.** ``_vendor/<arch>`` and then ``_vendor/common`` go just before the first
  ``site-packages`` (or ``dist-packages``) entry: after the standard library, ahead of every
  installed package, so the payload's bindings (which must match its GDAL) win over a user's own
  copies. ``PYTHONPATH`` entries stay ahead of them. With no such entry they are appended.
- **Data.** ``GDAL_DATA`` and ``PROJ_DATA`` point at ``_native/share``, each only when unset;
  ``PROJ_DATA`` is not set when the user set ``PROJ_LIB`` (PROJ's older name, still read). A ``PROJ_DATA`` or
  ``PROJ_LIB`` keeps only its directories that hold a ``proj.db`` this process can read, in order (DBR 19's
  ``/databricks/native/proj-data`` on a standard-access driver is not one); a ``PROJ_DATA`` with none counts as
  unset and is removed, and an all-readable value is never changed. The readability probe never raises.
- **HDF5 filter plugins.** ``_native/<arch>/hdf5-plugins`` (netCDF-C's zstd, blosc, bzip2, szip,
  ... filters) is registered with the payload's OWN netCDF-C and HDF5, never through the
  environment: ``HDF5_PLUGIN_PATH`` is never set, so it reaches no other HDF5 in the process (a
  user's h5py) and no child process. Activation installs a meta-path hook that only watches: on the
  first import of a payload binding that reaches the payload's HDF5 (netCDF4, rasterio, osgeo,
  pyogrio, a Rust kernel) it loads the payload's libnetcdf, calls ``nc_initialize()`` (which sets
  HDF5's plugin path from the user's ``HDF5_PLUGIN_PATH``, else netCDF-C's compiled-in default),
  appends the payload's directory through netCDF-C's own plugin-path API (which syncs HDF5 and
  NCZarr), and from then on does nothing (it stays as the marker that registration happened). A user's ``HDF5_PLUGIN_PATH`` directories therefore come first and
  the payload's filters fill the gaps; ``GBX_NATIVE_DISABLE=1`` turns all of it off.
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
# The payload's bindings whose import loads the payload's libhdf5 (directly, or through libgdal): the first one
# imported registers the filter plugins. A Rust kernel (databricks.labs.gbx._rs) links libgdal too.
HDF5_USERS = ("netCDF4", "rasterio", "osgeo", "pyogrio")
HDF5_USERS_FULL = ("databricks.labs.gbx._rs",)


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


def proj_db_readable(value):
    """True when a directory of ``value`` (``os.pathsep``-joined) holds a ``proj.db`` this process can read.
    Never raises: a probe that fails (EACCES and the like) means not readable."""
    for d in (value or "").split(os.pathsep):
        if not d:
            continue
        db = os.path.join(d, "proj.db")
        try:
            if os.path.isfile(db) and os.access(db, os.R_OK):
                return True
        except (OSError, ValueError):
            continue
    return False


def readable_proj_dirs(value):
    """The directories of ``value`` (``os.pathsep``-joined), in order, that hold a ``proj.db`` this process can read."""
    return [d for d in (value or "").split(os.pathsep) if d and proj_db_readable(d)]


def set_data_dirs(env, share):
    env.setdefault("GDAL_DATA", os.path.join(share, "gdal"))
    # A PROJ_DATA or PROJ_LIB directory with no proj.db this process can read is no PROJ data: DBR 19 exports
    # PROJ_DATA=/databricks/native/proj-data, which the notebook user of a standard-access cluster cannot read, and
    # the vendored pyproj (no data dir of its own) checks each entry in order with Path.exists(), which raises on
    # EACCES. So only the readable entries stay, in order; a PROJ_DATA with none counts as unset (and goes), and a
    # PROJ_LIB with none is left to PROJ_DATA, which PROJ and pyproj read first. A readable value is kept as it is.
    for key in ("PROJ_DATA", "PROJ_LIB"):
        if key in env:
            keep = readable_proj_dirs(env[key])
            if keep:
                if keep != env[key].split(os.pathsep):
                    env[key] = os.pathsep.join(keep)
            elif key == "PROJ_DATA":
                del env["PROJ_DATA"]
    if not ("PROJ_LIB" in env and readable_proj_dirs(env["PROJ_LIB"])):
        env.setdefault("PROJ_DATA", os.path.join(share, "proj"))


def payload_lib(root, arch, soname_prefix):
    """The payload library whose original SONAME starts with ``soname_prefix`` (``libnetcdf.so.``), by its
    hashed name from ``_native/<arch>/MANIFEST.json``; None when there is none."""
    import json

    native = os.path.join(root, "_native", arch)
    with open(os.path.join(native, "MANIFEST.json")) as fh:
        names = json.load(fh).get("sonames", {})
    hit = next(
        (new for old, new in names.items() if old.startswith(soname_prefix)), None
    )
    return os.path.join(native, "lib", hit) if hit else None


def register_hdf5_plugins(root, arch):
    """Append ``_native/<arch>/hdf5-plugins`` to the plugin path of the payload's own netCDF-C (which
    syncs it to the payload's HDF5 and to NCZarr), after netCDF-C has set its initial path. Returns the
    plugin path now in force (a list of directories), or None where the payload has no plugins or
    they cannot be registered. Idempotent; never raises."""
    try:
        plugins = os.path.join(root, "_native", arch, "hdf5-plugins")
        lib = (
            payload_lib(root, arch, "libnetcdf.so.") if os.path.isdir(plugins) else None
        )
        if lib is None:
            return None
        import ctypes

        nc = ctypes.CDLL(lib)
        if nc.nc_initialize() != 0:
            return None
        n = nc.ncaux_plugin_path_stringlen()
        buf = ctypes.create_string_buffer(max(n, 0) + 1)
        if n > 0 and nc.ncaux_plugin_path_stringget(len(buf), buf) != 0:
            return None
        dirs = [d for d in buf.value.decode(errors="replace").split(";") if d]
        if plugins not in dirs:
            dirs.append(plugins)
            joined = ";".join(dirs).encode()
            if nc.ncaux_plugin_path_stringset(len(joined), joined) != 0:
                return None
        return dirs
    except (
        Exception
    ):  # noqa: BLE001 - activation never raises; a filtered read then names the missing filter
        return None


class _Hdf5PluginHook:
    """A ``sys.meta_path`` finder that only watches: the first import of a payload binding that reaches
    the payload's HDF5 registers the filter plugins. It finds nothing, and once done it stays as the
    process's marker, so the second activator (the .pth one, then the import-time one) adds nothing.
    """

    gbx_hdf5_hook = True

    def __init__(self, root, arch, done=False):
        self.root, self.arch, self.done = root, arch, done

    def find_spec(self, name, path=None, target=None):
        if not self.done and (
            name.partition(".")[0] in HDF5_USERS or name in HDF5_USERS_FULL
        ):
            self.done = True
            register_hdf5_plugins(self.root, self.arch)
        return None


def install_hdf5_hook(root, arch, meta_path=None, modules=None):
    """Register the filter plugins now when a binding that reaches the payload's HDF5 is already
    imported, else install the hook that registers them on the first such import (once).
    """
    meta_path = sys.meta_path if meta_path is None else meta_path
    modules = sys.modules if modules is None else modules
    if any(getattr(f, "gbx_hdf5_hook", False) for f in meta_path):
        return
    loaded = any(m in modules for m in HDF5_USERS + HDF5_USERS_FULL)
    meta_path.insert(0, _Hdf5PluginHook(root, arch, done=loaded))
    if loaded:
        register_hdf5_plugins(root, arch)


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
    set_ca(env, candidates, fallback)
    install_hdf5_hook(root, arch)
    return True
