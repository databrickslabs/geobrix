"""The payload's activation rules: the one copy both activators use.

Used by ``_gbx_native_path`` (interpreter start, through ``geobrix-native.pth``; it loads this file
by path so that no geobrix package is imported) and by ``databricks.labs.gbx._native.activate``
(``import databricks.labs.gbx``). Pure standard library (``os``, ``sys``; ``json`` and ``ctypes``
only when the filter plugins are registered; ``sqlite3`` only when ``PROJ_DATA``/``PROJ_LIB`` name a
``proj.db``); imports nothing from geobrix; never raises for any input it is given.

The rules:

- **Where the payload can load.** Linux, ``x86_64`` or ``aarch64``, and CPython 3.12 (the
  vendored bindings are ``cpython-312`` extensions; on any other interpreter activation does
  nothing, so a stray install never puts them on ``sys.path``).
- **``sys.path``.** ``_vendor/<arch>`` and then ``_vendor/common`` go just before the first
  ``site-packages`` (or ``dist-packages``) entry: after the standard library, ahead of every
  installed package, so the payload's bindings (which must match its GDAL) win over a user's own
  copies. ``PYTHONPATH`` entries stay ahead of them. With no such entry they are appended.
- **Data.** ``GDAL_DATA`` and ``PROJ_DATA`` point at ``_native/share``, each only when unset;
  ``PROJ_DATA`` is not set when the user's ``PROJ_LIB`` (PROJ's older name, still read) holds a usable
  ``proj.db``. A ``PROJ_DATA`` or ``PROJ_LIB`` drops the entries that exist but this process cannot read (DBR 19's
  ``/databricks/native/proj-data`` on a standard-access driver) and the ones whose ``proj.db`` is another PROJ's
  (its ``metadata`` layout version, or ``PROJ.VERSION``, is not the payload ``proj.db``'s: DBR 19's same directory
  on a dedicated cluster, where the payload's PROJ finds no usable database in it), and keeps every other one in
  order, grid directories included; a ``PROJ_LIB`` left with none goes. A ``PROJ_DATA`` left with no ``proj.db``
  gets the user's ``PROJ_LIB`` grid and data directories appended, then ``_native/share/proj`` unless one of those
  holds a ``proj.db``; with no ``PROJ_DATA``, ``PROJ_DATA`` is the user's ``PROJ_LIB`` grid directories followed by
  ``_native/share/proj``. A value with a usable ``proj.db`` and nothing dropped is never changed. The probes never
  raise.
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


PROJ_DB_KEYS = (
    "DATABASE.LAYOUT.VERSION.MAJOR",
    "DATABASE.LAYOUT.VERSION.MINOR",
    "PROJ.VERSION",
)


def proj_db_version(db):
    """``(layout major, layout minor, PROJ.VERSION or None)`` from a ``proj.db``'s ``metadata`` table, or None when
    it is not a readable PROJ database. Read-only and immutable (no lock, no journal: the file may be on a read-only
    file system). Never raises."""
    try:
        import sqlite3
        from urllib.parse import quote

        con = sqlite3.connect(f"file:{quote(str(db))}?mode=ro&immutable=1", uri=True)
        try:
            rows = dict(
                con.execute(
                    "SELECT key, value FROM metadata WHERE key IN (?, ?, ?)",
                    PROJ_DB_KEYS,
                ).fetchall()
            )
        finally:
            con.close()
    except Exception:
        return None
    if PROJ_DB_KEYS[0] not in rows or PROJ_DB_KEYS[1] not in rows:
        return None
    return tuple(rows.get(k) for k in PROJ_DB_KEYS)


def _same_proj_db(version, ref):
    """``version`` (``proj_db_version``) is the payload's: the same layout, and the same PROJ.VERSION where both
    carry one."""
    return (
        version is not None
        and version[:2] == ref[:2]
        and (version[2] is None or ref[2] is None or version[2] == ref[2])
    )


def lazy_proj_ref(db):
    """A ``ref`` for ``proj_entry`` that reads the payload ``proj.db`` (``proj_db_version``) on first use and then
    remembers it, so an interpreter whose PROJ variables name no ``proj.db`` never opens (or imports ``sqlite3`` for)
    the payload's."""
    memo = []

    def ref():
        if not memo:
            memo.append(proj_db_version(db))
        return memo[0]

    return ref


def proj_entry(d, ref=None):
    """What a ``PROJ_DATA``/``PROJ_LIB`` entry is to this process: ``"db"`` (a directory holding a readable
    ``proj.db``), ``"dir"`` (a readable directory without one: a grid directory), ``"missing"`` (nothing there),
    ``"unreadable"`` (there, but this process cannot read it or its ``proj.db``: the entry pyproj's ``Path.exists()``
    raises on, EACCES), or, given ``ref`` (the payload ``proj.db``'s ``proj_db_version``, or a callable returning it,
    called only once a ``proj.db`` is found), ``"foreign"`` (a readable ``proj.db`` that is not the payload's
    version, which the payload's PROJ cannot use). Never raises.
    """
    try:
        os.stat(d)
    except (FileNotFoundError, NotADirectoryError):
        return "missing"
    except (OSError, ValueError):
        return "unreadable"
    try:
        if os.path.isdir(d) and not os.access(d, os.R_OK | os.X_OK):
            return "unreadable"
        db = os.path.join(d, "proj.db")
        try:
            os.stat(db)
        except (FileNotFoundError, NotADirectoryError):
            return "dir"
        if not os.access(db, os.R_OK):
            return "unreadable"
        if callable(ref):
            ref = ref()
        if ref is not None and not _same_proj_db(proj_db_version(db), ref):
            return "foreign"
        return "db"
    except (OSError, ValueError):
        return "unreadable"


def proj_db_readable(value):
    """True when a directory of ``value`` (``os.pathsep``-joined) holds a ``proj.db`` this process can read."""
    return any(proj_entry(d) == "db" for d in (value or "").split(os.pathsep) if d)


def usable_proj_dirs(value, ref=None):
    """The entries of ``value`` (``os.pathsep``-joined), in order, but those that exist and this process cannot read
    and, given ``ref``, those holding another PROJ's ``proj.db``."""
    return [
        d
        for d in (value or "").split(os.pathsep)
        if d and proj_entry(d, ref) not in ("unreadable", "foreign")
    ]


def set_data_dirs(env, share):
    env.setdefault("GDAL_DATA", os.path.join(share, "gdal"))
    # PROJ_DATA / PROJ_LIB. DBR 19 exports PROJ_DATA=/databricks/native/proj-data. A standard-access notebook user
    # cannot read it, and the vendored pyproj (no data dir of its own) checks each entry in order with
    # Path.exists(), which raises on EACCES; on a dedicated cluster it is readable but its proj.db is the runtime's
    # PROJ's, and the payload's PROJ then finds no usable database ("no database context specified"). So an entry
    # that exists but cannot be read goes, and so does one whose proj.db is not the payload's version; every other
    # entry stays, in order (a grid directory holds no proj.db, and a missing one costs nothing). A PROJ_LIB left
    # empty goes. Then PROJ must still find a proj.db, and PROJ reads PROJ_DATA before PROJ_LIB: a PROJ_DATA left
    # with none gets PROJ_LIB's grid and data directories, then the payload's share/proj unless PROJ_LIB has a
    # proj.db; a PROJ_DATA left empty goes; with no PROJ_DATA and no usable PROJ_LIB proj.db, PROJ_DATA is
    # PROJ_LIB's grid directories followed by the payload's share/proj. A value with a usable proj.db and nothing
    # dropped is kept as it is.
    proj = os.path.join(share, "proj")
    ref = lazy_proj_ref(
        os.path.join(proj, "proj.db")
    )  # read on first use; None (no payload proj.db): no version check
    lib = []
    if "PROJ_LIB" in env:
        lib = usable_proj_dirs(env["PROJ_LIB"], ref)
        if not lib:
            del env["PROJ_LIB"]
        elif lib != env["PROJ_LIB"].split(os.pathsep):
            env["PROJ_LIB"] = os.pathsep.join(lib)
    kinds = {d: proj_entry(d, ref) for d in lib}
    lib_db = [d for d in lib if kinds[d] == "db"]
    lib_dirs = [d for d in lib if kinds[d] in ("db", "dir")]
    if "PROJ_DATA" in env:
        data = usable_proj_dirs(env["PROJ_DATA"], ref)
        if not data:
            del env["PROJ_DATA"]
        else:
            if not any(proj_entry(d, ref) == "db" for d in data):
                data = (
                    data
                    + [d for d in lib_dirs if d not in data]
                    + ([] if lib_db else [proj])
                )
            if data != env["PROJ_DATA"].split(os.pathsep):
                env["PROJ_DATA"] = os.pathsep.join(data)
    if "PROJ_DATA" not in env and not lib_db:
        env["PROJ_DATA"] = os.pathsep.join(
            [d for d in lib if kinds[d] == "dir"] + [proj]
        )


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
