"""The env wheel's native payload: where it is, and its activation (spec 5.1; rulings NP-1, NP-2).

Layout under the payload root (``databricks/labs/gbx/`` in an installed env wheel, or
``$GBX_NATIVE_ROOT``, e.g. ``build/env-wheel/env6/payload`` for a source checkout)::

    _native/<arch>/lib/       GDAL, PROJ, GEOS, HDF5, netCDF and the libraries they need (hashed
                              SONAMEs, RUNPATH $ORIGIN) and libgdalalljni.so, the JNI
    _native/<arch>/rs/        the Rust kernels (_gbxrs.abi3.so), when the wheel carries them
    _native/<arch>/MANIFEST.json  the build record: versions, the original -> hashed SONAME map
    _native/share/{gdal,proj} GDAL and PROJ data (identical for both architectures)
    _native/licenses/         the licence of every native library and vendored binding
    _vendor/<arch>/           rasterio, pyogrio, pyproj, shapely, netCDF4 built against _native
    _vendor/common/           rio-tiler, morecantile, mapbox-vector-tile (pure Python)
    _jar/                     the GeoBrix JAR

``<arch>`` is ``x86_64`` or ``aarch64`` (``platform.machine()``). The bindings are served under
their public import names (NP-1): activation puts ``_vendor/<arch>`` and then ``_vendor/common``
at the front of ``sys.path``. It runs at interpreter start through ``geobrix-native.pth``
(``_gbx_native_path``) and again at ``import databricks.labs.gbx`` (NP-2's fallback, for an
environment where ``.pth`` files are not processed); the second run changes nothing.

Activation also sets, in ``os.environ`` and only where the user has set nothing:
``GDAL_DATA`` and ``PROJ_DATA`` (GDAL fixes its data search path at a thread's first lookup, so
only the environment before the first binding import reaches every thread), and the CA bundle
our curl uses (it has no compiled-in CA path): ``GDAL_CURL_CA_BUNDLE`` unless one of
``GDAL_CURL_CA_BUNDLE``, ``CURL_CA_BUNDLE``, ``SSL_CERT_FILE`` is set, and ``PROJ_CURL_CA_BUNDLE``
unless one of ``PROJ_CURL_CA_BUNDLE``, ``CURL_CA_BUNDLE``, ``SSL_CERT_FILE`` is set. The bundle is
the system store (where Databricks admins add corporate CAs), else certifi's. ``CURL_CA_BUNDLE``
itself is never set: every curl user in the process reads it.

Nothing is activated off Linux, on another architecture, or with ``GBX_NATIVE_DISABLE=1``; then
the bindings come from wherever pip installed them. ``GBX_NATIVE_PTH=0`` skips only the startup
(``.pth``) activation.
"""

import glob
import json
import os
import platform
import re
import sys
from typing import List, Optional

_ARCHES = ("x86_64", "aarch64")
_SYSTEM_CA = [
    "/etc/ssl/certs/ca-certificates.crt",  # Debian / Ubuntu (Databricks runtimes)
    "/etc/pki/tls/certs/ca-bundle.crt",  # RHEL / AlmaLinux
    "/etc/ssl/cert.pem",
]
# The CA variables each library reads, highest precedence first (GDAL cpl_http, PROJ filemanager).
_CA_READERS = {
    "GDAL_CURL_CA_BUNDLE": ("GDAL_CURL_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE"),
    "PROJ_CURL_CA_BUNDLE": ("PROJ_CURL_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE"),
}
_activated_by: Optional[str] = None


def payload_root() -> str:
    """``$GBX_NATIVE_ROOT``, else the installed package directory (``databricks/labs/gbx``)."""
    return os.environ.get("GBX_NATIVE_ROOT") or os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )


def arch(machine: Optional[str] = None) -> Optional[str]:
    """The payload architecture for this machine, or None off Linux or on another architecture."""
    if not sys.platform.startswith("linux"):
        return None
    m = machine or platform.machine()
    return m if m in _ARCHES else None


def native_dir(
    root: Optional[str] = None, machine: Optional[str] = None
) -> Optional[str]:
    """``<root>/_native/<arch>`` when it holds ``lib/``: the value of
    ``spark.databricks.labs.gbx.native.dir``. None where the payload cannot load."""
    a = arch(machine)
    d = os.path.join(root or payload_root(), "_native", a) if a else None
    return d if d and os.path.isdir(os.path.join(d, "lib")) else None


def ca_bundle(candidates: Optional[List[str]] = None) -> Optional[str]:
    """The system CA store (Databricks runtimes are Ubuntu; admins add corporate CAs there), else certifi."""
    for p in _SYSTEM_CA if candidates is None else candidates:
        if os.path.isfile(p):
            return p
    try:
        import certifi

        return certifi.where()
    except ImportError:
        return None


def activate(
    root: Optional[str] = None, machine: Optional[str] = None, by: str = "import"
) -> bool:
    """Serve the bindings from the payload and point GDAL and PROJ at its data. Idempotent.

    Returns False (and changes nothing) where the payload cannot load.
    """
    global _activated_by
    if os.environ.get("GBX_NATIVE_DISABLE") == "1":
        return False
    d = native_dir(root, machine)
    if d is None:
        return False
    r, a = root or payload_root(), arch(machine)
    for p in (os.path.join(r, "_vendor", "common"), os.path.join(r, "_vendor", a)):
        if os.path.isdir(p) and p not in sys.path:
            sys.path.insert(0, p)
    share = os.path.join(r, "_native", "share")
    os.environ.setdefault("GDAL_DATA", os.path.join(share, "gdal"))
    os.environ.setdefault("PROJ_DATA", os.path.join(share, "proj"))
    unset = [
        k
        for k, readers in _CA_READERS.items()
        if not any(os.environ.get(n) for n in readers)
    ]
    ca = ca_bundle() if unset else None
    for k in unset if ca else ():
        os.environ[k] = ca
    if _activated_by is None:
        startup = getattr(sys.modules.get("_gbx_native_path"), "ACTIVE", False)
        _activated_by = "pth" if startup else by
    return True


def activated_by() -> Optional[str]:
    """``"pth"`` (interpreter start), ``"import"`` (``import databricks.labs.gbx``), or None."""
    return _activated_by


def _manifest(d: str) -> dict:
    try:
        with open(os.path.join(d, "MANIFEST.json")) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def libgdal_path(
    root: Optional[str] = None, machine: Optional[str] = None
) -> Optional[str]:
    """The payload's libgdal by absolute path (its hashed name, from ``MANIFEST.json``), or None."""
    d = native_dir(root, machine)
    if d is None:
        return None
    names = [
        h
        for o, h in _manifest(d).get("sonames", {}).items()
        if re.fullmatch(r"libgdal\.so\.\d+", o)
    ]
    if (
        not names
    ):  # a tree without the map: libgdal.so.<n> or libgdal-<sha256[:8]>.so.<n>
        pat = re.compile(r"libgdal(-[0-9a-f]{8})?\.so\.\d+")
        names = sorted(
            n for n in os.listdir(os.path.join(d, "lib")) if pat.fullmatch(n)
        )
    hits = [
        os.path.join(d, "lib", n)
        for n in names
        if os.path.isfile(os.path.join(d, "lib", n))
    ]
    return hits[0] if hits else None


def jar_path(root: Optional[str] = None) -> Optional[str]:
    """The JAR the wheel carries (``_jar/geobrix-<version>-jar-with-dependencies.jar``), or None."""
    hits = sorted(
        glob.glob(
            os.path.join(
                root or payload_root(), "_jar", "geobrix-*-jar-with-dependencies.jar"
            )
        )
    )
    return hits[-1] if hits else None
