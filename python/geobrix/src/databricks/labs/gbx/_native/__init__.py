"""The env wheel's native payload: where it is, and its activation.

Layout under the payload root (``databricks/labs/gbx/`` in an installed env wheel, or
``$GBX_NATIVE_ROOT``, e.g. ``build/env-wheel/env6/payload`` for a source checkout)::

    _native/<arch>/lib/       GDAL, PROJ, GEOS, HDF5, netCDF and the libraries they need (hashed
                              SONAMEs, RUNPATH $ORIGIN) and libgdalalljni.so, the JNI
    _native/<arch>/rs/        the Rust kernels (_gbxrs.abi3.so), when the wheel carries them
    _native/<arch>/hdf5-plugins/  netCDF-C's HDF5 filter plugins (zstd, blosc, bzip2, ...):
                              ``HDF5_PLUGIN_PATH`` points here unless the user set it
    _native/<arch>/bin/projsync   PROJ's grid downloader (``projsync_path()``)
    _native/<arch>/MANIFEST.json  the build record: versions, the original -> hashed SONAME map,
                              the build-id of every file (its debug symbols are a separate tarball)
    _native/share/{gdal,proj} GDAL and PROJ data (identical for both architectures); share/proj
                              holds the OSTN15, OSGM15 and EGM96 grids
    _native/licenses/         the licence of every native library and vendored binding
    _vendor/<arch>/           rasterio, pyogrio, pyproj, shapely, netCDF4 built against _native,
                              and GDAL's own osgeo from the same build
    _vendor/common/           rio-tiler, morecantile, mapbox-vector-tile, osgeo_utils (pure Python)
    _jar/                     the GeoBrix JAR

``<arch>`` is ``x86_64`` or ``aarch64`` (``platform.machine()``). The bindings are served under
their public import names: activation puts ``_vendor/<arch>`` and then ``_vendor/common``
just before the first site-packages entry of ``sys.path`` (after the standard library, ahead of
every installed package). It runs at interpreter start through ``geobrix-native.pth``
(``_gbx_native_path``) and again at ``import databricks.labs.gbx`` (the fallback, for an
environment where ``.pth`` files are not processed); the second run changes nothing. Both apply
the one set of rules in ``_native/_rules.py``: where the payload can load (Linux, x86_64 or
aarch64, CPython 3.12), the ``sys.path`` position, ``GDAL_DATA``/``PROJ_DATA`` (never over a
user's value, nor ``PROJ_DATA`` over a user's ``PROJ_LIB``) and the CA bundle our curl uses (it
has no compiled-in CA path): a user's ``GDAL_CURL_CA_BUNDLE``/``PROJ_CURL_CA_BUNDLE`` is never
touched; otherwise they get the user's ``CURL_CA_BUNDLE`` or ``SSL_CERT_FILE``, else the system
store (where Databricks admins add corporate CAs), else certifi's (this import-time activation
only). ``CURL_CA_BUNDLE`` itself is never set.

Nothing is activated off Linux, on another architecture or interpreter, or with
``GBX_NATIVE_DISABLE=1``; then the bindings come from wherever pip installed them.
``GBX_NATIVE_PTH=0`` skips only the startup (``.pth``) activation.
"""

import glob
import json
import os
import platform
import re
import sys
from typing import List, Optional

from databricks.labs.gbx._native import _rules

_ARCHES = _rules.ARCHES
_SYSTEM_CA = list(_rules.SYSTEM_CA)
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


def _certifi() -> str:
    import certifi

    return certifi.where()


def ca_bundle(candidates: Optional[List[str]] = None) -> Optional[str]:
    """The system CA store (Databricks runtimes are Ubuntu; admins add corporate CAs there), else certifi."""
    return _rules.ca_for({}, _SYSTEM_CA if candidates is None else candidates, _certifi)


def activate(
    root: Optional[str] = None, machine: Optional[str] = None, by: str = "import"
) -> bool:
    """Serve the bindings from the payload and point GDAL and PROJ at its data. Idempotent.

    Returns False (and changes nothing) where the payload cannot load.
    """
    global _activated_by
    a = arch(machine)
    if a is None:
        return False
    if not _rules.activate(
        root or payload_root(), a, candidates=_SYSTEM_CA, fallback=_certifi
    ):
        return False
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


def projsync_path(
    root: Optional[str] = None, machine: Optional[str] = None
) -> Optional[str]:
    """PROJ's grid downloader from the payload (``_native/<arch>/bin/projsync``), or None.

    It fetches grids from the PROJ CDN into a directory, e.g. ``[projsync_path(), "--source-id",
    "uk_os", "--target-dir", "/Volumes/<catalog>/<schema>/proj-grids"]`` with ``subprocess``;
    register that directory with ``register_proj_grids``. Needs network access to the CDN.
    """
    d = native_dir(root, machine)
    p = os.path.join(d, "bin", "projsync") if d else None
    return p if p and os.access(p, os.X_OK) else None


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
