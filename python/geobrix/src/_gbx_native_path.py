"""Startup activation of the GeoBrix env wheel's native payload (ruling NP-2).

Imported at interpreter start by ``geobrix-native.pth`` (one line: ``import _gbx_native_path``),
which the env wheel installs beside this module in site-packages. Path arithmetic only: it
imports nothing from geobrix and never raises. Off Linux, on an unknown architecture, with
``GBX_NATIVE_DISABLE=1`` or ``GBX_NATIVE_PTH=0``, or without the payload, it does nothing.
Otherwise it puts ``databricks/labs/gbx/_vendor/<arch>`` and then ``_vendor/common`` at the front
of ``sys.path`` (so ``import rasterio`` serves the payload's build even before geobrix is
imported), sets ``GDAL_DATA`` and ``PROJ_DATA`` and, from the system CA store, the GDAL and PROJ
CA bundles, each only where the user has set nothing (the rules of ``databricks.labs.gbx._native``,
which repeats this at import and falls back to certifi), and sets ``ACTIVE = True``.
"""

import os
import sys

ACTIVE = False


def _activate():
    global ACTIVE
    env = os.environ
    if env.get("GBX_NATIVE_DISABLE") == "1" or env.get("GBX_NATIVE_PTH") == "0":
        return
    if not sys.platform.startswith("linux"):
        return
    arch = os.uname().machine
    if arch not in ("x86_64", "aarch64"):
        return
    here = os.path.dirname(os.path.abspath(__file__))
    root = env.get("GBX_NATIVE_ROOT") or os.path.join(here, "databricks", "labs", "gbx")
    if not os.path.isdir(os.path.join(root, "_native", arch, "lib")):
        return
    for p in (
        os.path.join(root, "_vendor", "common"),
        os.path.join(root, "_vendor", arch),
    ):
        if os.path.isdir(p) and p not in sys.path:
            sys.path.insert(0, p)
    share = os.path.join(root, "_native", "share")
    env.setdefault("GDAL_DATA", os.path.join(share, "gdal"))
    env.setdefault("PROJ_DATA", os.path.join(share, "proj"))
    # Before any binding import: rasterio and pyogrio set both CA variables to certifi's bundle
    # at their own import when unset, which would drop the system store's corporate CAs.
    ca = next(
        (
            p
            for p in (
                "/etc/ssl/certs/ca-certificates.crt",
                "/etc/pki/tls/certs/ca-bundle.crt",
                "/etc/ssl/cert.pem",
            )
            if os.path.isfile(p)
        ),
        None,
    )
    for var in ("GDAL_CURL_CA_BUNDLE", "PROJ_CURL_CA_BUNDLE") if ca else ():
        if not any(env.get(n) for n in (var, "CURL_CA_BUNDLE", "SSL_CERT_FILE")):
            env[var] = ca
    ACTIVE = True


try:
    _activate()
except Exception:  # noqa: BLE001 - a .pth import must never stop the interpreter
    pass
