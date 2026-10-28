"""The payload is activated only where its libraries can load (Linux, a known arch), and only once.

A fake payload in the env wheel's layout: ``_native/<arch>/lib`` holds the libraries under the
hashed names ``MANIFEST.json`` maps (never a hard-coded file name), ``_native/share`` the GDAL and
PROJ data, ``_vendor/<arch>`` and ``_vendor/common`` the bindings, ``_jar/`` the JAR. Activation
puts the vendored trees at the front of ``sys.path`` and sets the data and CA variables only
where the user has set nothing; on macOS, an unknown arch or with ``GBX_NATIVE_DISABLE=1`` it
changes nothing.
"""

import json
import os
import sys

from databricks.labs.gbx import _native

_ENV = (
    "GDAL_DATA",
    "PROJ_DATA",
    "GDAL_CURL_CA_BUNDLE",
    "PROJ_CURL_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "SSL_CERT_FILE",
    "GBX_NATIVE_DISABLE",
    "GBX_NATIVE_ROOT",
)


def _payload(root):
    lib = root / "_native" / "x86_64" / "lib"
    lib.mkdir(parents=True)
    (lib / "libgdal-0a1b2c3d.so.39").write_bytes(b"\x7fELF")
    (lib / "libgdalalljni.so").write_bytes(b"\x7fELF")
    manifest = {
        "jni": "libgdalalljni.so",
        "sonames": {"libgdal.so.39": "libgdal-0a1b2c3d.so.39"},
    }
    (root / "_native" / "x86_64" / "MANIFEST.json").write_text(json.dumps(manifest))
    for d in (
        "_native/share/gdal",
        "_native/share/proj",
        "_vendor/x86_64",
        "_vendor/common",
        "_jar",
    ):
        (root / d).mkdir(parents=True)
    (root / "_jar" / "geobrix-0.6.0-jar-with-dependencies.jar").write_bytes(b"PK")
    return root


def _state():
    return list(sys.path), {k: os.environ.get(k) for k in _ENV}


def test_the_payload_is_used_only_where_it_can_load(tmp_path, monkeypatch):
    root = _payload(tmp_path / "gbx")
    bundle = tmp_path / "ca-certificates.crt"
    bundle.write_text("-----BEGIN CERTIFICATE-----\n")
    monkeypatch.setattr(sys, "path", list(sys.path))
    for k in _ENV:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(_native, "_SYSTEM_CA", [str(bundle)])
    monkeypatch.setattr(_native, "_activated_by", None)
    monkeypatch.setattr(sys, "platform", "linux")
    r = str(root)

    # Linux x86_64: the arch tree, then the common tree, at the front; data and CA set when unset,
    # except a CA the user chose (a pre-set GDAL_CURL_CA_BUNDLE survives).
    monkeypatch.setenv("GDAL_CURL_CA_BUNDLE", "/user/corporate.pem")
    assert _native.activate(root=r, machine="x86_64") is True
    assert sys.path[:2] == [f"{r}/_vendor/x86_64", f"{r}/_vendor/common"]
    assert os.environ["GDAL_DATA"] == f"{r}/_native/share/gdal"
    assert os.environ["PROJ_DATA"] == f"{r}/_native/share/proj"
    assert os.environ["GDAL_CURL_CA_BUNDLE"] == "/user/corporate.pem"
    assert os.environ["PROJ_CURL_CA_BUNDLE"] == str(bundle)
    assert (
        "CURL_CA_BUNDLE" not in os.environ
    )  # never set: it is every curl user's in the process
    assert _native.activated_by() == "import"
    assert _native.native_dir(r, "x86_64") == f"{r}/_native/x86_64"
    assert (
        _native.libgdal_path(r, "x86_64")
        == f"{r}/_native/x86_64/lib/libgdal-0a1b2c3d.so.39"
    )
    assert _native.jar_path(r) == f"{r}/_jar/geobrix-0.6.0-jar-with-dependencies.jar"

    # A second call changes nothing.
    before = _state()
    assert _native.activate(root=r, machine="x86_64") is True
    assert _state() == before

    # A user CA in the variables both GDAL and PROJ read: neither of ours is set.
    for k in ("GDAL_CURL_CA_BUNDLE", "PROJ_CURL_CA_BUNDLE"):
        monkeypatch.delenv(k)
    monkeypatch.setenv("SSL_CERT_FILE", "/user/corporate.pem")
    _native.activate(root=r, machine="x86_64")
    assert (
        "GDAL_CURL_CA_BUNDLE" not in os.environ
        and "PROJ_CURL_CA_BUNDLE" not in os.environ
    )
    monkeypatch.delenv("SSL_CERT_FILE")

    # macOS (arm64), an unknown arch and GBX_NATIVE_DISABLE=1: nothing changes.
    before = _state()
    monkeypatch.setattr(sys, "platform", "darwin")
    assert _native.activate(root=r, machine="arm64") is False
    assert (
        _native.native_dir(r, "arm64") is None
        and _native.libgdal_path(r, "arm64") is None
    )
    monkeypatch.setattr(sys, "platform", "linux")
    assert _native.activate(root=r, machine="riscv64") is False
    monkeypatch.setenv("GBX_NATIVE_DISABLE", "1")
    assert _native.activate(root=r, machine="x86_64") is False
    monkeypatch.delenv("GBX_NATIVE_DISABLE")
    assert _state() == before

    # The CA bundle: the first existing candidate, else certifi's.
    assert _native.ca_bundle([str(tmp_path / "missing.pem"), str(bundle)]) == str(
        bundle
    )
    try:  # the heavy CI environment has no certifi: then there is no bundle to fall back to
        import certifi

        fallback = certifi.where()
    except ImportError:
        fallback = None
    assert _native.ca_bundle([]) == fallback
