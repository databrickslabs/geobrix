"""A GeoBrix Rust kernel links the payload's GDAL, PROJ and GEOS and shares one GDAL with rasterio and osgeo.

The probe extension (``rust/gbxrs-native-probe``, built by ``gbx:build:native --target rust-native``) sits where the
env wheel puts Rust kernels, ``_native/<arch>/rs/``, in a payload root that is otherwise the env6 payload
(``build/env-wheel/env6/payload``). ``scripts/native/rust_native_smoke.py`` runs in one process with that root as
``GBX_NATIVE_ROOT``, once with the probe loaded after rasterio and osgeo and once before: one libgdal, a ``/vsimem``
file rasterio writes reads back through Rust, and GEOS and PROJ answer as shapely and pyproj do.

Linux only (the payload loads on Linux; the macOS host has neither payload nor probe): it skips there.
"""

import json
import os
import platform
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[4]
SRC = REPO / "python" / "geobrix" / "src"
PAYLOAD = REPO / "build" / "env-wheel" / "env6" / "payload"
SMOKE = REPO / "scripts" / "native" / "rust_native_smoke.py"
NAME = "_gbx_native_probe.abi3.so"
HASHED_NEED = re.compile(rb"lib[A-Za-z0-9_+]+-[0-9a-f]{8}\.so[.0-9]*")


def _payload_with_probe(tmp_path, a):
    """The env6 payload with the probe in _native/<a>/rs/: links to the payload's parts, the probe copied."""
    probe = (
        REPO
        / "build"
        / "native"
        / "out"
        / a
        / "rust-native"
        / "payload"
        / "_native"
        / a
        / "rs"
        / NAME
    )
    lib = PAYLOAD / "_native" / a / "lib"
    if not probe.is_file() or not lib.is_dir():
        pytest.skip(
            f"PRECONDITION MISSING: {probe} (gbx:build:native --target rust-native) or {lib} "
            "(gbx:build:wheel --env 6)"
        )
    # the payload libraries the probe needs, by their hashed names; a payload from another
    # stage-native build has other names: a stale artifact, not a defect
    needs = sorted({m.decode() for m in HASHED_NEED.findall(probe.read_bytes())})
    absent = [n for n in needs if not (lib / n).is_file()]
    if not needs or absent:
        pytest.skip(
            f"PRECONDITION MISSING: {lib} has no {absent or 'hashed library'} the probe needs "
            "(rebuild both from one stage-native: gbx:build:native --target rust-native, "
            "gbx:build:wheel --env 6)"
        )
    root = tmp_path / "payload"
    (root / "_native" / a / "rs").mkdir(parents=True)
    for part in ("_vendor", "_jar"):
        (root / part).symlink_to(PAYLOAD / part)
    for part in ("share", "licenses"):
        (root / "_native" / part).symlink_to(PAYLOAD / "_native" / part)
    for part in ("lib", "MANIFEST.json"):
        (root / "_native" / a / part).symlink_to(PAYLOAD / "_native" / a / part)
    (root / "_native" / a / "rs" / NAME).write_bytes(probe.read_bytes())
    return root


@pytest.mark.parametrize("order", ["bindings-first", "probe-first"])
def test_a_rust_kernel_runs_on_the_payload_gdal_beside_rasterio_and_osgeo(
    tmp_path, order
):
    if not sys.platform.startswith("linux"):
        pytest.skip(
            "the payload and the Rust probe load on Linux only (run in the container)"
        )
    root = _payload_with_probe(tmp_path, platform.machine())
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("GDAL_DATA", "PROJ_DATA", "PROJ_LIB")
    }
    env.pop("GBX_NATIVE_DISABLE", None)
    env.update(GBX_NATIVE_ROOT=str(root), PYTHONPATH=str(SRC))
    res = subprocess.run(
        [sys.executable, str(SMOKE), "--order", order],
        env=env,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0 and "R1_SMOKE OK" in res.stdout, (
        res.stdout[-3000:] + res.stderr[-2000:]
    )
    ev = json.loads(res.stdout.splitlines()[-2])
    lib = PAYLOAD / "_native" / platform.machine() / "lib"
    assert [Path(p).parent for p in ev["maps"]["libgdal"]] == [lib], ev["maps"]
