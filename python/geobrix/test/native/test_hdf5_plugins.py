"""The payload's HDF5 filter plugins reach the payload's own netCDF-C and HDF5, and nothing else.

Against a built payload (``build/env-wheel/env6/payload`` as ``GBX_NATIVE_ROOT``), in a fresh
process per row, through either activator (the startup ``.pth`` module, or ``import
databricks.labs.gbx``), with and without a user's own ``HDF5_PLUGIN_PATH``:

- zstd, blosc and bzip2 netCDF-4 variables written by netCDF4 read back the same through netCDF4
  and through GDAL (rasterio);
- the process's environment, and a child process's, carry no ``HDF5_PLUGIN_PATH`` of ours (a
  user's value is passed on unchanged, and its directories stay first in the path);
- a second, foreign libhdf5 loaded into the same process (the dev image's own HDF5, standing in
  for a user's h5py) keeps its own plugin path: the payload's directory is registered only with the
  payload's libhdf5.

Linux only; skips with ``PRECONDITION MISSING`` where there is no payload.
"""

import ctypes.util
import glob
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[4]
SRC = REPO / "python" / "geobrix" / "src"
PAYLOAD = REPO / "build" / "env-wheel" / "env6" / "payload"

_PROBE = r"""
import ctypes, json, os, subprocess, sys
if sys.argv[1] == "pth":
    sys.path.insert(0, os.environ["GBX_SRC"])
    import _gbx_native_path  # the .pth module activates by itself, before any binding import
    assert _gbx_native_path.ACTIVE
else:
    import databricks.labs.gbx  # noqa: F401 - the import-time activation, first
import numpy as np
import netCDF4, rasterio
assert "/_vendor/" in netCDF4.__file__ and "/_vendor/" in rasterio.__file__, (netCDF4.__file__, rasterio.__file__)
from databricks.labs.gbx import _native  # (after the .pth activation this one adds nothing)
from databricks.labs.gbx._native import _rules

a = (np.arange(64 * 48, dtype=np.float32).reshape(64, 48) % 251) * 0.5
codecs = {}
for codec in ("zstd", "blosc_lz4", "bzip2"):
    path = os.path.join(os.environ["TMP_DIR"], codec + ".nc")
    with netCDF4.Dataset(path, "w", format="NETCDF4") as nc:
        nc.createDimension("y", 64); nc.createDimension("x", 48)
        nc.createVariable("v", "f4", ("y", "x"), compression=codec, complevel=4)[:] = a
    with netCDF4.Dataset(path) as nc:
        back = np.asarray(nc["v"][:])
    with rasterio.Env(GDAL_NETCDF_BOTTOMUP="NO"), rasterio.open('NETCDF:"%s":v' % path) as ds:
        via_gdal = ds.read(1)
    codecs[codec] = bool(np.array_equal(back, a) and np.array_equal(via_gdal, a))

def h5pl(lib):
    h = ctypes.CDLL(lib)
    h.H5open()
    count = ctypes.c_uint()
    h.H5PLsize(ctypes.byref(count))
    out = []
    for i in range(count.value):
        buf = ctypes.create_string_buffer(4096)
        h.H5PLget(i, buf, ctypes.c_size_t(4096))
        out.append(buf.value.decode())
    return out

arch = os.uname().machine
root = os.environ["GBX_NATIVE_ROOT"]
foreign = os.environ.get("FOREIGN_LIBHDF5")
print(json.dumps({
    "codecs": codecs,
    "registered": _native.hdf5_plugin_path(),
    "plugin_dir": _native.hdf5_plugin_dir(),
    "payload_hdf5": h5pl(_rules.payload_lib(root, arch, "libhdf5.so.")),
    "foreign_hdf5": h5pl(foreign) if foreign else None,
    "env": os.environ.get("HDF5_PLUGIN_PATH"),
    "child_env": subprocess.run([sys.executable, "-c", "import os; print(os.environ.get('HDF5_PLUGIN_PATH'))"],
                                capture_output=True, text=True).stdout.strip(),
}))
"""


def _foreign_libhdf5():
    """Another libhdf5 than the payload's: the dev image's own (Ubuntu's), else a PyPI netCDF4 wheel's."""
    found = ctypes.util.find_library("hdf5_serial")
    if found:
        return found
    hits = sorted(
        glob.glob("/usr/local/lib/python3*/dist-packages/netcdf4.libs/libhdf5-*.so*")
    )
    return next((h for h in hits if "hdf5_hl" not in h), None)


@pytest.mark.parametrize(
    "user_path", [None, "/tmp/gbx-user-hdf5-plugins"], ids=["no-user-path", "user-path"]
)
@pytest.mark.parametrize("activator", ["pth", "import"])
def test_the_filter_plugins_reach_only_the_payloads_hdf5(
    tmp_path, activator, user_path
):
    if not sys.platform.startswith("linux"):
        pytest.skip(
            "the env wheels' payload loads on Linux only (run in the container)"
        )
    a = platform.machine()
    if not (PAYLOAD / "_native" / a / "hdf5-plugins").is_dir():
        pytest.skip(
            f"PRECONDITION MISSING: {PAYLOAD}/_native/{a}/hdf5-plugins (gbx:build:wheel --env 6)"
        )
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("HDF5_PLUGIN_PATH", "GBX_NATIVE_DISABLE", "PYTHONPATH")
    }
    env.update(GBX_NATIVE_ROOT=str(PAYLOAD), GBX_SRC=str(SRC), TMP_DIR=str(tmp_path))
    if activator == "import":
        env["PYTHONPATH"] = str(SRC)
    if user_path:
        env["HDF5_PLUGIN_PATH"] = user_path
    foreign = _foreign_libhdf5()
    if foreign:
        env["FOREIGN_LIBHDF5"] = foreign
    res = subprocess.run(
        [sys.executable, "-c", _PROBE, activator],
        env=env,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr[-2000:]
    got = json.loads(res.stdout.splitlines()[-1])
    ours = str(PAYLOAD / "_native" / a / "hdf5-plugins")

    assert got["codecs"] == {"zstd": True, "blosc_lz4": True, "bzip2": True}
    assert got["plugin_dir"] == ours and got["registered"][-1] == ours
    assert ours in got["payload_hdf5"]  # netCDF-C synced it to the payload's HDF5
    assert got["env"] == user_path and got["child_env"] == str(
        user_path
    )  # nothing of ours in any environment
    if (
        user_path
    ):  # the user's directories come first; ours fills in the filters theirs lack
        assert got["registered"][0] == user_path
    if foreign:  # another HDF5 in the same process never sees the payload's plugins
        assert ours not in got["foreign_hdf5"], got["foreign_hdf5"]
