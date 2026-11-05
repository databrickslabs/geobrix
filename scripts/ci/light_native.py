#!/usr/bin/env python3
"""The native payload the light CI shards test on: GDAL's Python bindings, the five vendored bindings and the Rust module.

    light_native.py assemble --native-out build/native/out/x86_64 --jvm-payload build/native/jvm-payload \\
                             --rs <_gbxrs.so> --out build/native/light-payload
    light_native.py install  --payload <dir>      # in each light shard, after the lock and the package are installed
    light_native.py check    --payload <dir>      # fails unless every binding and the Rust module load from the payload

Why: the light tier is tested on the bindings it ships with. Without them a light run uses PyPI's rasterio and has no
osgeo and no Rust module, so every test that needs GDAL's own bindings or the Rust kernels skips ("no osgeo", "no Rust
build"). `assemble` lays out the GBX_NATIVE_ROOT layout of the env wheel (databricks/labs/gbx/_native/_rules.py) from the
x86_64 native build CI already makes for the JVM jobs (the stage-native and bindings outputs of build-gdal-native.yml):

    _native/x86_64/{lib,hdf5-plugins,bin,MANIFEST.json,rs/_gbxrs.abi3.so}, _native/share   scripts/native/jvm_payload.py + the Rust module
    _vendor/x86_64/{rasterio,pyogrio,pyproj,shapely,netCDF4}                               bindings/vendor (RUNPATH finds _native/x86_64/lib)
    _vendor/x86_64/osgeo                                                                   gdal-native/python/osgeo (see below)
    _vendor/common/osgeo_utils                                                             gdal-native/python/osgeo_utils

gdal-native's osgeo has RUNPATH $ORIGIN/../../lib (beside gdal-native/lib). Under _vendor/x86_64/osgeo that is
_vendor/lib, which the symlink _vendor/lib -> ../_native/x86_64/lib answers; the env wheel instead rewrites the RUNPATH
with patchelf in its toolchain image, which CI has no need to run. `check` proves the result loads.

`install` does what the env wheel's install does: _gbx_native_path.py and geobrix-native.pth go into site-packages and
GBX_NATIVE_ROOT is exported, so the payload is on sys.path at interpreter start, before pytest imports rasterio.
Stdlib only (the check imports the bindings).
"""

import argparse
import os
import shutil
import sys
import sysconfig
from pathlib import Path

ARCH = "x86_64"
REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "python" / "geobrix" / "src"
BINDINGS = ("rasterio", "pyogrio", "pyproj", "shapely", "netCDF4")


def fail(msg):
    print(f"LIGHT PAYLOAD FAILED: {msg}")
    sys.exit(1)


def _need(path, what):
    if not Path(path).exists():
        fail(f"{what} is missing: {path}")
    return Path(path)


def assemble(native_out, jvm_payload, rs, out):
    native_out, jvm_payload, rs, out = (Path(p) for p in (native_out, jvm_payload, rs, out))
    _need(jvm_payload / "_native" / ARCH / "lib", "the JVM payload (scripts/native/jvm_payload.py)")
    vendor = _need(native_out / "bindings" / "vendor", "the bindings output")
    py = _need(native_out / "stage-native" / "gdal-native" / "python", "gdal-native's Python bindings")
    _need(rs, "the Rust module")
    if out.exists():
        shutil.rmtree(out)
    shutil.copytree(jvm_payload / "_native", out / "_native", symlinks=True)
    for name in BINDINGS:
        _need(vendor / name, f"the vendored {name}")
        shutil.copytree(vendor / name, out / "_vendor" / ARCH / name, symlinks=True)
    shutil.copytree(_need(py / "osgeo", "osgeo"), out / "_vendor" / ARCH / "osgeo", symlinks=True)
    shutil.copytree(_need(py / "osgeo_utils", "osgeo_utils"), out / "_vendor" / "common" / "osgeo_utils", symlinks=True)
    (out / "_vendor" / "lib").symlink_to(Path("..") / "_native" / ARCH / "lib")
    dest = out / "_native" / ARCH / "rs"
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copy2(rs, dest / "_gbxrs.abi3.so")
    print(f"LIGHT_PAYLOAD root={out} arch={ARCH} bindings={','.join(BINDINGS)},osgeo rs=_gbxrs.abi3.so")


def install(payload):
    payload = Path(payload).resolve()
    _need(payload / "_vendor" / ARCH, "the payload's _vendor")
    site = Path(sysconfig.get_paths()["purelib"])
    shutil.copy2(SRC / "_gbx_native_path.py", site / "_gbx_native_path.py")
    (site / "geobrix-native.pth").write_text("import _gbx_native_path\n")
    env_file = os.environ.get("GITHUB_ENV")
    if env_file:
        with open(env_file, "a") as fh:
            fh.write(f"GBX_NATIVE_ROOT={payload}\n")
    print(f"LIGHT_PAYLOAD_INSTALLED site={site} GBX_NATIVE_ROOT={payload}")


def check(payload):
    """Fail (exit 1) unless the interpreter started on the payload and every binding and the Rust module load from it."""
    payload = Path(payload).resolve()
    problems = []
    if os.environ.get("GBX_NATIVE_ROOT") != str(payload):
        problems.append(f"GBX_NATIVE_ROOT is {os.environ.get('GBX_NATIVE_ROOT')!r}, not {payload}")
    import _gbx_native_path  # the .pth's module: it must have activated at interpreter start

    if not _gbx_native_path.ACTIVE:
        problems.append("_gbx_native_path is not active (the .pth did not activate the payload at interpreter start)")
    sys.path.insert(0, str(SRC))
    import importlib

    for name in (*BINDINGS, "osgeo"):
        try:
            mod = importlib.import_module(name)
        except Exception as exc:  # noqa: BLE001 - every cause is a finding
            problems.append(f"import {name} failed: {type(exc).__name__}: {exc}")
            continue
        if payload not in Path(mod.__file__).resolve().parents:
            problems.append(f"{name} came from {mod.__file__}, not the payload")
    try:
        from databricks.labs.gbx import _osgeo, _rs

        why = _rs.unavailable_reason()
        if why:
            problems.append(f"Rust module: {why}")
        _osgeo.bindings("light_native check")  # refuses an osgeo that is not rasterio's GDAL
    except Exception as exc:  # noqa: BLE001
        problems.append(f"{type(exc).__name__}: {exc}")
    if problems:
        fail("the light shard would not test on the native bindings:\n  - " + "\n  - ".join(problems))
    import rasterio
    from osgeo import gdal

    print(f"LIGHT_PAYLOAD_OK rasterio={rasterio.__version__} gdal={gdal.VersionInfo()} rust=loaded root={payload}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("assemble")
    for opt in ("native-out", "jvm-payload", "rs", "out"):
        a.add_argument(f"--{opt}", required=True)
    for cmd in ("install", "check"):
        sub.add_parser(cmd).add_argument("--payload", required=True)
    args = ap.parse_args(argv)
    if args.cmd == "assemble":
        assemble(args.native_out, args.jvm_payload, args.rs, args.out)
    elif args.cmd == "install":
        install(args.payload)
    else:
        check(args.payload)


if __name__ == "__main__":
    main()
