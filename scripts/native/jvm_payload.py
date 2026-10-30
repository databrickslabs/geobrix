#!/usr/bin/env python3
"""Lay out a JVM payload from one architecture's gdal-native tree (gbx:build:native --target stage-native).

    jvm_payload.py --native build/native/out/x86_64/stage-native/gdal-native --arch x86_64 --out build/native/jvm-payload

<out> gets the env wheel's native layout, the GBX_NATIVE_ROOT layout GDALManager reads, as gbx:build:wheel lays it
out (build_env_wheel.py add_payload, for one architecture):

    _native/<arch>/lib/           gdal-native/lib/ without pkgconfig/ and cmake/: the JNI (libgdalalljni.so) and the
                                  libraries it needs, RUNPATH $ORIGIN; exactly the libraries MANIFEST.json names,
                                  and every file laid out has the sha256 MANIFEST.json records
    _native/<arch>/MANIFEST.json  the native build record
    _native/<arch>/hdf5-plugins/, _native/<arch>/bin/projsync
    _native/share/{gdal,proj}     GDAL's and PROJ's data: a JNI under <dir>/lib takes <dir>/../share (GDALManager)

No Python bindings (_vendor): this is the payload a JVM loads. CI links /usr/lib/libgdalalljni.so to
<out>/_native/<arch>/lib/libgdalalljni.so (scripts/docker/extras/docker_native_jni.sh), as the dev container links it
into the env6 payload. The last line is `JVM_PAYLOAD root=<out> arch=<arch> gdal=<version> jni=<path>`. Stdlib only.
"""

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

DEV_ONLY = ("pkgconfig", "cmake")
EXTRAS = ("hdf5-plugins", "bin/projsync")


def fail(msg):
    print(f"JVM PAYLOAD FAILED: {msg}")
    sys.exit(1)


def copy_tree(src, dst, skip=()):
    for p in sorted(src.rglob("*")):
        rel = p.relative_to(src)
        if rel.parts[0] in skip:
            continue
        if p.is_symlink():
            fail(f"{p} is a symlink (gdal-native has one file per SONAME)")
        if p.is_file():
            (dst / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, dst / rel)


def assemble(native, arch, out):
    manifest = json.loads((native / "MANIFEST.json").read_text())
    if manifest.get("arch") != arch:
        fail(f"{native} is the {manifest.get('arch')} build, not {arch}")
    if out.exists():
        shutil.rmtree(out)
    d = out / "_native" / arch
    copy_tree(native / "lib", d / "lib", skip=DEV_ONLY)
    want = set(manifest["sonames"].values()) | {manifest["jni"]}
    have = {p.name for p in (d / "lib").iterdir()}
    if want != have:
        fail(f"lib/ holds {sorted(have ^ want)} beyond or short of MANIFEST.json's libraries")
    shutil.copy2(native / "MANIFEST.json", d / "MANIFEST.json")
    for extra in EXTRAS:
        src = native / extra
        if not src.exists():
            fail(f"{src} is missing")
        if src.is_dir():
            copy_tree(src, d / extra)
        else:
            (d / extra).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, d / extra)
    copy_tree(native / "share", out / "_native" / "share")
    for rel in ("share/proj/proj.db", "share/gdal"):
        if not (out / "_native" / rel).exists():
            fail(f"gdal-native has no {rel}")
    # content, not just names: every file laid out has the sha256 MANIFEST.json records (a partly restored cache
    # entry fails here, not when a JVM loads it)
    laid = {}
    for p in sorted((d / "lib").iterdir()):
        laid[f"lib/{p.name}"] = p
    for extra in EXTRAS:
        for p in sorted((d / extra).rglob("*")) if (d / extra).is_dir() else [d / extra]:
            if p.is_file():
                laid[f"{extra}/{p.relative_to(d / extra)}" if (d / extra).is_dir() else extra] = p
    for p in sorted((out / "_native" / "share").rglob("*")):
        if p.is_file():
            laid[f"share/{p.relative_to(out / '_native' / 'share')}"] = p
    files = manifest.get("files") or {}
    bad = sorted(r for r, p in laid.items() if files.get(r) != hashlib.sha256(p.read_bytes()).hexdigest())
    if bad:
        fail(f"{len(bad)} of {len(laid)} files are not what MANIFEST.json records (sha256), e.g. {bad[:3]}")
    jni = d / "lib" / manifest["jni"]
    print(f"JVM_PAYLOAD root={out} arch={arch} gdal={manifest['versions']['gdal']} jni={jni}")


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--native", type=Path, required=True, help="a gdal-native tree")
    ap.add_argument("--arch", choices=("x86_64", "aarch64"), required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)
    assemble(args.native.resolve(), args.arch, args.out.resolve())
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
