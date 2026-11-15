#!/usr/bin/env python3
"""Give every bundled shared library an auditwheel-style hashed name, so none shares a SONAME with a system library.

    rename-sonames.py --tree DIR [--map-out FILE]

DIR is a gdal-native tree (lib/, bin/, python/, lib/cmake, lib/pkgconfig, lib-dev/). The loader reuses an already-loaded
library with the same SONAME, so a bundled libsqlite3.so.0 or libcurl.so.4 is shadowed by (or shadows) the host's copy
of the same name, and libgdal then runs on whatever the process loaded first. A hashed name cannot match.

For each file in lib/ except libgdalalljni.so (the Java bindings load it by that name):
  libgdal.so.39 -> libgdal-<sha256[:8] of the file>.so.39      (<stem>-<hash>.so<rest>)
with `patchelf --set-soname`, then `patchelf --replace-needed` in every ELF file under lib/, bin/, python/ and
hdf5-plugins/ (the JNI, the osgeo extension modules and the HDF5 filter plugins included). The text files that name a
library (lib/cmake, lib/pkgconfig, bin/*-config) are rewritten to the new names, and lib-dev/ (symlinks for linking) is
rebuilt: for each library its original linker name (libgdal.so), its original SONAME (libgdal.so.39) and its new name
all point at ../lib/<new>, so `-lgdal` finds it and the program records the NEW SONAME in its NEEDED. --map-out writes
{"sonames": {old: new}, "jni": name} as JSON.

Fails (`RENAME FAILED: ...`, exit 1) if a library is not named after its SONAME, is already renamed, or any ELF still
needs an old name afterwards. Needs patchelf and readelf (the build container has both).
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys

JNI = "libgdalalljni.so"
HASHED = re.compile(r"-[0-9a-f]{8}\.so(\.|$)")


def fail(msg):
    print(f"RENAME FAILED: {msg}")
    sys.exit(1)


def is_elf(path):
    with open(path, "rb") as f:
        return f.read(4) == b"\x7fELF"


def patchelf(*args):
    r = subprocess.run(["patchelf", *args], capture_output=True, text=True)
    if r.returncode != 0:
        fail(f"patchelf {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout.strip()


def needed(path):
    return patchelf("--print-needed", path).split()


def elf_files(tree, subdirs):
    for sub in subdirs:
        for root, _dirs, files in os.walk(os.path.join(tree, sub)):
            for name in sorted(files):
                path = os.path.join(root, name)
                if not os.path.islink(path) and is_elf(path):
                    yield path


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--tree", required=True)
    ap.add_argument("--map-out")
    args = ap.parse_args()
    tree = os.path.abspath(args.tree)
    lib = os.path.join(tree, "lib")

    # 1. the new name of every library
    renames = {}
    for name in sorted(os.listdir(lib)):
        path = os.path.join(lib, name)
        if name == JNI or os.path.islink(path) or not os.path.isfile(path):
            continue
        if ".so" not in name or not is_elf(path):
            continue
        if HASHED.search(name):
            fail(f"{name} is already renamed")
        so = patchelf("--print-soname", path)
        if so != name:
            fail(
                f"{name} has SONAME {so!r}; a bundled library must be named after its SONAME"
            )
        with open(path, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()[:8]
        stem, rest = name.split(".so", 1)
        renames[name] = f"{stem}-{digest}.so{rest}"
    if len(set(renames.values())) != len(renames):
        fail("two libraries got the same new name")
    if not os.path.isfile(os.path.join(lib, JNI)):
        fail(f"no lib/{JNI}")

    # 2. SONAME and file name
    for old, new in renames.items():
        patchelf("--set-soname", new, os.path.join(lib, old))
        os.rename(os.path.join(lib, old), os.path.join(lib, new))

    # 3. NEEDED in every ELF file that links against them
    patched = 0
    for path in elf_files(tree, ("lib", "bin", "python", "hdf5-plugins")):
        pairs = [(n, renames[n]) for n in needed(path) if n in renames]
        if pairs:
            cmd = []
            for old, new in pairs:
                cmd += ["--replace-needed", old, new]
            patchelf(*cmd, path)
            patched += 1
        left = [n for n in needed(path) if n in renames]
        if left:
            fail(f"{os.path.relpath(path, tree)} still needs {left}")

    # 4. text files that name a library: cmake exports, .pc files, the *-config scripts
    ordered = sorted(renames, key=len, reverse=True)
    n_text = 0
    for sub in ("lib/cmake", "lib/pkgconfig", "bin"):
        for root, _dirs, files in os.walk(os.path.join(tree, sub)):
            for name in files:
                path = os.path.join(root, name)
                if os.path.islink(path) or is_elf(path):
                    continue
                with open(path, encoding="utf-8", errors="surrogateescape") as f:
                    text = f.read()
                new_text = text
                for old in ordered:
                    new_text = re.sub(
                        r"(?<![\w.-])" + re.escape(old) + r"(?![\w.])",
                        renames[old],
                        new_text,
                    )
                if new_text != text:
                    with open(
                        path, "w", encoding="utf-8", errors="surrogateescape"
                    ) as f:
                        f.write(new_text)
                    n_text += 1

    # 5. lib-dev: the linker name, the old SONAME and the new name, all pointing at the renamed file
    dev = os.path.join(tree, "lib-dev")
    os.makedirs(dev, exist_ok=True)
    for name in os.listdir(dev):
        os.unlink(os.path.join(dev, name))
    links = 0
    targets = dict(renames)
    targets[JNI] = JNI
    for old, new in sorted(targets.items()):
        for link in {old, new, old.split(".so")[0] + ".so"}:
            os.symlink("../lib/" + new, os.path.join(dev, link))
            links += 1

    if args.map_out:
        with open(args.map_out, "w") as f:
            json.dump({"sonames": renames, "jni": JNI}, f, indent=1, sort_keys=True)
            f.write("\n")
    print(
        f"rename-sonames: {len(renames)} libraries renamed (JNI kept), NEEDED rewritten in {patched} ELF files, "
        f"{n_text} text files, lib-dev: {links} links"
    )


if __name__ == "__main__":
    main()
