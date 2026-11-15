#!/usr/bin/env python3
"""Give every ELF file in a tree a RUNPATH that finds the shared libraries relative to the file itself.

    set-rpath.py --tree DIR --tree-at LAYOUT --lib-at LAYOUT

DIR is a directory on disk. LAYOUT paths are where things will sit in the final layout (a wheel, a Volume, a
staging tree), relative to one common root: --tree-at is where DIR ends up, --lib-at is the directory that holds
the shared libraries. For the file DIR/bin/gdalinfo with --tree-at gdal-native --lib-at gdal-native/lib the RUNPATH
becomes $ORIGIN/../lib; for DIR/lib/libgdal.so.39 it becomes $ORIGIN; for DIR/python/osgeo/_gdal.so it becomes
$ORIGIN/../../lib.

Replaces any existing RPATH or RUNPATH (a build prefix in them is how a library would find the wrong copy), and
checks afterwards that each file has a RUNPATH and no RPATH. Symlinks and non-ELF files are skipped. Needs patchelf
and readelf (the build container has both). Exit 1 and `RPATH FAILED: ...` on any problem.
"""

import argparse
import os
import posixpath
import subprocess
import sys


def is_elf(path):
    with open(path, "rb") as f:
        return f.read(4) == b"\x7fELF"


def dynamic_tags(path):
    """The (RPATH, RUNPATH) strings of an ELF file, None where absent."""
    out = subprocess.run(
        ["readelf", "-d", path], capture_output=True, text=True, check=False
    ).stdout
    rpath = runpath = None
    for line in out.splitlines():
        if "(RPATH)" in line:
            rpath = line.split("[", 1)[1].rsplit("]", 1)[0]
        elif "(RUNPATH)" in line:
            runpath = line.split("[", 1)[1].rsplit("]", 1)[0]
    return rpath, runpath


def runpath_for(layout_file, lib_at):
    rel = posixpath.relpath(lib_at, posixpath.dirname(layout_file))
    return "$ORIGIN" if rel == "." else "$ORIGIN/" + rel


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--tree", required=True, help="directory on disk")
    ap.add_argument(
        "--tree-at", required=True, help="where --tree sits in the final layout"
    )
    ap.add_argument(
        "--lib-at",
        required=True,
        help="the directory holding the shared libraries, in the final layout",
    )
    args = ap.parse_args()

    tree = os.path.abspath(args.tree)
    tree_at = posixpath.normpath(args.tree_at)
    lib_at = posixpath.normpath(args.lib_at)
    changed = 0
    for root, _dirs, files in os.walk(tree):
        for name in sorted(files):
            path = os.path.join(root, name)
            if os.path.islink(path) or not is_elf(path):
                continue
            rel = os.path.relpath(path, tree).replace(os.sep, "/")
            want = runpath_for(posixpath.join(tree_at, rel), lib_at)
            for cmd in (["--remove-rpath"], ["--set-rpath", want]):
                r = subprocess.run(
                    ["patchelf", *cmd, path], capture_output=True, text=True
                )
                if r.returncode != 0:
                    print(f"RPATH FAILED: patchelf {cmd[0]} {rel}: {r.stderr.strip()}")
                    return 1
            rpath, runpath = dynamic_tags(path)
            if rpath is not None or runpath != want:
                print(
                    f"RPATH FAILED: {rel} has RPATH={rpath!r} RUNPATH={runpath!r}, wanted RUNPATH={want!r}"
                )
                return 1
            changed += 1
    print(
        f"set-rpath: {changed} ELF files under {args.tree} -> RUNPATH relative to {lib_at}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
