#!/usr/bin/env python3
"""A byte-reproducible .tar.xz: repro_tar.py <dir> <member> <out.tar.xz> <epoch>

Packs <dir>/<member> (a file or a tree) into <out.tar.xz> with the members sorted by name, every mtime set to <epoch>
(SOURCE_DATE_EPOCH), owner and group 0 with no names, modes normalized (directories and executables 0755, other files
0644) and no extended attributes, so the same content always gives the same bytes on any host (macOS's bsdtar has no
--sort or --mtime). Used for the debuginfo tarballs of gbx:build:native. Standard library only.
"""

import os
import sys
import tarfile


def _members(base, member):
    top = os.path.join(base, member)
    if not os.path.isdir(top):
        return [member]
    out = [member]
    for root, dirs, files in os.walk(top):
        dirs.sort()
        rel = os.path.relpath(root, base)
        out += [os.path.join(rel, d) for d in dirs] + [os.path.join(rel, f) for f in sorted(files)]
    return sorted(set(out))


def pack(base, member, out, epoch):
    def normalize(info):
        info.mtime = epoch
        info.uid = info.gid = 0
        info.uname = info.gname = ""
        if info.isdir() or info.mode & 0o111:
            info.mode = 0o755
        else:
            info.mode = 0o644
        info.pax_headers = {}
        return info

    tmp = out + ".tmp"
    with tarfile.open(tmp, "w:xz", format=tarfile.GNU_FORMAT) as tf:
        for name in _members(base, member):
            tf.add(os.path.join(base, name), arcname=name, recursive=False, filter=normalize)
    os.replace(tmp, out)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        sys.exit(__doc__.split("\n", 1)[0])
    pack(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))
