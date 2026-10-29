"""scripts/native/repro_tar.py: the debuginfo tarballs of gbx:build:native are byte-reproducible."""

import hashlib
import os
import sys
import tarfile
import time
from pathlib import Path

NATIVE = Path(__file__).resolve().parents[1] / "native"
sys.path.insert(0, str(NATIVE))
import repro_tar  # noqa: E402

EPOCH = 1767225600


def _tree(root, mtime):
    for rel, data, mode in (
        (".build-id/ab/cdef.debug", b"one", 0o640),
        (".build-id/01/2345.debug", b"two", 0o600),
        (".build-id/ab/0000.debug", b"three", 0o755),
    ):
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        p.chmod(mode)
        os.utime(p, (mtime, mtime))
    for d in (root / ".build-id", root / ".build-id/ab", root / ".build-id/01"):
        os.utime(d, (mtime, mtime))


def test_the_same_content_packs_to_the_same_bytes(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    _tree(a, time.time() - 86400)
    _tree(b, time.time())  # other mtimes, as a rebuild gives
    for d in (a, b):
        repro_tar.pack(str(d), ".build-id", str(d) + ".tar.xz", EPOCH)
    digest = [
        hashlib.sha256(Path(str(d) + ".tar.xz").read_bytes()).hexdigest()
        for d in (a, b)
    ]
    assert digest[0] == digest[1]
    with tarfile.open(str(a) + ".tar.xz") as tf:
        members = tf.getmembers()
    names = [m.name for m in members]
    assert names == sorted(names) and ".build-id/ab/cdef.debug" in names
    assert {m.mtime for m in members} == {EPOCH} and {
        (m.uid, m.gid, m.uname, m.gname) for m in members
    } == {(0, 0, "", "")}
    assert {m.name: m.mode for m in members}[".build-id/ab/0000.debug"] == 0o755
    assert {m.name: m.mode for m in members}[".build-id/01/2345.debug"] == 0o644


def test_gbx_build_native_packs_debuginfo_with_it():
    text = (NATIVE.parent / "commands" / "gbx-build-native.sh").read_text()
    assert (
        'scripts/native/repro_tar.py" "$out/debuginfo" .build-id "$tarball" "$epoch"'
        in text
    )
