"""scripts/native/repro_tar.py: the debuginfo tarballs of gbx:build:native are byte-reproducible."""

import hashlib
import os
import re
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


def test_the_epoch_gbx_build_native_extracts_is_one_exported_number():
    # pack_debuginfo reads it with `sed -n 's/^export SOURCE_DATE_EPOCH=//p'` and fails the build otherwise;
    # a renamed or doubled export would show up only at build time.
    lines = [
        ln
        for ln in (NATIVE / "build-lib.sh").read_text().splitlines()
        if ln.startswith("export SOURCE_DATE_EPOCH=")
    ]
    assert len(lines) == 1 and re.fullmatch(
        r"export SOURCE_DATE_EPOCH=\d+", lines[0]
    ), lines


def test_gbx_build_native_exports_the_image_it_just_built_not_the_cache():
    # after --no-cache the build cache also holds the previous build's layers, and an export that read them
    # wrote out a stale tree: the export stage must read the tagged image through a named build context
    text = (NATIVE.parent / "commands" / "gbx-build-native.sh").read_text()
    lines = text.splitlines()
    at = [i for i, ln in enumerate(lines) if "docker buildx build" in ln and "--build-context" in ln and "$TARGET=" in ln]
    assert len(at) == 1, at
    command = lines[at[0]] + lines[at[0] + 1]  # the command and its continuation line
    assert '--build-context "$TARGET=docker-image://$tag"' in command
    assert '--target "export-$TARGET"' in command
