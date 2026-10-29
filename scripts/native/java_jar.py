#!/usr/bin/env python3
"""The GDAL Java classes of the GeoBrix JAR, from the native build that makes the JNI.

The native build leaves GDAL's Java classes in build/native/out/<arch>/stage-native/gdal-native/java/gdal-<GDAL>.jar
for each architecture. The classes do not depend on the architecture, but the two jars differ in their zip
timestamps. `stage` checks that both hold the same entries with the same bytes, and writes those entries once, as a
deterministic jar (stored, META-INF first then sorted, one fixed timestamp), to
scripts/native/maven/gdal-<GDAL>-gbx<REV>.jar: the file pom.xml's install-gdal-java execution installs as
org.gdal:gdal:<GDAL>-gbx<REV> (see its comment for the first install). It prints the jar's sha256, the value of
GDAL_JAVA_JAR_SHA256 in rust-pins.conf (`pins.py audit` checks the committed jar against it).

    java_jar.py stage [--root DIR]

Run it after a native rebuild of both architectures; a moved sha256 means the classes changed (re-pin it).
Stdlib only.
"""

import argparse
import hashlib
import sys
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import pins  # noqa: E402

ARCHES = ("aarch64", "x86_64")
EPOCH = (1980, 1, 1, 0, 0, 0)  # the earliest zip timestamp: no build time in the bytes


def entries(jar: Path) -> dict:
    with zipfile.ZipFile(jar) as z:
        return {i.filename: z.read(i) for i in z.infolist()}


def _order(name: str):
    # a jar's manifest comes first (java.util.jar.JarInputStream reads it from the front)
    return (name != "META-INF/", name != "META-INF/MANIFEST.MF", name)


def write(path: Path, content: dict) -> str:
    """Write `content` (name -> bytes) as a deterministic jar; return its sha256."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as z:
        for name in sorted(content, key=_order):
            info = zipfile.ZipInfo(name, EPOCH)
            info.create_system = 3  # Unix, whatever the host
            is_dir = name.endswith("/")
            info.external_attr = ((0o40755 if is_dir else 0o100644) << 16) | (0x10 if is_dir else 0)
            z.writestr(info, content[name])
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stage(root: Path) -> int:
    p = pins.load_all(root)
    version = p["GDAL_VERSION"]
    jars = {
        a: root / f"build/native/out/{a}/stage-native/gdal-native/java/gdal-{version}.jar"
        for a in ARCHES
    }
    missing = [str(j.relative_to(root)) for j in jars.values() if not j.is_file()]
    if missing:
        print(f"BUILD FAILED: no GDAL Java classes at {', '.join(missing)} (gbx:build:native both architectures)")
        return 1
    content = {a: entries(j) for a, j in jars.items()}
    a, b = (content[x] for x in ARCHES)
    differ = sorted(set(a) ^ set(b) | {n for n in set(a) & set(b) if a[n] != b[n]})
    if differ:
        print(f"BUILD FAILED: the architectures' GDAL Java classes differ in {len(differ)} entries: {differ[:5]}")
        return 1
    out = root / f"scripts/native/maven/gdal-{version}-gbx{p['GBX_NATIVE_REVISION']}.jar"
    sha = write(out, a)
    print(f"{out.relative_to(root)}: {len(a)} entries, identical on {' and '.join(ARCHES)}")
    print(f"GDAL_JAVA_JAR_SHA256={sha}")
    return 0


def main(argv: list) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("stage")
    s.add_argument("--root", default=str(pins.DEFAULT_ROOT))
    args = ap.parse_args(argv)
    return stage(Path(args.root))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
