#!/usr/bin/env python3
"""The GDAL Java classes of the GeoBrix JAR, from the native build that makes the JNI.

The native build leaves GDAL's Java classes in build/native/out/<arch>/stage-native/gdal-native/java/gdal-<GDAL>.jar
for each architecture. The classes do not depend on the architecture, but the two jars differ in their zip
timestamps.

    java_jar.py stage [--check] [--root DIR]
        Check that both architectures' jars hold the same entries with the same bytes and that both builds are
        GDAL <GDAL_VERSION> (their MANIFEST.json), then write those entries once, as a deterministic jar (stored,
        META-INF first then sorted, one fixed timestamp), to scripts/native/maven/gdal-<GDAL>-gbx<REV>.jar. The jar
        also carries GDAL's licence (META-INF/LICENSE-GDAL.TXT, MIT) and the build it came from
        (META-INF/gbx-gdal.properties: gdal.version, gbx.native.revision; `pins.py audit` holds gdal.version to
        pins.conf). It prints the jar's sha256, the value of GDAL_JAVA_JAR_SHA256 in rust-pins.conf. --check writes
        nothing and fails when the committed jar is not what today's build gives.

    java_jar.py install [--repo DIR] [--root DIR]
        Put the committed jar, and a minimal pom, into a Maven local repository (default ~/.m2/repository) as
        org.gdal:gdal:<GDAL>-gbx<REV>, after checking its sha256 against GDAL_JAVA_JAR_SHA256; a mismatch is refused
        and nothing is written. No Maven plugin runs, so it can come before the PGP check of every other artifact.
        The dev container's Maven setup runs it; a host or IDE build runs it once per checkout, and again after
        every version bump or re-stage:
            python3 scripts/native/java_jar.py install

Run `stage` after a native rebuild of both architectures; a moved sha256 means the classes changed (re-pin it), and
a new GDAL_VERSION or GBX_NATIVE_REVISION is a new file name (rename pom.xml's gdal.java.version and the two
.maven-keys.list lines with it). Stdlib only.
"""

import argparse
import hashlib
import json
import sys
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import pins  # noqa: E402

ARCHES = ("aarch64", "x86_64")
EPOCH = (1980, 1, 1, 0, 0, 0)  # the earliest zip timestamp: no build time in the bytes
LICENSE_ENTRY = "META-INF/LICENSE-GDAL.TXT"
BUILD_ENTRY = "META-INF/gbx-gdal.properties"


def entries(jar: Path) -> dict:
    with zipfile.ZipFile(jar) as z:
        return {i.filename: z.read(i) for i in z.infolist()}


def _order(name: str):
    # a jar's manifest comes first (java.util.jar.JarInputStream reads it from the front)
    return (name != "META-INF/", name != "META-INF/MANIFEST.MF", name)


def jar_bytes(content: dict) -> bytes:
    """`content` (name -> bytes) as a deterministic jar."""
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as z:
        for name in sorted(content, key=_order):
            info = zipfile.ZipInfo(name, EPOCH)
            info.create_system = 3  # Unix, whatever the host
            is_dir = name.endswith("/")
            info.external_attr = ((0o40755 if is_dir else 0o100644) << 16) | (0x10 if is_dir else 0)
            z.writestr(info, content[name])
    return buf.getvalue()


def coordinates(p: dict):
    version = f"{p['GDAL_VERSION']}-gbx{p['GBX_NATIVE_REVISION']}"
    return version, f"scripts/native/maven/gdal-{version}.jar"


def stage(root: Path, check: bool = False) -> int:
    p = pins.load_all(root)
    version = p["GDAL_VERSION"]
    gdal_native = {a: root / f"build/native/out/{a}/stage-native/gdal-native" for a in ARCHES}
    jars = {a: d / f"java/gdal-{version}.jar" for a, d in gdal_native.items()}
    missing = [str(j.relative_to(root)) for j in jars.values() if not j.is_file()]
    if missing:
        print(f"BUILD FAILED: no GDAL Java classes at {', '.join(missing)} (gbx:build:native both architectures)")
        return 1
    built = {a: json.loads((d / "MANIFEST.json").read_text())["versions"]["gdal"] for a, d in gdal_native.items()}
    if set(built.values()) != {version}:
        print(f"BUILD FAILED: the native builds are GDAL {built}, pins.conf says GDAL_VERSION={version}")
        return 1
    content = {a: entries(j) for a, j in jars.items()}
    a, b = (content[x] for x in ARCHES)
    differ = sorted(set(a) ^ set(b) | {n for n in set(a) & set(b) if a[n] != b[n]})
    if differ:
        print(f"BUILD FAILED: the architectures' GDAL Java classes differ in {len(differ)} entries: {differ[:5]}")
        return 1
    licences = {(d / "licenses/gdal/LICENSE.TXT").read_bytes() for d in gdal_native.values()}
    if len(licences) != 1:
        print("BUILD FAILED: the architectures' GDAL licences differ")
        return 1
    content = dict(a)
    content[LICENSE_ENTRY] = licences.pop()
    content[BUILD_ENTRY] = f"gdal.version={version}\ngbx.native.revision={p['GBX_NATIVE_REVISION']}\n".encode()
    data = jar_bytes(content)
    sha = hashlib.sha256(data).hexdigest()
    out = root / coordinates(p)[1]
    if check:
        if not out.is_file() or out.read_bytes() != data:
            print(f"DRIFT {out.relative_to(root)} is not what today's native build gives (sha256 {sha}): run java_jar.py stage")
            return 1
        print(f"{out.relative_to(root)}: matches today's native build")
        return 0
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(data)
    print(f"{out.relative_to(root)}: {len(a)} class-jar entries, identical on {' and '.join(ARCHES)}, + licence and build")
    print(f"GDAL_JAVA_JAR_SHA256={sha}")
    return 0


POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.gdal</groupId>
  <artifactId>gdal</artifactId>
  <version>{version}</version>
  <packaging>jar</packaging>
  <description>GDAL's Java bindings from the GeoBrix native build (scripts/native/maven), installed by java_jar.py.</description>
  <licenses>
    <license>
      <name>MIT</name>
    </license>
  </licenses>
</project>
"""


def install(root: Path, repo: Path) -> int:
    p = pins.load_all(root)
    version, rel = coordinates(p)
    jar = root / rel
    want = p.get("GDAL_JAVA_JAR_SHA256")
    if not jar.is_file() or want is None:
        print(f"INSTALL REFUSED: {rel} or GDAL_JAVA_JAR_SHA256 is missing")
        return 1
    data = jar.read_bytes()
    got = hashlib.sha256(data).hexdigest()
    if got != want:
        print(f"INSTALL REFUSED: {rel} has sha256 {got}, rust-pins.conf pins GDAL_JAVA_JAR_SHA256={want}")
        return 1
    dest = repo / "org/gdal/gdal" / version
    dest.mkdir(parents=True, exist_ok=True)
    for stale in dest.glob("*.lastUpdated"):  # a failed remote lookup of these coordinates
        stale.unlink()
    (dest / f"gdal-{version}.jar").write_bytes(data)
    (dest / f"gdal-{version}.pom").write_text(POM.format(version=version))
    print(f"installed org.gdal:gdal:{version} (sha256 {got[:12]}...) into {dest}")
    return 0


def main(argv: list) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("stage")
    s.add_argument("--check", action="store_true")
    s.add_argument("--root", default=str(pins.DEFAULT_ROOT))
    i = sub.add_parser("install")
    i.add_argument("--repo", default=str(Path.home() / ".m2" / "repository"))
    i.add_argument("--root", default=str(pins.DEFAULT_ROOT))
    args = ap.parse_args(argv)
    if args.cmd == "stage":
        return stage(Path(args.root), args.check)
    return install(Path(args.root), Path(args.repo))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
