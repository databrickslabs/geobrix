#!/usr/bin/env python3
"""Fail when a GeoBrix fat JAR bundles the Scala standard library.

Databricks Runtime provides scala-library. A bundled copy in /databricks/jars can shadow the
runtime's newer one (on DCS, DBR 19's driver failed with NoSuchMethodError
MurmurHash3$.caseClassHash), so pom.xml keeps scala-library at provided scope.

Usage: python3 scripts/ci/check_jar_packaging.py target/geobrix-<ver>-jar-with-dependencies.jar
"""

import sys
import zipfile

FORBIDDEN_PREFIXES = ("scala/",)
FORBIDDEN_NAMES = ("library.properties",)


def bundled_scala_entries(jar_path: str) -> list[str]:
    with zipfile.ZipFile(jar_path) as zf:
        return [
            n
            for n in zf.namelist()
            if n.startswith(FORBIDDEN_PREFIXES) or n in FORBIDDEN_NAMES
        ]


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(
            "usage: check_jar_packaging.py <geobrix-*-jar-with-dependencies.jar>",
            file=sys.stderr,
        )
        return 2
    jar = argv[1]
    bad = bundled_scala_entries(jar)
    if bad:
        print(
            f"{jar}: bundles the Scala library ({len(bad)} entries, e.g. {bad[0]}); "
            "scala-library must stay at provided scope in pom.xml",
            file=sys.stderr,
        )
        return 1
    print(f"{jar}: OK (no bundled Scala library)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
