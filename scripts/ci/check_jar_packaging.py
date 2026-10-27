#!/usr/bin/env python3
"""Fail when a GeoBrix fat JAR bundles classes that Databricks Runtime provides.

Three groups are refused, each reported with a count and its first entry:

- Scala library (`scala/`, `library.properties`): a bundled copy in /databricks/jars can shadow the
  runtime's newer one (on DCS, DBR 19's driver failed with NoSuchMethodError
  MurmurHash3$.caseClassHash), so pom.xml keeps scala-library at provided scope.
- log4j (`org/apache/logging/log4j/`): the runtime already provides log4j-api and log4j-core;
  bundling them can cause version conflicts.
- Spark (`org/apache/spark/`): Spark itself is never packaged (its artifacts stay `provided`).
  GeoBrix's own classes under `org/apache/spark/sql/adapters/` are allowed; they live in that
  package on purpose to reach Spark-internal APIs.

Zip directory entries are ignored; only files count. Exit codes: 0 clean, 1 a group is
bundled, 2 usage error, 3 the JAR is missing or not a readable zip file (it was not checked).

Usage: python3 scripts/ci/check_jar_packaging.py target/geobrix-<ver>-jar-with-dependencies.jar
"""

import sys
import zipfile

# (label, entry prefixes, exact entry names, allowed prefixes, remedy)
GROUPS = (
    (
        "the Scala library",
        ("scala/",),
        ("library.properties",),
        (),
        "scala-library must stay at provided scope in pom.xml "
        "(Databricks Runtime provides it)",
    ),
    (
        "log4j",
        ("org/apache/logging/log4j/",),
        (),
        (),
        "log4j-api and log4j-core must stay unbundled "
        "(Databricks Runtime provides them; bundling can cause version conflicts)",
    ),
    (
        "Spark classes",
        ("org/apache/spark/",),
        (),
        ("org/apache/spark/sql/adapters/",),
        "org.apache.spark artifacts must stay at provided scope "
        "(Databricks Runtime provides Spark)",
    ),
)


def bundled_entries(jar_path: str) -> dict[str, list[str]]:
    """Map each offending group's label to its bundled entries (clean groups are omitted)."""
    with zipfile.ZipFile(jar_path) as zf:
        # Directory entries hold no classes; the fat JAR carries them for the ancestors of
        # GeoBrix's own org/apache/spark/sql/adapters/ package.
        names = [i.filename for i in zf.infolist() if not i.is_dir()]
    found = {}
    for label, prefixes, exact, allowed, _ in GROUPS:
        hits = [
            n
            for n in names
            if (n.startswith(prefixes) or n in exact) and not n.startswith(allowed)
        ]
        if hits:
            found[label] = hits
    return found


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(
            "usage: check_jar_packaging.py <geobrix-*-jar-with-dependencies.jar>",
            file=sys.stderr,
        )
        return 2
    jar = argv[1]
    try:
        found = bundled_entries(jar)
    except (OSError, zipfile.BadZipFile) as e:
        print(
            f"{jar}: cannot read the JAR ({type(e).__name__}: {e}); rebuild it "
            "(mvn clean package)",
            file=sys.stderr,
        )
        return 3
    for label, _, _, _, remedy in GROUPS:
        if label in found:
            hits = found[label]
            print(
                f"{jar}: bundles {label} ({len(hits)} entries, e.g. {hits[0]}); "
                f"{remedy}",
                file=sys.stderr,
            )
    if found:
        return 1
    print(f"{jar}: OK (no bundled Scala, Spark or log4j)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
