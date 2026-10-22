#!/bin/bash
# fetch-delta-test-jars.sh - make the OSS Delta Lake jars available for the Zarr manifest tests.
#
# The transactional Zarr manifest is a Delta table, so test/zarr runs against a local
# Spark session with delta-spark. PySpark needs the jars on the JVM classpath at launch;
# python/geobrix/test/conftest.py picks them up from a Maven repo (see _delta_jars()).
# This script puts them there:
#   * no-op when delta-spark and delta-storage are already in a repo that conftest checks
#     ($GBX_M2_REPO, <checkout>/scripts/docker/m2, ~/.m2/repository);
#   * otherwise `mvn dependency:get` into the Maven local repo (pulls delta-storage too).
#     Spark's --packages (Ivy) cannot be used: it does not follow the JFrog/dev-proxy
#     mirror that Maven is configured with.
#
# Used by gbx:test:python (inside geobrix-dev) and by .github/actions/pyrx_build.
# Keep DELTA_VERSION in step with _DELTA_VERSION in python/geobrix/test/conftest.py.
# Delta 4.0.x is the line that runs on Spark 4.0.x (pyspark 4.0.0 in the dev image).

set -euo pipefail

DELTA_VERSION="4.0.1"
SCALA="2.13"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPOS=("${GBX_M2_REPO:-}" "$REPO_ROOT/scripts/docker/m2" "$HOME/.m2/repository")

for repo in "${REPOS[@]}"; do
    [ -n "$repo" ] || continue
    if [ -f "$repo/io/delta/delta-spark_${SCALA}/${DELTA_VERSION}/delta-spark_${SCALA}-${DELTA_VERSION}.jar" ] \
        && [ -f "$repo/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" ]; then
        echo "delta-spark ${DELTA_VERSION}: present"
        exit 0
    fi
done

if ! command -v mvn >/dev/null 2>&1; then
    echo "delta-spark ${DELTA_VERSION}: missing and mvn is not on PATH. Fix: install Maven, or run this inside geobrix-dev." >&2
    exit 1
fi

echo "delta-spark ${DELTA_VERSION}: fetching with mvn dependency:get"
# Fully qualified plugin coordinates: the short `dependency:` prefix does not resolve in
# geobrix-dev. Maven echoes repository URLs, which have no place in a log.
set +e
mvn -B -q org.apache.maven.plugins:maven-dependency-plugin:3.7.0:get \
    -Dartifact="io.delta:delta-spark_${SCALA}:${DELTA_VERSION}" 2>&1 | grep -viE 'https?://'
status=${PIPESTATUS[0]}
set -e
if [ "$status" -ne 0 ]; then
    echo "delta-spark ${DELTA_VERSION}: mvn dependency:get failed (exit $status). Fix: check the Maven mirror settings (gbx:docker:start runs docker_maven_setup.sh), then re-run." >&2
    exit "$status"
fi
echo "delta-spark ${DELTA_VERSION}: fetched"
