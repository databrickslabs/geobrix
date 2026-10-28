#!/bin/bash
# fetch-delta-test-jars.sh - make the OSS Delta Lake jars available for the Zarr manifest tests,
# and verify them against fixed sha256 values on every run.
#
# The transactional Zarr manifest is a Delta table, so test/zarr runs against a local
# Spark session with delta-spark. PySpark needs the jars on the JVM classpath at launch;
# python/geobrix/test/conftest.py picks them up from a Maven repo (see _delta_jars()).
# This script puts them there and checks them:
#   * looks for delta-spark and delta-storage in the repos conftest checks
#     ($GBX_M2_REPO, <checkout>/scripts/docker/m2, ~/.m2/repository);
#   * if both are missing from every repo, fetches them with `mvn dependency:get`
#     (delta-storage comes in transitively) into the Maven local repo;
#   * then verifies the sha256 of BOTH jars, whether they were already there or just fetched,
#     and fails loudly on a mismatch. These are test-only jars pulled outside the project
#     build, so the pgpverify step that guards the build does not cover them; the sha256
#     pins below do, and `-C` (strict checksums) makes Maven abort on a registry checksum
#     mismatch for the plugin and the jars. See docs/docs/security.mdx.
# Spark's --packages (Ivy) cannot be used: it does not follow the JFrog/dev-proxy mirror that
# Maven is configured with.
#
# Exit codes: 0 ok; 3 sha256 mismatch (gbx:test:python aborts on this one); 1 or the mvn exit
# code for anything else (missing mvn, failed fetch, jars not found).
#
# With GBX_M2_REPO set, Maven uses it as its local repository too (-Dmaven.repo.local), so the
# jars land where conftest looks first.
#
# Used by gbx:test:python (inside geobrix-dev) and by .github/actions/pyrx_build.
# Keep DELTA_VERSION in step with _DELTA_VERSION in python/geobrix/test/conftest.py (a wiring
# test checks this). Delta 4.0.x is the line that runs on Spark 4.0.x (pyspark 4.0.0 in the dev
# image).
#
# To bump the version: update DELTA_VERSION and both hashes (shasum -a 256 of the jars, then
# compare against the sha1 published on Maven Central), and _DELTA_VERSION in conftest.

set -euo pipefail

DELTA_VERSION="4.0.1"
SCALA="2.13"
# maven-dependency-plugin is pinned; `-C` below checksum-verifies it and its dependencies.
DEPENDENCY_PLUGIN="3.7.0"
# sha256 of the published jars, computed from the artifacts fetched through the Maven mirror and
# cross-checked against Maven Central's published .sha1 and a direct download (2026-10-22).
SHA256_DELTA_SPARK="a1e76860b19fe1e6b205a4ea7a9099a2f9575d39bb71d49b3e679b299d0efb91"
SHA256_DELTA_STORAGE="ecc42c5aaa51c423312d9e4e48aa4a12b34740bf2c37de0e8f279277ec3d12e4"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPOS=("${GBX_M2_REPO:-}" "$REPO_ROOT/scripts/docker/m2" "$HOME/.m2/repository")

SPARK_REL="io/delta/delta-spark_${SCALA}/${DELTA_VERSION}/delta-spark_${SCALA}-${DELTA_VERSION}.jar"
STORAGE_REL="io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

# Print the first repo holding both jars, or nothing.
find_repo() {
    local repo
    for repo in "${REPOS[@]}"; do
        [ -n "$repo" ] || continue
        if [ -f "$repo/$SPARK_REL" ] && [ -f "$repo/$STORAGE_REL" ]; then
            echo "$repo"
            return 0
        fi
    done
    return 0
}

# verify <repo>: both jars must match their pinned sha256.
verify() {
    local repo="$1" jar want got bad=0
    for jar in "SPARK_REL:SHA256_DELTA_SPARK" "STORAGE_REL:SHA256_DELTA_STORAGE"; do
        local rel="${jar%%:*}" pin="${jar##*:}"
        rel="${!rel}"
        want="${!pin}"
        got="$(sha256_of "$repo/$rel")"
        if [ "$got" != "$want" ]; then
            echo "delta-spark ${DELTA_VERSION}: sha256 MISMATCH for $repo/$rel" >&2
            echo "  expected $want" >&2
            echo "  got      $got" >&2
            bad=1
        fi
    done
    if [ "$bad" -ne 0 ]; then
        echo "Fix: delete io/delta/delta-spark_${SCALA}/${DELTA_VERSION} and io/delta/delta-storage/${DELTA_VERSION} under $repo and re-run; if it still mismatches, do not use the jars and report it." >&2
        exit 3
    fi
}

repo="$(find_repo)"
if [ -n "$repo" ]; then
    verify "$repo"
    echo "delta-spark ${DELTA_VERSION}: present, sha256 verified"
    exit 0
fi

# GBX_MVN names the Maven to run. CI sets it to ./mvnw (the pinned Maven); in geobrix-dev the
# image's mvn is the same version, so it stays the default there.
MVN="${GBX_MVN:-mvn}"
if ! command -v "$MVN" >/dev/null 2>&1; then
    echo "delta-spark ${DELTA_VERSION}: missing and ${MVN} is not available. Fix: run this inside geobrix-dev, or set GBX_MVN to a Maven 3.9.16 or ./mvnw." >&2
    exit 1
fi

echo "delta-spark ${DELTA_VERSION}: fetching with ${MVN} dependency:get"
# Fully qualified, version-pinned plugin coordinates: the short `dependency:` prefix does not
# resolve in geobrix-dev. -C aborts on any checksum mismatch. Maven echoes repository URLs,
# which have no place in a log, so they are redacted (the rest of each line is kept).
MVN_ARGS=(-B -q -C)
[ -z "${GBX_M2_REPO:-}" ] || MVN_ARGS+=("-Dmaven.repo.local=${GBX_M2_REPO}")
set +e
"$MVN" "${MVN_ARGS[@]}" \
    "org.apache.maven.plugins:maven-dependency-plugin:${DEPENDENCY_PLUGIN}:get" \
    -Dartifact="io.delta:delta-spark_${SCALA}:${DELTA_VERSION}" 2>&1 \
    | sed -E 's#https?://[^ )]+#<url>#g'
status=${PIPESTATUS[0]}
set -e
if [ "$status" -ne 0 ]; then
    echo "delta-spark ${DELTA_VERSION}: mvn dependency:get failed (exit $status). Fix: check the Maven mirror settings (gbx:docker:start runs docker_maven_setup.sh), then re-run." >&2
    exit "$status"
fi

repo="$(find_repo)"
if [ -z "$repo" ]; then
    echo "delta-spark ${DELTA_VERSION}: mvn succeeded but the jars are not in any repo conftest checks. Fix: set GBX_M2_REPO to Maven's local repository, then re-run." >&2
    exit 1
fi
verify "$repo"
echo "delta-spark ${DELTA_VERSION}: fetched, sha256 verified"
