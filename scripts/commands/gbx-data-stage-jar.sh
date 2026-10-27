#!/bin/bash
# gbx:data:stage-jar - mvn clean package -DskipTests then upload BOTH jars: product *-jar-with-dependencies.jar -> GBX_ARTIFACT_VOLUME/ (init-script dir; dedupes to one geobrix-*-jar-with-dependencies.jar), and bench *-tests.jar -> bundle volroot (overwrite if exists); GBX_BUNDLE_SKIP_JAR_UPLOAD=1 still builds the JAR locally but skips all uploads, GBX_BUNDLE_SKIP_JAR=1 skips build and upload, GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1 skips just the tests.jar

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    echo "Usage: bash scripts/commands/gbx-data-stage-jar.sh [--no-wait] [--help]"
    echo ""
    echo "Build the GeoBrix JAR (in Docker) and stage both JARs to Databricks Volumes."
    echo ""
    echo "  product *-jar-with-dependencies.jar → GBX_ARTIFACT_VOLUME/"
    echo "    (init-script dir; dedupes to a single geobrix-*-jar-with-dependencies.jar)"
    echo "  bench *-tests.jar                   → bundle volroot"
    echo ""
    echo "Options:"
    echo "  --no-wait   Fail at once (exit 75) if another test run holds the container lock,"
    echo "              instead of waiting for it. The Maven build wipes target/ (mvn clean), so"
    echo "              it takes the container lock unless GBX_BUNDLE_SKIP_JAR=1."
    echo ""
    echo "Environment:"
    echo "  GBX_BUNDLE_SKIP_JAR_UPLOAD=1        Build the JAR locally; skip the Databricks upload"
    echo "  GBX_BUNDLE_SKIP_JAR=1               Skip the JAR step entirely (no build, no upload)"
    echo "  GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1  Stage only the product jar"
    echo "  GBX_BENCH_TESTS_JAR_VOLUME_PATH      Override tests.jar destination"
    echo ""
    echo "Examples:"
    echo "  bash scripts/commands/gbx-data-stage-jar.sh"
    echo "  GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1 bash scripts/commands/gbx-data-stage-jar.sh"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --help|-h) show_help; exit 0 ;;
        --no-wait) GBX_LOCK_NO_WAIT=true; shift ;;
        *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
    esac
done

cd "$PROJECT_ROOT" || exit 1

# The Maven build (mvn clean package, in stage_jar_to_volume.py) wipes target/: never under a
# running test. GBX_BUNDLE_SKIP_JAR=1 runs no Maven at all, so it needs no lock.
case "$(printf '%s' "${GBX_BUNDLE_SKIP_JAR:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes) ;;
    *) check_docker; gbx_acquire_test_lock ;;
esac

# Prefer the project venv interpreter -- it has databricks-sdk (the bare `python` on PATH
# often does not, and the upload uses WorkspaceClient). Fall back to PATH python.
PY="python"
if [ -x "$PROJECT_ROOT/.venv-pyrx/bin/python" ]; then
  PY="$PROJECT_ROOT/.venv-pyrx/bin/python"
fi
"$PY" examples/tests/stage_jar_to_volume.py
exit $?
