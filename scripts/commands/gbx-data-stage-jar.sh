#!/bin/bash
# gbx:data:stage-jar - mvn clean package -DskipTests then upload BOTH jars: product *-jar-with-dependencies.jar -> GBX_ARTIFACT_VOLUME/ (init-script dir; dedupes to one geobrix-*-jar-with-dependencies.jar), and bench *-tests.jar -> bundle volroot (overwrite if exists); GBX_BUNDLE_SKIP_JAR_UPLOAD=1 skips all, GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1 skips just the tests.jar

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Minimal --help handler (delegates real work to the Python script).
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "Usage: bash scripts/commands/gbx-data-stage-jar.sh [--help]"
    echo ""
    echo "Build the GeoBrix JAR (in Docker) and stage both JARs to Databricks Volumes."
    echo ""
    echo "  product *-jar-with-dependencies.jar → GBX_ARTIFACT_VOLUME/"
    echo "    (init-script dir; dedupes to a single geobrix-*-jar-with-dependencies.jar)"
    echo "  bench *-tests.jar                   → bundle volroot"
    echo ""
    echo "Environment:"
    echo "  GBX_BUNDLE_SKIP_JAR_UPLOAD=1        Skip build/upload entirely"
    echo "  GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1  Stage only the product jar"
    echo "  GBX_BENCH_TESTS_JAR_VOLUME_PATH      Override tests.jar destination"
    echo ""
    echo "Examples:"
    echo "  bash scripts/commands/gbx-data-stage-jar.sh"
    echo "  GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1 bash scripts/commands/gbx-data-stage-jar.sh"
    exit 0
fi

cd "$PROJECT_ROOT" || exit 1

# Prefer the project venv interpreter -- it has databricks-sdk (the bare `python` on PATH
# often does not, and the upload uses WorkspaceClient). Fall back to PATH python.
PY="python"
if [ -x "$PROJECT_ROOT/.venv-pyrx/bin/python" ]; then
  PY="$PROJECT_ROOT/.venv-pyrx/bin/python"
fi
"$PY" notebooks/tests/stage_jar_to_volume.py
exit $?
