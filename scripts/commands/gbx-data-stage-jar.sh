#!/bin/bash
# gbx:data:stage-jar - with --from-wheel <env wheel>: stage the JAR bundled in that wheel (sha256 checked against the wheel's RECORD, with a .sha256 sidecar) to <bundle volroot>/geobrix/<version>/jar/, no build and no container. Otherwise: mvn clean package -DskipTests then upload BOTH jars: product *-jar-with-dependencies.jar -> GBX_ARTIFACT_VOLUME/ (artifact Volume dir; dedupes to one geobrix-*-jar-with-dependencies.jar), and bench *-tests.jar -> bundle volroot (overwrite if exists); GBX_BUNDLE_SKIP_JAR_UPLOAD=1 still builds the JAR locally but skips all uploads, GBX_BUNDLE_SKIP_JAR=1 skips build and upload, GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1 skips just the tests.jar

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    echo "Usage: bash scripts/commands/gbx-data-stage-jar.sh [--from-wheel <whl>] [--profile <name>] [--log <path>] [--no-wait] [--help]"
    echo ""
    echo "Build the GeoBrix JAR (in Docker) and stage both JARs to Databricks Volumes."
    echo "With --from-wheel, stage the JAR bundled in an env wheel instead (no build, no Docker, no lock)."
    echo ""
    echo "  product *-jar-with-dependencies.jar → GBX_ARTIFACT_VOLUME/"
    echo "    (artifact Volume dir; dedupes to a single geobrix-*-jar-with-dependencies.jar)"
    echo "  bench *-tests.jar                   → bundle volroot"
    echo ""
    echo "Options:"
    echo "  --from-wheel <whl>  Stage the JAR bundled in this env wheel to <bundle volroot>/geobrix/<version>/jar/"
    echo "              with a <jar>.sha256 sidecar. Refused unless the JAR is the wheel's version and its"
    echo "              sha256 is the one in the wheel's RECORD. GBX_ARTIFACT_VOLUME is not used."
    echo "  --profile <name>  Databricks config profile for the upload (default: DATABRICKS_CONFIG_PROFILE"
    echo "              in the environment or examples/tests/databricks_cluster_config.env)"
    echo "  --log <path>  Write output to a log file (filename → test-logs/<name>)"
    echo "  --no-wait   Fail at once (exit 75) if another test run holds the container lock,"
    echo "              instead of waiting for it. The Maven build wipes target/ (mvn clean), so"
    echo "              it takes the container lock unless GBX_BUNDLE_SKIP_JAR=1."
    echo ""
    echo "Environment:"
    echo "  GBX_BUNDLE_SKIP_JAR_UPLOAD=1        Build the JAR locally; skip the Databricks upload"
    echo "  GBX_BUNDLE_SKIP_JAR=1               Skip the JAR step entirely (no build, no upload)"
    echo "  GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1  Stage only the product jar"
    echo "  GBX_BENCH_TESTS_JAR_VOLUME_PATH      Override tests.jar destination"
    echo "  GBX_PYRX_VENV=<dir>                 Python venv the upload runs on instead of .venv-pyrx"
    echo "                                      (needs Python 3.11+; databricks-sdk is added on top when it lacks it)"
    echo ""
    echo "Examples:"
    echo "  bash scripts/commands/gbx-data-stage-jar.sh"
    echo "  bash scripts/commands/gbx-data-stage-jar.sh --from-wheel dist/env6/geobrix-0.6.0+env6-py3-none-any.whl"
    echo "  GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1 bash scripts/commands/gbx-data-stage-jar.sh"
}

LOG_PATH=""
FROM_WHEEL=""
PROFILE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --help|-h) show_help; exit 0 ;;
        --from-wheel)
            if [[ -z "${2:-}" || "$2" == --* ]]; then
                echo "--from-wheel needs the path of an env wheel" >&2
                exit 2
            fi
            # absolute now: the command changes to the project root below
            FROM_WHEEL="$(cd "$(dirname "$2")" 2>/dev/null && pwd)/$(basename "$2")"
            shift 2 ;;
        --profile)
            if [[ -z "${2:-}" || "$2" == --* ]]; then
                echo "--profile needs a profile name" >&2
                exit 2
            fi
            PROFILE="$2"; shift 2 ;;
        --no-wait) GBX_LOCK_NO_WAIT=true; shift ;;
        --log) [[ $# -ge 2 ]] || { echo "Option --log needs a value" >&2; exit 2; }
               LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
    esac
done

cd "$PROJECT_ROOT" || exit 1
setup_log_file "$LOG_PATH"
# --profile wins over the config file: the launcher fills the profile from it only when the environment has none.
[[ -n "$PROFILE" ]] && export DATABRICKS_CONFIG_PROFILE="$PROFILE"

# The Maven build (mvn clean package, in stage_jar_to_volume.py) wipes target/: never under a
# running test. GBX_BUNDLE_SKIP_JAR=1 runs no Maven at all, so it needs no lock.
# --from-wheel builds nothing, so it takes no lock and needs no container.
case "$(printf '%s' "${GBX_BUNDLE_SKIP_JAR:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes) ;;
    *) [[ -n "$FROM_WHEEL" ]] || { check_docker; gbx_acquire_test_lock; } ;;
esac

# The upload uses WorkspaceClient (databricks-sdk): see gbx_python_with_sdk in common.sh. The checks of
# --from-wheel without an upload import no SDK, so they keep system python3 and need neither network nor uv.
PY_ARGS=()
[[ -n "$FROM_WHEEL" ]] && PY_ARGS+=(--from-wheel "$FROM_WHEEL")
SKIP_UPLOAD=false
case "$(printf '%s' "${GBX_BUNDLE_SKIP_JAR_UPLOAD:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes) SKIP_UPLOAD=true ;;
esac
if [[ -n "$FROM_WHEEL" && "$SKIP_UPLOAD" == true ]]; then
    GBX_SDK_PYTHON=(python3)
else
    gbx_python_with_sdk || exit 1
fi
"${GBX_SDK_PYTHON[@]}" examples/tests/stage_jar_to_volume.py ${PY_ARGS[@]+"${PY_ARGS[@]}"}
exit $?
