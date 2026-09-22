#!/bin/bash
# gbx:data:push-wheel - build JAR first (mvn clean package -DskipTests in Docker), then
# python3 -m build, and upload wheel + JARs to GBX_ARTIFACT_VOLUME/ (overwrite if exists).
# Set GBX_BUNDLE_SKIP_JAR=1 to skip the JAR step entirely (no Maven build) — for
# light-tier-only wheel changes (pyrx/pyvx/pygx); the staged JAR is left untouched.
# Set GBX_BUNDLE_SKIP_JAR_UPLOAD=1 to build the JAR locally but skip Databricks upload.
# Set GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1 to build the wheel locally but skip Databricks upload.
# Both flags together = full local build with no Databricks calls (no auth required).

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

LOG_FILE=""
SHOW_HELP=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --log)
            LOG_FILE="$(resolve_log_path "$2")"
            shift 2
            ;;
        --help|-h)
            SHOW_HELP=1
            shift
            ;;
        *)
            shift
            ;;
    esac
done

if [[ "$SHOW_HELP" == "1" ]]; then
    echo "Usage: bash scripts/commands/gbx-data-push-wheel.sh [OPTIONS]"
    echo ""
    echo "Build the GeoBrix JAR (in Docker) and Python wheel, then upload to Databricks."
    echo ""
    echo "Options:"
    echo "  --log <path>   Write output to this log file (filename → test-logs/<name>)"
    echo "  --help, -h     Show this help"
    echo ""
    echo "Environment:"
    echo "  GBX_BUNDLE_SKIP_JAR=1           Skip the JAR step ENTIRELY (no Maven build,"
    echo "                                  no upload) — light-tier-only wheel changes"
    echo "  GBX_BUNDLE_SKIP_JAR_UPLOAD=1    Build JAR locally; skip Databricks upload"
    echo "  GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1  Build wheel locally; skip Databricks upload"
    echo "  Both together = full local build, no auth required"
    echo ""
    echo "Examples:"
    echo "  bash scripts/commands/gbx-data-push-wheel.sh"
    echo "  GBX_BUNDLE_SKIP_JAR_UPLOAD=1 GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1 bash scripts/commands/gbx-data-push-wheel.sh --log my-build.log"
    exit 0
fi

cd "$PROJECT_ROOT" || exit 1

if [[ -n "$LOG_FILE" ]]; then
    mkdir -p "$(dirname "$LOG_FILE")"
    python3 notebooks/tests/push_wheel_to_volume.py 2>&1 | tee "$LOG_FILE"
    exit "${PIPESTATUS[0]}"
else
    python3 notebooks/tests/push_wheel_to_volume.py
    exit $?
fi
