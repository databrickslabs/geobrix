#!/bin/bash
# gbx:data:stage-wheel - build JAR first (mvn clean package -DskipTests in Docker), then
# python3 -m build, and stage wheel to BOTH locations: GBX_ARTIFACT_VOLUME/ (init-script
# dir; dedupes to one geobrix-*.whl) and the bundle volroot (bundle/%pip path). Set
# GBX_BUNDLE_SKIP_JAR=1 or GBX_BUNDLE_SKIP_JAR_UPLOAD=1 to skip the JAR step entirely
# (no Maven build, no JAR upload) — for light-tier-only wheel changes (pyrx/pyvx/pygx);
# the staged JAR and python/geobrix/lib/ are left untouched.
# Set GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1 to build the wheel locally but skip Databricks upload.
# Skip-JAR + SKIP_WHEEL_UPLOAD = local wheel-only build with no Maven and no Databricks
# calls (no auth required). A failure to list/clean the artifact dir exits non-zero.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

LOG_FILE=""
SHOW_HELP=0
STAGE_ARGS=()
ENV_STAGE=false

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
        --no-wait)
            GBX_LOCK_NO_WAIT=true
            shift
            ;;
        --env)
            case "${2:-}" in 5|6|both) ;; *) echo "--env must be 5, 6 or both" >&2; exit 2 ;; esac
            STAGE_ARGS+=(--env "$2"); ENV_STAGE=true
            shift 2
            ;;
        --no-prune)
            STAGE_ARGS+=(--no-prune)
            shift
            ;;
        *)
            shift
            ;;
    esac
done

if [[ "$SHOW_HELP" == "1" ]]; then
    echo "Usage: bash scripts/commands/gbx-data-stage-wheel.sh [OPTIONS]"
    echo ""
    echo "Build the GeoBrix JAR (in Docker, unless skipped) and Python wheel, then stage to Databricks."
    echo ""
    echo "Destinations:"
    echo "  GBX_ARTIFACT_VOLUME/   (init-script dir; dedupes to a single geobrix-*.whl)"
    echo "  bundle volroot/        (bundle/%pip path; overwrite in place)"
    echo ""
    echo "Options:"
    echo "  --env 5|6|both Stage the env wheel(s) instead: dist/env<N>/geobrix-<version>+env<N>-py3-none-any.whl"
    echo "                 (gbx:build:wheel --env <N> builds it). No JAR step and no build; only the bundle"
    echo "                 volroot (the %pip path), by the wheel's own +env<N> name. The init-script dir is"
    echo "                 left as it is, so no other wheel there is touched."
    echo "  --no-prune     Keep every other geobrix-*.whl in the init-script dir (skip the single-version cleanup)"
    echo "  --log <path>   Write output to this log file (filename → test-logs/<name>)"
    print_lock_help
    echo "  --help, -h     Show this help"
    echo ""
    echo "Environment:"
    echo "  GBX_BUNDLE_SKIP_JAR=1           Skip the JAR step ENTIRELY (no Maven build,"
    echo "                                  no upload) — light-tier-only wheel changes"
    echo "  GBX_BUNDLE_SKIP_JAR_UPLOAD=1    Same as GBX_BUNDLE_SKIP_JAR=1 for this command"
    echo "                                  (no Maven build, no JAR upload)"
    echo "  GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1  Build wheel locally; skip Databricks upload"
    echo "  A skip-JAR flag + SKIP_WHEEL_UPLOAD = local wheel-only build, no auth required"
    echo ""
    echo "Examples:"
    echo "  bash scripts/commands/gbx-data-stage-wheel.sh"
    echo "  # light-tier-only wheel (no Maven):"
    echo "  GBX_BUNDLE_SKIP_JAR_UPLOAD=1 bash scripts/commands/gbx-data-stage-wheel.sh --log my-build.log"
    echo "  # both env wheels (the Databricks install), built first with gbx:build:wheel:"
    echo "  bash scripts/commands/gbx-data-stage-wheel.sh --env both"
    exit 0
fi

cd "$PROJECT_ROOT" || exit 1

# The JAR step runs `mvn clean package`, which wipes target/: never under a running test, so it
# takes the container lock. With a skip-JAR flag no Maven runs and no lock is needed.
JAR_STEP=true
[ "$ENV_STAGE" = true ] && JAR_STEP=false  # the env wheel carries its own JAR (_jar/)
for flag in GBX_BUNDLE_SKIP_JAR GBX_BUNDLE_SKIP_JAR_UPLOAD; do
    case "$(printf '%s' "${!flag:-}" | tr '[:upper:]' '[:lower:]')" in
        1|true|yes) JAR_STEP=false ;;
    esac
done
if [ "$JAR_STEP" = true ]; then
    check_docker
    gbx_acquire_test_lock
fi

# The upload uses WorkspaceClient (databricks-sdk): see gbx_python_with_sdk in common.sh. With
# no .venv-pyrx the launcher falls back to `python -m build` on that interpreter, so ask for
# `build` as well. A local-only build (GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1) imports no SDK, so it
# keeps system python3 and needs neither network nor uv.
case "$(printf '%s' "${GBX_BUNDLE_SKIP_WHEEL_UPLOAD:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes) GBX_SDK_PYTHON=(python3) ;;
    *) gbx_python_with_sdk build || exit 1 ;;
esac

if [[ -n "$LOG_FILE" ]]; then
    mkdir -p "$(dirname "$LOG_FILE")"
    "${GBX_SDK_PYTHON[@]}" examples/tests/stage_wheel_to_volume.py ${STAGE_ARGS[@]+"${STAGE_ARGS[@]}"} 2>&1 | tee "$LOG_FILE"
    exit "${PIPESTATUS[0]}"
else
    "${GBX_SDK_PYTHON[@]}" examples/tests/stage_wheel_to_volume.py ${STAGE_ARGS[@]+"${STAGE_ARGS[@]}"}
    exit $?
fi
