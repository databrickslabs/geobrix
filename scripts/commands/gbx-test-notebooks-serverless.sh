#!/bin/bash
# gbx:test:notebooks-serverless - Run Helios example notebooks on Databricks Serverless
#
# Imports local .ipynb files into the workspace (stripping %pip/%restart_python cells),
# submits each as a one-time Serverless notebook job via jobs.submit, polls to terminal,
# and stops on the first failure.
#
# Default run (no --notebook or --dir): runs all four Helios notebooks
# (examples/notebooks/helios/0*.ipynb) on the env6 wheel with --extras stac,vizx and
# --extra-deps rich and the overturemaps CLI.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"
gbx_snapshot_exec "${BASH_SOURCE[0]}" "$@"   # run from a copy: an edit during the run cannot change it
# In the snapshot's run the code is the snapshot's, the state (venv, logs, notebooks) the source tree's.
SNAP_ROOT="$PROJECT_ROOT"
PROJECT_ROOT="${GBX_SOURCE_ROOT:-$PROJECT_ROOT}"

RUNNER_SCRIPT="$SNAP_ROOT/examples/tests/run_notebooks_serverless.py"

show_help() {
    show_banner "GeoBrix: Serverless Notebook Tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:notebooks-serverless${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--notebook PATH${NC}       Local .ipynb to run (repeatable; or use --dir)."
    echo -e "  ${GREEN}--dir DIR${NC}             Directory of .ipynb files to run (all *.ipynb, sorted)."
    echo -e "  ${GREEN}--dep-notebook PATH${NC}   Local .ipynb to import but NOT submit (e.g. config_nb.ipynb"
    echo -e "                        referenced via %%run; repeatable)."
    echo -e "  ${GREEN}--set-var NAME=VALUE${NC}  Inject a Python assignment into the uploaded copy after the"
    echo -e "                        %%run/config cell so a validation run forces compute the"
    echo -e "                        committed notebook skips (e.g. --set-var FORCE_REBUILD=True);"
    echo -e "                        the committed notebook keeps its defaults. Repeatable."
    echo -e "  ${GREEN}--ws-dir WSPATH${NC}       Workspace folder to import notebooks into."
    echo -e "                        (default: /Users/<current-user>/GeoBrix/serverless-run)"
    echo -e "  ${GREEN}--extras CSV${NC}          Geobrix extras whose deps to install (default: stac,vizx). With an"
    echo -e "                        env wheel, that wheel's extras (envs/env<N>/extra-*.txt); no light_*."
    echo -e "  ${GREEN}--extra-deps CSV${NC}      Additional pip requirements (comma-separated; a specifier keeps"
    echo -e "                        its own commas, e.g. 'rich,overturemaps>=1.0.2,<2')."
    echo -e "  ${GREEN}--wheel VOLPATH${NC}       Volume path to the geobrix wheel (default: the env6 wheel,"
    echo -e "                        /Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.6.0+env6-py3-none-any.whl)."
    echo -e "                        An env wheel (+env5 / +env6) must match --env-version."
    echo -e "  ${GREEN}--env-version VER${NC}     Serverless environment version (default: 6)."
    echo -e "  ${GREEN}--hardware-accelerator VALUE${NC} Serverless GPU AI Runtime accelerator"
    echo -e "                        (e.g. GPU_1xA10, GPU_1xH100). Omit for CPU Serverless"
    echo -e "                        (default). Pair with a GPU wheel extra via --extras."
    echo -e "  ${GREEN}--existing-cluster-id ID${NC} Run on a warm classic cluster (%%pip cells kept)."
    echo -e "  ${GREEN}--new-cluster [VER]${NC}   Run on a classic job cluster made for each run and"
    echo -e "                        terminated when it ends (%%pip cells kept; prints its id)."
    echo -e "  ${GREEN}--jar VOLPATH${NC}         --new-cluster library: the JAR, for heavyweight notebooks (also the --jar-task JAR)."
    echo -e "  ${GREEN}--access-mode MODE${NC}    --new-cluster access: dedicated (default) or standard."
    echo -e "  ${GREEN}--spark-version VER${NC}   --new-cluster runtime (default: 19.x-scala2.13)."
    echo -e "  ${GREEN}--node-type TYPE${NC}      --new-cluster node type (default: m5d.xlarge)."
    echo -e "  ${GREEN}--num-workers N${NC}       --new-cluster workers (default: 1)."
    echo -e "  ${GREEN}--profile PROFILE${NC}     Databricks config profile (default: oauth-fe or"
    echo -e "                        DATABRICKS_CONFIG_PROFILE env var)."
    echo -e "  ${GREEN}--poll-secs N${NC}         Polling interval in seconds (default: 20)."
    echo -e "  ${GREEN}--timeout-min N${NC}       Time limit of each run in minutes (default: 60); the Jobs service ends a run that"
    echo -e "                        outlives it. The launcher runs from a snapshot of its files and notebooks."
    echo -e "  ${GREEN}--no-strip-pip${NC}        Do NOT strip %%pip/%%restart_python cells."
    echo -e "  ${GREEN}--jar-task MAIN${NC}       Run ONE Serverless JAR task with this main class instead of notebooks"
    echo -e "                        (with --jar VOLPATH, the JAR, and --jar-args CSV, its parameters;"
    echo -e "                        --env-version 4 or later)."
    echo -e "  ${GREEN}--harvest-outputs${NC}     After each SUCCESS, export and harvest cell outputs"
    echo -e "                        back into the local .ipynb (aligns by source, preserves"
    echo -e "                        serialization, strips injected cells)."
    echo -e "  ${GREEN}--log <path>${NC}          Write output to log (filename → test-logs/<name>)."
    echo -e "  ${GREEN}--help${NC}                This help."
    echo ""
    echo -e "${CYAN}Environment:${NC}"
    echo -e "  ${YELLOW}GBX_PYRX_VENV=<dir>${NC}  Run the launcher on this venv instead of .venv-pyrx (Python 3.11+;"
    echo -e "                      databricks-sdk is added on top when the venv lacks it)"
    echo ""
    echo -e "${CYAN}Default behavior (no --notebook or --dir):${NC}"
    echo -e "  Runs all four Helios notebooks (examples/notebooks/helios/0*.ipynb) on the env6 wheel"
    echo -e "  with extras=stac,vizx and --extra-deps rich,overturemaps (the Overture CLI fast path)."
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-notebooks-serverless.sh${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-notebooks-serverless.sh --log helios.log${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-notebooks-serverless.sh \\${NC}"
    echo -e "    ${YELLOW}--notebook 'examples/notebooks/helios/01. Vector Engine (MVT).ipynb' \\${NC}"
    echo -e "    ${YELLOW}--extras vizx${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-notebooks-serverless.sh \\${NC}"
    echo -e "    ${YELLOW}--notebook examples/notebooks/orthomosaic/03_segment.ipynb \\${NC}"
    echo -e "    ${YELLOW}--wheel /Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.6.0+env5-py3-none-any.whl \\${NC}"
    echo -e "    ${YELLOW}--env-version 5 --extras vizx --hardware-accelerator GPU_1xA10 \\${NC}"
    echo -e "    ${YELLOW}--extra-deps 'torch==2.9.0,torchvision==0.24.0,segment-geospatial,opencv-python-headless==4.11.0.86'${NC}"
    echo ""
}

# ---------------------------------------------------------------------------
# Parse args: consume --help and --log here; forward everything else.
# ---------------------------------------------------------------------------
LOG_PATH=""
RUNNER_ARGS=()
HAS_NOTEBOOK_OR_DIR=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --notebook|--dir|--jar-task)
            HAS_NOTEBOOK_OR_DIR=true
            RUNNER_ARGS+=("$1" "$2")
            shift 2
            ;;
        --hardware-accelerator)
            RUNNER_ARGS+=("$1" "$2")
            shift 2
            ;;
        --harvest-outputs)
            RUNNER_ARGS+=("$1")
            shift
            ;;
        *)
            RUNNER_ARGS+=("$1")
            shift
            ;;
    esac
done

# ---------------------------------------------------------------------------
# If no --notebook or --dir supplied, default to the four Helios notebooks.
# ---------------------------------------------------------------------------
if [[ "$HAS_NOTEBOOK_OR_DIR" == false ]]; then
    HELIOS_DIR="$PROJECT_ROOT/examples/notebooks/helios"
    for nb in "$HELIOS_DIR"/0*.ipynb; do
        RUNNER_ARGS+=("--notebook" "$nb")
    done
    RUNNER_ARGS+=("--extra-deps" "rich,overturemaps>=1.0.2,<2")
fi

# ---------------------------------------------------------------------------
# Banner + log setup (no check_docker — this hits Databricks, not Docker).
# ---------------------------------------------------------------------------
show_banner "GeoBrix: Serverless Notebook Tests"
setup_log_file "$LOG_PATH"

# The runner needs databricks-sdk and Python >= 3.11 (tomllib): see gbx_python_with_sdk in common.sh.
# Chosen here, after --help, so help works on a machine with no SDK.
gbx_python_with_sdk || exit 1
PYTHON_CMD=("${GBX_SDK_PYTHON[@]}")

echo -e "${CYAN}Runner : ${YELLOW}$RUNNER_SCRIPT${NC}"
echo -e "${CYAN}Python : ${YELLOW}${PYTHON_CMD[*]}${NC}"
echo ""

# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------
"${PYTHON_CMD[@]}" "$RUNNER_SCRIPT" "${RUNNER_ARGS[@]}"
EXIT_CODE=$?

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}All Serverless notebook runs succeeded.${NC}"
else
    echo -e "${RED}Serverless notebook run failed (exit code: $EXIT_CODE).${NC}"
fi
show_separator

exit $EXIT_CODE
