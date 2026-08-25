#!/bin/bash
# gbx:test:notebooks-serverless - Run Helios example notebooks on Databricks Serverless
#
# Imports local .ipynb files into the workspace (stripping %pip/%restart_python cells),
# submits each as a one-time Serverless notebook job via jobs.submit, polls to terminal,
# and stops on the first failure.
#
# Default run (no --notebook or --dir): runs all four Helios notebooks
# (notebooks/examples/helios/0*.ipynb) with --extra-deps rich.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

RUNNER_SCRIPT="$PROJECT_ROOT/notebooks/tests/run_notebooks_serverless.py"

# Prefer the project pyrx venv; fall back to system python3
if [[ -x "$PROJECT_ROOT/.venv-pyrx/bin/python" ]]; then
    PYTHON="$PROJECT_ROOT/.venv-pyrx/bin/python"
else
    PYTHON="python3"
fi

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
    echo -e "  ${GREEN}--extras CSV${NC}          Geobrix extras whose deps to install"
    echo -e "                        (default: light_env6,stac,vizx,overture)."
    echo -e "  ${GREEN}--extra-deps CSV${NC}      Additional pip requirements (comma-separated)."
    echo -e "  ${GREEN}--wheel VOLPATH${NC}       Volume path to the geobrix wheel."
    echo -e "  ${GREEN}--env-version VER${NC}     Serverless environment version (default: 6)."
    echo -e "  ${GREEN}--profile PROFILE${NC}     Databricks config profile (default: oauth-fe or"
    echo -e "                        DATABRICKS_CONFIG_PROFILE env var)."
    echo -e "  ${GREEN}--poll-secs N${NC}         Polling interval in seconds (default: 20)."
    echo -e "  ${GREEN}--no-strip-pip${NC}        Do NOT strip %%pip/%%restart_python cells."
    echo -e "  ${GREEN}--log <path>${NC}          Write output to log (filename → test-logs/<name>)."
    echo -e "  ${GREEN}--help${NC}                This help."
    echo ""
    echo -e "${CYAN}Default behavior (no --notebook or --dir):${NC}"
    echo -e "  Runs all four Helios notebooks (notebooks/examples/helios/0*.ipynb)"
    echo -e "  with extras=light_env6,stac,vizx,overture and --extra-deps rich."
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-notebooks-serverless.sh${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-notebooks-serverless.sh --log helios.log${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-notebooks-serverless.sh \\${NC}"
    echo -e "    ${YELLOW}--notebook 'notebooks/examples/helios/01. Vector Engine (MVT).ipynb' \\${NC}"
    echo -e "    ${YELLOW}--extras light${NC}"
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
        --notebook|--dir)
            HAS_NOTEBOOK_OR_DIR=true
            RUNNER_ARGS+=("$1" "$2")
            shift 2
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
    HELIOS_DIR="$PROJECT_ROOT/notebooks/examples/helios"
    for nb in "$HELIOS_DIR"/0*.ipynb; do
        RUNNER_ARGS+=("--notebook" "$nb")
    done
    RUNNER_ARGS+=("--extra-deps" "rich")
fi

# ---------------------------------------------------------------------------
# Banner + log setup (no check_docker — this hits Databricks, not Docker).
# ---------------------------------------------------------------------------
show_banner "GeoBrix: Serverless Notebook Tests"
setup_log_file "$LOG_PATH"

echo -e "${CYAN}Runner : ${YELLOW}$RUNNER_SCRIPT${NC}"
echo -e "${CYAN}Python : ${YELLOW}$PYTHON${NC}"
echo ""

# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------
"$PYTHON" "$RUNNER_SCRIPT" "${RUNNER_ARGS[@]}"
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
