#!/bin/bash
# gbx:lint:python - Run isort, black, flake8 on the Python package + Lakeflow connector (same as CI)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PY_DIR="$PROJECT_ROOT/python/geobrix"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "Lint: Python (isort, black, flake8)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:lint:python${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--check${NC}         Check only (no edits). Default when run in CI or without --fix."
    echo -e "  ${GREEN}--fix${NC}            Apply isort and black (in Docker, py3.12 — same as CI), then run flake8."
    echo -e "  ${GREEN}--log <path>${NC}     Write output to log file."
    echo -e "  ${GREEN}--help${NC}           Show this help."
    echo ""
    echo -e "${CYAN}Modes:${NC}"
    echo -e "  ${YELLOW}--check${NC}  Runs in Docker (same as CI). Fails if imports or format are not clean."
    echo -e "  ${YELLOW}--fix${NC}    Runs in Docker too (py3.12 + CI-pinned black/isort), so auto-formatted"
    echo -e "           files are byte-identical to what the CI gate checks. A host Python"
    echo -e "           (e.g. 3.10) formats some constructs differently even at the same black"
    echo -e "           version, which then fails CI — so --fix does NOT run on the host."
    echo ""
}

MODE="check"
LOG_PATH=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --check)
            MODE="check"
            shift
            ;;
        --fix)
            MODE="fix"
            shift
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"
show_banner "Lint: Python (isort, black, flake8)"
setup_log_file "$LOG_PATH"

# One in-container driver for both modes. Each scope (a project dir + the paths in it) is
# linted from its own cwd, so isort/black/flake8 read THAT project's pyproject.toml:
#   - python/geobrix      src test   (the package; unchanged)
#   - connectors/lakeflow sources    (the Lakeflow Connect connector; its own pyproject.toml)
# Keep this list identical to the "Lint Python" step in .github/actions/python_build/action.yml.
# ISORT_FLAGS/BLACK_FLAGS are "--check-only"/"--check" in check mode and empty in fix mode.
# flake8 does NOT read pyproject.toml [tool.flake8] natively — CI relies on the
# flake8-pyproject plugin (pinned in requirements-ci.txt) to honor its ignore list
# (E203,E266,E501,E704,W503) and max-line-length=88. The dev container's lockfile omits it,
# so without the ensure-step flake8 falls back to defaults (79 cols, no ignores) and
# floods false E501s that DON'T match CI. Install the CI-pinned version idempotently so
# the run actually matches CI. (Pin in sync with python/geobrix/requirements-ci.txt.)
read -r -d '' LINT_SCRIPT <<'LINT_EOF'
{ pip show flake8-pyproject >/dev/null 2>&1 || pip install -q 'flake8-pyproject==1.2.4' --break-system-packages; }
overall=0
lint_scope() {
    scope_dir="$1"; shift
    cd "/root/geobrix/$scope_dir" || return 1
    echo ''; echo "=== Lint scope: $scope_dir ($*) ==="
    isort_exit=0; black_exit=0; flake8_exit=0
    isort $ISORT_FLAGS "$@" || isort_exit=$?
    black $BLACK_FLAGS "$@" || black_exit=$?
    flake8 "$@" || flake8_exit=$?
    echo ''; echo "Lint stage results ($scope_dir):"
    [ $isort_exit -eq 0 ] && echo '  isort: PASS' || echo "  isort: FAIL (exit code: $isort_exit)"
    [ $black_exit -eq 0 ] && echo '  black: PASS' || echo "  black: FAIL (exit code: $black_exit)"
    [ $flake8_exit -eq 0 ] && echo '  flake8: PASS' || echo "  flake8: FAIL (exit code: $flake8_exit)"
    [ $isort_exit -eq 0 ] && [ $black_exit -eq 0 ] && [ $flake8_exit -eq 0 ] || overall=1
}
lint_scope python/geobrix src test
lint_scope connectors/lakeflow sources
exit $overall
LINT_EOF

run_lint_docker() {
    check_docker
    if [ "$MODE" = "fix" ]; then
        echo -e "${CYAN}Applying isort and black in Docker (py3.12, same as CI), then running flake8...${NC}"
        ISORT_FLAGS=""; BLACK_FLAGS=""
    else
        echo -e "${CYAN}Running isort/black/flake8 in Docker (check only)...${NC}"
        ISORT_FLAGS="--check-only"; BLACK_FLAGS="--check"
    fi
    echo ""
    show_separator
    # --fix runs in the SAME container as --check (and therefore the same Python 3.12
    # and the same CI-pinned black/isort) so auto-formatted files are byte-identical to
    # what CI's `black --check` gates on. A host Python — even at the identical black
    # version — can format some constructs differently (black's output is interpreter-
    # dependent for e.g. multiline-string call args), which then fails the CI gate. That
    # host/CI drift is exactly what this command exists to prevent, so --fix is Docker-only.
    # Docker writes to the /root/geobrix bind mount preserve host file ownership.
    docker exec -e ISORT_FLAGS="$ISORT_FLAGS" -e BLACK_FLAGS="$BLACK_FLAGS" \
        geobrix-dev /bin/bash -c "$LINT_SCRIPT"
}

run_lint_docker
EXIT_CODE=$?
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}Python lint passed.${NC}"
else
    echo -e "${RED}Python lint failed (exit code: $EXIT_CODE).${NC}"
    [ "$MODE" = "check" ] && echo -e "${CYAN}Tip: run with ${YELLOW}--fix${NC}${CYAN} (in Docker, py3.12 — same as CI) to auto-fix isort/black.${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
