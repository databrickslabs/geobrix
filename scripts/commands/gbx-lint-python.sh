#!/bin/bash
# gbx:lint:python - Run isort, black, flake8 on Python package (same as CI)

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

run_check_docker() {
    check_docker
    echo -e "${CYAN}Running isort/black/flake8 in Docker (check only)...${NC}"
    echo ""
    show_separator
    # flake8 does NOT read pyproject.toml [tool.flake8] natively — CI relies on the
    # flake8-pyproject plugin (pinned in requirements-ci.txt) to honor its ignore list
    # (E203,E266,E501,W503) and max-line-length=88. The dev container's lockfile omits it,
    # so without this ensure-step flake8 falls back to defaults (79 cols, no ignores) and
    # floods false E501s that DON'T match CI. Install the CI-pinned version idempotently so
    # --check actually matches CI. (Pin in sync with python/geobrix/requirements-ci.txt.)
    docker exec geobrix-dev /bin/bash -c "cd /root/geobrix/python/geobrix && \
        { pip show flake8-pyproject >/dev/null 2>&1 || pip install -q 'flake8-pyproject==1.2.4' --break-system-packages; } && \
        isort --check-only src test && black --check src test && flake8 src test"
}

run_fix_docker() {
    check_docker
    echo -e "${CYAN}Applying isort and black in Docker (py3.12, same as CI), then running flake8...${NC}"
    echo ""
    show_separator
    # --fix runs in the SAME container as --check (and therefore the same Python 3.12
    # and the same CI-pinned black/isort) so auto-formatted files are byte-identical to
    # what CI's `black --check` gates on. A host Python — even at the identical black
    # version — can format some constructs differently (black's output is interpreter-
    # dependent for e.g. multiline-string call args), which then fails the CI gate. That
    # host/CI drift is exactly what this command exists to prevent, so --fix is Docker-only.
    # Docker writes to the /root/geobrix bind mount preserve host file ownership.
    # flake8-pyproject: see run_check_docker — installed idempotently so the container's
    # flake8 honors [tool.flake8] (ignore=E203,E266,E501,W503; max-line-length=88) like CI.
    docker exec geobrix-dev /bin/bash -c "cd /root/geobrix/python/geobrix && \
        { pip show flake8-pyproject >/dev/null 2>&1 || pip install -q 'flake8-pyproject==1.2.4' --break-system-packages; } && \
        isort src test && black src test && flake8 src test"
}

if [ "$MODE" = "fix" ]; then
    run_fix_docker
else
    run_check_docker
fi

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
