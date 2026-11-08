#!/bin/bash
# gbx:test:mutation-gate - Compare an old and a slim test selection by mutation testing (mutmut 3.8.0, Tier 0, host).

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: Mutation Gate for a Test Fold"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:mutation-gate${NC} ${YELLOW}--family <name> --modules <a,b> --tests \"<old>\" --slim-tests \"<new>\" [options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--family <name>${NC}        Family name; names the work dir and the default output dir"
    echo -e "  ${GREEN}--modules <a,b,...>${NC}    Kernel modules under src/databricks/labs/gbx, e.g. pygx/_bng,pygx/_quadbin"
    echo -e "  ${GREEN}--tests \"<paths>\"${NC}      The old tests (pytest args relative to python/geobrix, one quoted string)"
    echo -e "  ${GREEN}--slim-tests \"<paths>\"${NC} The slim tests (same form)"
    echo -e "  ${GREEN}--oracle-tables <T1,T2>${NC} GBX_ORACLE_TABLES for the family harness (default: all)"
    echo -e "  ${GREEN}--out <dir>${NC}            Output dir (default test-logs/mutation-gate/<family>)"
    echo -e "  ${GREEN}--max-children <n>${NC}     Most mutmut workers this gate may use (default: its share of the host budget)"
    echo -e "  ${GREEN}--seed <n>${NC}             Sample seed (default 31)"
    echo -e "  ${GREEN}--full${NC}                 Run every mutant in both passes (default: a sample of 50, 100 after a loss)"
    echo -e "  ${GREEN}--old-codes <file>${NC}     Reuse the K_old.json of an earlier run instead of running the old tests again"
    echo -e "  ${GREEN}--old-ref <git-ref>${NC}    Run the old tests as they were at this ref (for a fold that edits or deletes test files in place)"
    echo -e "  ${GREEN}--keep-work${NC}            Keep the throwaway copy under /private/tmp (debugging)"
    echo -e "  ${GREEN}--plan${NC}                 Print the plan and stop; runs nothing"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log file"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:mutation-gate --family pygx --modules pygx/_bng,pygx/_quadbin --tests \"test/pygx/test_bng_batched_walk.py\" --slim-tests \"test/manager/test_family_cross_tier.py::test_family_oracle_l3_callables\" --oracle-tables GRID_SCALAR${NC}"
    echo ""
}

PASSTHROUGH=()
LOG_PATH=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
        --log)
            [[ $# -ge 2 ]] || { echo -e "${RED}Option --log needs a value${NC}" >&2; exit 2; }
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --family|--modules|--tests|--slim-tests|--oracle-tables|--out|--max-children|--seed|--old-codes|--old-ref)
            [[ $# -ge 2 ]] || { echo -e "${RED}Option $1 needs a value${NC}" >&2; exit 2; }
            PASSTHROUGH+=("$1" "$2")
            shift 2
            ;;
        --keep-work|--plan|--full)
            PASSTHROUGH+=("$1")
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}" >&2
            echo ""
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"
show_banner "GeoBrix: Mutation Gate for a Test Fold"
setup_log_file "$LOG_PATH"

# The host venv's python runs the driver (standard library only); the driver builds its own mutmut venv.
VENV="${GBX_PYRX_VENV:-${PROJECT_ROOT}/.venv-pyrx}"
if [[ -x "${VENV}/bin/python" ]]; then PY="${VENV}/bin/python"; else PY="$(command -v python3)"; fi
if [[ ${#PASSTHROUGH[@]} -eq 0 ]]; then
    echo -e "${RED}Missing --family, --modules, --tests and --slim-tests${NC}" >&2
    exit 2
fi

"$PY" "$SCRIPT_DIR/../mutation/gate.py" "${PASSTHROUGH[@]}"
