#!/bin/bash
# gbx:bench:local-timing - time the forms of a levels set at three sizes on local[8] in the dev container and
# flag a form that does not scale with n (the Task 17 recipe, minutes instead of the cluster's hours).

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: Local Timing Pass (levels set, local[8], dev container)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}bash scripts/commands/gbx-bench-local-timing.sh${NC} ${YELLOW}[OPTIONS]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--set <name>${NC}     Levels set to time (default IO; only IO is implemented)"
    echo -e "  ${GREEN}--k <number>${NC}     Multiple that flags a form: vs the row UDF and vs its peers per tile (default 3)"
    echo -e "  ${GREEN}--log <path>${NC}     Write output to log file (filename -> test-logs/<name>)"
    print_lock_help
    echo -e "  ${GREEN}--help${NC}           Show this help"
    echo ""
    echo -e "${CYAN}Needs:${NC} a built JAR (python/geobrix/lib or target/) and the Rust payload"
    echo -e "  (GBX_NATIVE_ROOT, default build/env-wheel/env6/payload, from gbx:build:wheel --env 6)."
    echo -e "  Exit 2 with a PRECONDITION MISSING line when either is absent; exit 1 when the pass flags a form."
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-bench-local-timing.sh${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-bench-local-timing.sh --k 4 --log local-timing.log${NC}"
    echo ""
}

SET="IO"
K=""
LOG_PATH=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --set|--k|--log)
            if [[ $# -lt 2 ]]; then
                echo -e "${RED}Option $1 needs a value${NC}" >&2
                exit 2
            fi
            case $1 in
                --set) SET="$2" ;;
                --k)   K="$2" ;;
                --log) LOG_PATH=$(resolve_log_path "$2") ;;
            esac
            shift 2
            ;;
        --no-wait) GBX_LOCK_NO_WAIT=true; shift ;;
        --help|-h) show_help; exit 0 ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}" >&2
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"

show_banner "GeoBrix: Local Timing Pass ($SET)"
check_docker
setup_log_file "$LOG_PATH"
gbx_acquire_test_lock

# Preconditions next (a run that holds the lock): a missing artifact is not a code defect, so say so and stop (exit 2).
if ! compgen -G "python/geobrix/lib/geobrix-*-jar-with-dependencies.jar" >/dev/null && ! compgen -G "target/geobrix-*-jar-with-dependencies.jar" >/dev/null; then
    echo -e "${RED}PRECONDITION MISSING: no built JAR (python/geobrix/lib, target/); local timing not run. Build it: gbx:test:parity (or gbx:docker:exec \"mvn clean package -PskipScoverage -DskipTests\")${NC}"
    exit 2
fi
export GBX_NATIVE_ROOT="${GBX_NATIVE_ROOT:-build/env-wheel/env6/payload}"
payload="$GBX_NATIVE_ROOT"
[[ "$payload" = /* ]] || payload="$PROJECT_ROOT/$payload"
if ! ls "$payload"/_native/*/rs/_gbxrs*.so >/dev/null 2>&1; then
    echo -e "${RED}PRECONDITION MISSING: no Rust payload at $GBX_NATIVE_ROOT (needs _native/<arch>/rs/_gbxrs*.so); local timing not run. Build it: gbx:build:wheel --env 6${NC}"
    exit 2
fi

gbx_native_env_args

CMD="cd /root/geobrix && python3 -m databricks.labs.gbx.bench.local_timing --set $(printf '%q' "$SET")"
[ -n "$K" ] && CMD="$CMD --k $(printf '%q' "$K")"

echo -e "${CYAN}Timing the $SET forms on local[8] in $GBX_DEV_CONTAINER (a few minutes)...${NC}"
docker exec -e PYTHONPATH=/root/geobrix/python/geobrix/src ${GBX_NATIVE_ENV_ARGS[@]+"${GBX_NATIVE_ENV_ARGS[@]}"} \
    "$GBX_DEV_CONTAINER" $GBX_LOCK_RUN /bin/bash -c "$CMD"
EXIT=$?

if [ $EXIT -eq 0 ]; then
    echo -e "${GREEN}Local timing: no flags.${NC}"
elif [ $EXIT -eq 1 ]; then
    echo -e "${RED}Local timing flagged a form (see FLAGS above).${NC}"
fi
exit $EXIT
