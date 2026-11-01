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
    echo -e "  ${GREEN}--set <name>${NC}     Levels set to time (default IO; have IO, S)"
    echo -e "  ${GREEN}--k <number>${NC}     Multiple that flags a form: vs the row UDF and vs its peers per tile (default 3)"
    echo -e "  ${GREEN}--baseline [path]${NC}  Also flag a form whose cost per tile is over --baseline-k times the stored one"
    echo -e "                      (default path: test-logs/local-timing-baseline-<set>.json; untracked)"
    echo -e "  ${GREEN}--baseline-write${NC} Store this pass' cost per tile as the baseline (at --baseline path, or the default)."
    echo -e "                      A pass that flags a form is not stored"
    echo -e "  ${GREEN}--baseline-k <number>${NC}  Multiple of the baseline that flags a form (default 1.5)"
    echo -e "  ${GREEN}--repeats <n>${NC}    Measured runs per n; the table and the baseline use their median. Default 1 for the plain"
    echo -e "                      pass, 3 with --baseline or --baseline-write (--baseline-write needs at least 3)"
    echo -e "  ${GREEN}--corpus-dir <path>${NC}  A folder named rows is read in place (no copy; it must hold the tiles). Use the big folder"
    echo -e "                      /Volumes/main/default/bench-corpus/rows once before a cluster round: the default pass times a"
    echo -e "                      96-file copy in /tmp, where a worker's folder listing never shows. Prints the readdir_mode its tiles get"
    echo -e "  ${GREEN}--log <path>${NC}     Write output to log file (filename -> test-logs/<name>)"
    print_lock_help
    echo -e "  ${GREEN}--help${NC}           Show this help"
    echo ""
    echo -e "${CYAN}Needs:${NC} a built JAR (python/geobrix/lib or target/) and the Rust payload"
    echo -e "  (GBX_NATIVE_ROOT, default build/env-wheel/env6/payload, from gbx:build:wheel --env 6)."
    echo -e "  Exit 2 with a PRECONDITION MISSING line when either is absent, or the corpus is short, or the baseline"
    echo -e "  is missing; exit 1 when the pass flags a form; exit 3 when the pass itself fails."
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-bench-local-timing.sh${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-bench-local-timing.sh --k 4 --log local-timing.log${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-bench-local-timing.sh --baseline-write${NC}   (after a change you trust)"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-bench-local-timing.sh --baseline${NC}         (before a cluster round)"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-bench-local-timing.sh --corpus-dir /Volumes/main/default/bench-corpus/rows${NC}   (the big-folder shape, once before a cluster round)"
    echo ""
}

SET="IO"
K=""
LOG_PATH=""
BASELINE_ARGS=""
CORPUS_ARGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --baseline)
            # the path is optional: take the next word unless it is another option
            if [[ $# -ge 2 && "$2" != --* ]]; then
                BASELINE_ARGS="$BASELINE_ARGS --baseline $(printf '%q' "$2")"; shift 2
            else
                BASELINE_ARGS="$BASELINE_ARGS --baseline"; shift
            fi
            ;;
        --baseline-write) BASELINE_ARGS="$BASELINE_ARGS --baseline-write"; shift ;;
        --set|--k|--log|--baseline-k|--repeats|--corpus-dir)
            if [[ $# -lt 2 ]]; then
                echo -e "${RED}Option $1 needs a value${NC}" >&2
                exit 2
            fi
            case $1 in
                --set) SET="$2" ;;
                --k)   K="$2" ;;
                --repeats) BASELINE_ARGS="$BASELINE_ARGS --repeats $(printf '%q' "$2")" ;;
                --baseline-k) BASELINE_ARGS="$BASELINE_ARGS --baseline-k $(printf '%q' "$2")" ;;
                --corpus-dir) CORPUS_ARGS=" --corpus-dir $(printf '%q' "$2")" ;;
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
CMD="$CMD$BASELINE_ARGS$CORPUS_ARGS"

echo -e "${CYAN}Timing the $SET forms on local[8] in $GBX_DEV_CONTAINER (a few minutes)...${NC}"
# GBX_NATIVE_PTH=0: the image's startup .pth puts /opt/gbx-native's rasterio ahead of the payload's, while the per-open
# sidecar mode (pyrx readdir_option) reaches the payload's libgdal: the two differ and the mode is silently off.
docker exec -e PYTHONPATH=/root/geobrix/python/geobrix/src -e GBX_NATIVE_PTH=0 ${GBX_NATIVE_ENV_ARGS[@]+"${GBX_NATIVE_ENV_ARGS[@]}"} \
    "$GBX_DEV_CONTAINER" $GBX_LOCK_RUN /bin/bash -c "$CMD"
EXIT=$?

if [ $EXIT -eq 0 ]; then
    echo -e "${GREEN}Local timing: no flags.${NC}"
elif [ $EXIT -eq 1 ]; then
    echo -e "${RED}Local timing flagged a form (see FLAGS above).${NC}"
elif [ $EXIT -eq 2 ]; then
    echo -e "${RED}Local timing not run: a precondition is missing (see PRECONDITION MISSING above).${NC}"
else
    echo -e "${RED}Local timing failed (exit $EXIT; see above): this is not a flag.${NC}"
fi
exit $EXIT
