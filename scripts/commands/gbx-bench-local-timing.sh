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
    echo -e "  ${GREEN}--set <name>${NC}     Levels set to time (default IO; IO, S, SE, a function family of the manifest, <family>-<function> for a family's second function, or ZL = the Zarr size ladder)"
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
    echo -e "  ${GREEN}--zarr-rungs <csv>${NC}     ZL set: rungs to run (default 10MB,50MB,100MB); --zarr-families <csv> (default both);"
    echo -e "                      --zarr-cases <csv> (ZL-R1..ZL-R4); --zarr-reps <n> (default 3); --zarr-cores <n> (default 8);"
    echo -e "                      --zarr-root <dir> (the stores; default /tmp/gbx-local-timing/ZL in the container);"
    echo -e "                      --zarr-budget-mib <n> plans reads with that decoded unit budget; --zarr-plan-only plans all rungs from"
    echo -e "                      metadata-only stores (no data) and prints the tasks at the 64 and 1536 MiB budgets;"
    echo -e "                      --zarr-generate writes the missing stores first; --zarr-json-out <file> keeps every leg's metrics"
    echo -e "  ${GREEN}--dry-run${NC}        Check the set's preconditions, print the command and stop (no lock, no pass)"
    echo -e "  ${GREEN}--log <path>${NC}     Write output to log file (filename -> test-logs/<name>)"
    print_lock_help
    echo -e "  ${GREEN}--help${NC}           Show this help"
    echo ""
    echo -e "${CYAN}Needs:${NC} only what the set's forms use (the module's --needs says). The IO set (b, udf, d-py, floor) is"
    echo -e "  Python only: it needs neither. The S set needs the Rust payload for its d form (GBX_NATIVE_ROOT, default"
    echo -e "  build/env-wheel/env6/payload, from gbx:build:wheel --env 6). A form at level L1/L2/JVM-DF would need a built"
    echo -e "  JAR (python/geobrix/lib or target/). Exit 2 with a PRECONDITION MISSING line when a needed artifact is absent,"
    echo -e "  or the corpus is short, or the baseline is missing; exit 1 when the pass flags a form; exit 3 when the pass"
    echo -e "  itself fails or the --needs probe does (the probe runs with the pass' environment)."
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
ZARR_ARGS=""
DRY_RUN=false

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
        --zarr-generate|--zarr-plan-only) ZARR_ARGS="$ZARR_ARGS $1"; shift ;;
        --zarr-rungs|--zarr-families|--zarr-cases|--zarr-reps|--zarr-cores|--zarr-root|--zarr-json-out|--zarr-budget-mib)
            if [[ $# -lt 2 ]]; then
                echo -e "${RED}Option $1 needs a value${NC}" >&2
                exit 2
            fi
            ZARR_ARGS="$ZARR_ARGS $1 $(printf '%q' "$2")"; shift 2 ;;
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
        --dry-run) DRY_RUN=true; shift ;;
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

# The payload also carries the GDAL the timed reads use: pass it on when it is there, even if no form needs its kernels.
# Decided before the --needs probe, so the probe and the pass run with the same environment.
NATIVE_ROOT_DEFAULT="build/env-wheel/env6/payload"
NATIVE_ROOT_DIR="${GBX_NATIVE_ROOT:-$NATIVE_ROOT_DEFAULT}"
[[ "$NATIVE_ROOT_DIR" = /* ]] || NATIVE_ROOT_DIR="$PROJECT_ROOT/$NATIVE_ROOT_DIR"
if [ -n "${GBX_NATIVE_ROOT:-}" ] || [ -d "$NATIVE_ROOT_DIR/_native" ]; then
    export GBX_NATIVE_ROOT="${GBX_NATIVE_ROOT:-$NATIVE_ROOT_DEFAULT}"
fi
gbx_native_env_args

# The same docker exec environment for the --needs probe and the timed pass. Both run through $LOCK_PREFIX: the work
# lock in a real run (the lock is taken first, so a held lock refuses before anything reaches the container); empty in a
# --dry-run, which takes no lock and runs only the read-only probe.
# GBX_NATIVE_PTH=0: the image's startup .pth puts /opt/gbx-native's rasterio ahead of the payload's, while the per-open
# sidecar mode (pyrx readdir_option) reaches the payload's libgdal: the two differ and the mode is silently off.
# GBX_JAR: the JAR the pass starts Spark with (default: the newest of python/geobrix/lib, then target/). A path under this
# checkout becomes its /root/geobrix path. Use it to time a JAR copied out of a tree other work is rebuilding.
GBX_JAR_ARGS=()
if [ -n "${GBX_JAR:-}" ]; then
    _jar_path="$GBX_JAR"
    case "$_jar_path" in
        /root/geobrix/*) ;;
        "$PROJECT_ROOT"/*) _jar_path="/root/geobrix/${_jar_path#"$PROJECT_ROOT"/}" ;;
        /*) ;;
        *) _jar_path="/root/geobrix/${_jar_path#./}" ;;
    esac
    GBX_JAR_ARGS=(-e "GBX_JAR=$_jar_path")
fi
timing_exec() {
    docker exec -e PYTHONPATH=/root/geobrix/python/geobrix/src -e GBX_NATIVE_PTH=0 ${GBX_NATIVE_ENV_ARGS[@]+"${GBX_NATIVE_ENV_ARGS[@]}"} ${GBX_JAR_ARGS[@]+"${GBX_JAR_ARGS[@]}"} \
        "$GBX_DEV_CONTAINER" $LOCK_PREFIX "$@"
}
LOCK_PREFIX=""
if [ "$DRY_RUN" != true ]; then
    gbx_acquire_test_lock
    LOCK_PREFIX="$GBX_LOCK_RUN"
fi

# What the set's forms need (the module knows: an L1/L2 form the JAR, the L3 d form the Rust payload). Read-only (it
# runs no Maven or pytest), so a --dry-run takes no lock for it. An unknown set ends here with the module's own usage
# error (exit 2). A probe that fails otherwise (the module does not import, the container is broken) is a broken pass,
# exit 3, not a missing precondition.
NEEDS=$(timing_exec python3 -m databricks.labs.gbx.bench.local_timing --set "$SET" --needs)
PROBE_RC=$?
if [ $PROBE_RC -eq 2 ]; then
    echo "$NEEDS" >&2
    exit 2
elif [ $PROBE_RC -ne 0 ]; then
    echo "$NEEDS" >&2
    echo -e "${RED}Local timing failed: the --needs probe exited $PROBE_RC (see above): this is not a missing precondition.${NC}"
    exit 3
fi
NEED_JAR=0; NEED_RUST=0
[[ "$NEEDS" == *"jar=1"* ]] && NEED_JAR=1
[[ "$NEEDS" == *"rust=1"* ]] && NEED_RUST=1
echo -e "${CYAN}The $SET forms need: $NEEDS${NC}"

# A missing artifact is not a code defect, so say so and stop (exit 2). Only an artifact the set's forms need is demanded.
check_preconditions() {
    if [ "$NEED_JAR" = 1 ] && ! compgen -G "python/geobrix/lib/geobrix-*-jar-with-dependencies.jar" >/dev/null && ! compgen -G "target/geobrix-*-jar-with-dependencies.jar" >/dev/null; then
        echo -e "${RED}PRECONDITION MISSING: no built JAR (python/geobrix/lib, target/); local timing not run. Build it: gbx:test:parity (or gbx:docker:exec \"mvn clean package -PskipScoverage -DskipTests\")${NC}"
        exit 2
    fi
    if [ "$NEED_RUST" = 1 ] && ! ls "$NATIVE_ROOT_DIR"/_native/*/rs/_gbxrs*.so >/dev/null 2>&1; then
        echo -e "${RED}PRECONDITION MISSING: no Rust payload at ${GBX_NATIVE_ROOT:-$NATIVE_ROOT_DEFAULT} (needs _native/<arch>/rs/_gbxrs*.so); local timing not run. Build it: gbx:build:wheel --env 6${NC}"
        exit 2
    fi
}

CMD="cd /root/geobrix && python3 -m databricks.labs.gbx.bench.local_timing --set $(printf '%q' "$SET")"
[ -n "$K" ] && CMD="$CMD --k $(printf '%q' "$K")"
CMD="$CMD$BASELINE_ARGS$CORPUS_ARGS$ZARR_ARGS"

if [ "$DRY_RUN" = true ]; then
    check_preconditions
    echo -e "${CYAN}Command: $CMD${NC}"
    echo -e "${GREEN}Dry run: the preconditions for the $SET set hold; no lock taken, no pass run.${NC}"
    exit 0
fi
check_preconditions

echo -e "${CYAN}Timing the $SET forms on local[8] in $GBX_DEV_CONTAINER (a few minutes)...${NC}"
timing_exec /bin/bash -c "$CMD"
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
