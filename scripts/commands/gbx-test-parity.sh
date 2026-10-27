#!/bin/bash
# gbx:test:parity - Run the cross-tier (light vs heavy) parity suites as a real gate.
#
# The parity suites are integration-marked and need the assembly JAR staged where the
# light tier's Spark session can load it. Left to a plain `gbx:test:python` run they are
# SKIPPED, and a skipped suite reads as green -- so this command exists to make them a
# deliberate, self-contained gate: it rebuilds the JAR (unless told not to), stages it,
# runs the suites with integration enabled, and FAILS if everything skipped.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

# The cross-tier parity suites are DISCOVERED, not listed: a hand-kept list silently skipped
# every suite added after it was written (a skipped parity suite reads as a passing gate).
# A suite is any python/geobrix/test/**/test_parity*.py or test_*_parity.py. The few that are
# not cross-tier gates are named in PARITY_EXCLUDE with the reason; anything new is gated by
# default until it is excluded on purpose.
#
# Each file is run in its OWN pytest process, deliberately. `spark.jars` only takes effect
# at JVM startup, so a parity module that loads after some other test already created a
# JAR-free SparkSession cannot get the JAR and self-skips. Running the whole directory in
# one process therefore skipped every parity test while the run still exited 0 -- exactly
# the silently-not-running gate this command exists to prevent.
PARITY_ROOT="python/geobrix/test"
PARITY_EXCLUDE=(
    "python/geobrix/test/core/test_check_binding_parity.py"      # unit test of the binding-parity checker (gbx:test:bindings)
    "python/geobrix/test/ds/test_vector_writer_parity.py"        # light-only writer round trip, not a tier comparison
    "python/geobrix/test/ds/test_writer_parity.py"               # light-only writer round trip, not a tier comparison
    "python/geobrix/test/ds/test_reader_parity.py"               # skips by design when the heavy gdal reader yields 0 rows
)

# discover_parity_files [dir]: parity suites under dir (default the whole test tree), sorted.
discover_parity_files() {
    local root="${1:-$PARITY_ROOT}" f e skip
    while IFS= read -r f; do
        skip=false
        for e in "${PARITY_EXCLUDE[@]}"; do
            [ "$f" = "$e" ] && skip=true
        done
        [ "$skip" = true ] || echo "$f"
    done < <(cd "$PROJECT_ROOT" && find -L "$root" -type f \( -name 'test_parity*.py' -o -name 'test_*_parity.py' \) | LC_ALL=C sort)
}

show_help() {
    show_banner "⚖️  GeoBrix: Cross-Tier Parity Tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}bash scripts/commands/gbx-test-parity.sh${NC} ${YELLOW}[OPTIONS]${NC}"
    echo ""
    echo -e "${CYAN}What it does:${NC}"
    echo -e "  1. Rebuilds the assembly JAR (skip with ${GREEN}--skip-build${NC})"
    echo -e "  2. Stages it to ${YELLOW}python/geobrix/lib/${NC} where the light tier loads it"
    echo -e "  3. Runs every parity suite found under ${YELLOW}python/geobrix/test/${NC} (test_parity*.py and"
    echo -e "     test_*_parity.py, minus the documented exclusions) with ${YELLOW}@pytest.mark.integration${NC} ENABLED"
    echo -e "  4. Fails if every test skipped (a skipped gate is not a passing gate)"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--path <path>${NC}      Run this suite file, or the parity suites under this directory, instead of"
    echo -e "                       all of them; repeat it to run several (relative to the repo root)"
    echo -e "  ${GREEN}-k <expr>${NC}          Pytest keyword filter"
    echo -e "  ${GREEN}--skip-build${NC}       Reuse the staged JAR (fast; only if Scala is unchanged)"
    echo -e "  ${GREEN}--log <path>${NC}       Write output to log file (filename → test-logs/<name>)"
    print_lock_help
    echo -e "  ${GREEN}--help${NC}             Show this help"
    echo ""
    echo -e "${CYAN}Why the rebuild is the default:${NC}"
    echo -e "  A staged JAR older than the Scala sources makes every heavy call fail with"
    echo -e "  ${YELLOW}UNRESOLVED_ROUTINE${NC} — a mass failure that looks like a code bug but is"
    echo -e "  just a stale artifact. Rebuilding first removes that whole failure mode."
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-parity.sh${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-parity.sh --skip-build -k crs${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-parity.sh --skip-build --path python/geobrix/test/pyvx/test_parity_mvt.py --path python/geobrix/test/pygx/test_parity_bng.py${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-parity.sh --log parity.log${NC}"
    echo ""
}

TEST_PATHS=()
LOG_PATH=""
KEYWORD=""
SKIP_BUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --path)       TEST_PATHS+=("${2%/}"); shift 2 ;;
        -k)           KEYWORD="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --log)        LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        --no-wait)    GBX_LOCK_NO_WAIT=true; shift ;;
        --help|-h)    show_help; exit 0 ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"; echo ""; show_help; exit 1 ;;
    esac
done

cd "$PROJECT_ROOT"

show_banner "⚖️  GeoBrix: Cross-Tier Parity Tests"
check_docker
setup_log_file "$LOG_PATH"
gbx_acquire_test_lock

STAGE_DIR="$PROJECT_ROOT/python/geobrix/lib"

# --- Step 1: build the assembly JAR ---------------------------------------------------
if [ "$SKIP_BUILD" = true ]; then
    echo -e "${YELLOW}⏭️  Skipping JAR build (--skip-build)${NC}"
    if ! ls "$STAGE_DIR"/geobrix-*-jar-with-dependencies.jar >/dev/null 2>&1; then
        echo -e "${RED}❌ No staged JAR in $STAGE_DIR and --skip-build was given.${NC}"
        echo -e "   Re-run without ${GREEN}--skip-build${NC} to build and stage it."
        exit 1
    fi
    warn_if_jar_stale "$PROJECT_ROOT"
else
    echo -e "${CYAN}🔨 Building assembly JAR (skipScoverage, no tests)...${NC}"
    docker exec geobrix-dev /bin/bash -c \
        "$DOCKER_MAVEN_ENV && cd /root/geobrix && mvn -o -q -P skipScoverage clean package -DskipTests"
    BUILD_EXIT=$?
    if [ $BUILD_EXIT -ne 0 ]; then
        echo -e "${RED}❌ JAR build failed (exit $BUILD_EXIT) — parity cannot run without it.${NC}"
        exit $BUILD_EXIT
    fi
fi

# --- Step 2: stage it where the light tier's Spark session looks for it ----------------
if [ "$SKIP_BUILD" != true ]; then
    # Shared with gbx:test:python (common.sh); keeps ONLY the current JAR staged.
    if ! stage_jar_from_target "$PROJECT_ROOT"; then
        echo -e "${RED}❌ Build succeeded but no assembly JAR found under target/.${NC}"
        exit 1
    fi
fi

# --- Step 3: run each parity suite in its OWN process, integration ENABLED ------------
RUN_FILES=()
add_run_file() {   # once each, in the order given
    local f="$1" seen
    for seen in ${RUN_FILES[@]+"${RUN_FILES[@]}"}; do [ "$seen" = "$f" ] && return; done
    RUN_FILES+=("$f")
}
if [ ${#TEST_PATHS[@]} -gt 0 ]; then
    for p in "${TEST_PATHS[@]}"; do
        if [ -d "$PROJECT_ROOT/$p" ]; then
            while IFS= read -r f; do add_run_file "$f"; done < <(discover_parity_files "$p")
        else
            add_run_file "$p"   # a file; a missing one fails the run below, by name
        fi
    done
else
    while IFS= read -r f; do add_run_file "$f"; done < <(discover_parity_files)
fi
if [ ${#RUN_FILES[@]} -eq 0 ]; then
    echo -e "${RED}❌ No parity suites found${NC}"
    exit 1
fi

echo ""
echo -e "${CYAN}🎯 Parity suites: ${YELLOW}${#RUN_FILES[@]}${NC}${CYAN} file(s), each in its own process${NC}"
echo -e "${CYAN}🏷️  Markers: ${YELLOW}(none — integration ENABLED; parity is the point)${NC}"
echo ""

TOTAL_PASSED=0
TOTAL_SKIPPED_FILES=()
FAILED_FILES=()

for f in "${RUN_FILES[@]}"; do
    show_separator
    echo -e "${CYAN}▶ $f${NC}"
    show_separator
    if [ ! -f "$PROJECT_ROOT/$f" ]; then
        echo -e "${RED}❌ No such suite file: $f${NC}"
        FAILED_FILES+=("$f (missing)")
        continue
    fi

    REPORT="/root/geobrix/target/parity-$(basename "$f" .py).txt"
    PYTEST_CMD="unset JAVA_TOOL_OPTIONS && cd /root/geobrix && \
        python3 -m pytest /root/geobrix/$f -v --tb=short --color=yes -rs"
    [ -n "$KEYWORD" ] && PYTEST_CMD="$PYTEST_CMD -k '$KEYWORD'"
    PYTEST_CMD="$PYTEST_CMD | tee $REPORT"

    docker exec geobrix-dev /bin/bash -c "set -o pipefail; $PYTEST_CMD"
    FILE_EXIT=$?

    SUMMARY=$(docker exec geobrix-dev /bin/bash -c \
        "tail -6 $REPORT 2>/dev/null | grep -E 'passed|failed|skipped|no tests ran'" 2>/dev/null)
    N_PASSED=$(echo "$SUMMARY" | grep -oE '[0-9]+ passed' | grep -oE '[0-9]+' | head -1)
    N_PASSED=${N_PASSED:-0}
    N_SKIPPED=$(echo "$SUMMARY" | grep -oE '[0-9]+ skipped' | grep -oE '[0-9]+' | head -1)
    N_SKIPPED=${N_SKIPPED:-0}
    TOTAL_PASSED=$((TOTAL_PASSED + N_PASSED))

    if [ $FILE_EXIT -ne 0 ]; then
        FAILED_FILES+=("$f")
    elif [ "$N_PASSED" -eq 0 ] || [ "$N_SKIPPED" -gt 0 ]; then
        # Exit 0 while some or all of the file did not run: the silent non-gate this
        # command exists to catch. Requiring ZERO skips (not merely >=1 pass) closes the
        # partial-skip loophole -- a file where most tests skip and one passes would
        # otherwise read green. Every test in every parity file transitively needs the
        # JAR-bearing session, so in a healthy run no parity test skips at all.
        TOTAL_SKIPPED_FILES+=("$f ($N_PASSED passed, $N_SKIPPED skipped)")
    fi
    echo ""
done

# --- Step 4: a suite that skipped is NOT a passing gate -------------------------------
EXIT_CODE=0
show_separator
if [ ${#FAILED_FILES[@]} -gt 0 ]; then
    echo -e "${RED}❌ Cross-tier parity FAILED in ${#FAILED_FILES[@]} suite(s):${NC}"
    for f in "${FAILED_FILES[@]}"; do echo -e "     ${YELLOW}$f${NC}"; done
    echo -e "${CYAN}   Mass ${YELLOW}UNRESOLVED_ROUTINE${CYAN} failures mean a stale JAR — re-run without --skip-build.${NC}"
    EXIT_CODE=1
fi
if [ ${#TOTAL_SKIPPED_FILES[@]} -gt 0 ]; then
    echo -e "${RED}❌ Cross-tier parity did NOT RUN in ${#TOTAL_SKIPPED_FILES[@]} suite(s) — 0 tests passed:${NC}"
    for f in "${TOTAL_SKIPPED_FILES[@]}"; do echo -e "     ${YELLOW}$f${NC}"; done
    echo -e "${CYAN}   A skipped parity suite is not a passing gate. Common causes:${NC}"
    echo -e "     • no JAR staged in python/geobrix/lib/ (run without ${GREEN}--skip-build${NC})"
    echo -e "     • a JAR-free Spark session already live in the same process"
    echo -e "     • the suite's own dependency guard (e.g. mapbox-vector-tile) is unmet"
    EXIT_CODE=1
fi
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Cross-tier parity passed — $TOTAL_PASSED test(s) actually ran across ${#RUN_FILES[@]} suite(s).${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
