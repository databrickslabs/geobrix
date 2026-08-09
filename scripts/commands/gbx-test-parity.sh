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

# Every cross-tier parity suite, ONE PER LINE. Add new ones here so this stays the
# single gate.
#
# Each file is run in its OWN pytest process, deliberately. `spark.jars` only takes effect
# at JVM startup, so a parity module that loads after some other test already created a
# JAR-free SparkSession cannot get the JAR and self-skips. Running the whole directory in
# one process therefore skipped every parity test while the run still exited 0 -- exactly
# the silently-not-running gate this command exists to prevent.
PARITY_FILES=(
    "python/geobrix/test/pyvx/test_crs_parity.py"
    "python/geobrix/test/pyvx/test_parity_mvt.py"
    "python/geobrix/test/pyvx/test_parity_tin.py"
    "python/geobrix/test/pyvx/test_parity_legacy.py"
    "python/geobrix/test/pyvx/test_parity_h3_tessellate.py"
    "python/geobrix/test/pygx/test_gridx_error_parity.py"
)

show_help() {
    show_banner "⚖️  GeoBrix: Cross-Tier Parity Tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}bash scripts/commands/gbx-test-parity.sh${NC} ${YELLOW}[OPTIONS]${NC}"
    echo ""
    echo -e "${CYAN}What it does:${NC}"
    echo -e "  1. Rebuilds the assembly JAR (skip with ${GREEN}--skip-build${NC})"
    echo -e "  2. Stages it to ${YELLOW}python/geobrix/lib/${NC} where the light tier loads it"
    echo -e "  3. Runs the parity suites with ${YELLOW}@pytest.mark.integration${NC} ENABLED"
    echo -e "  4. Fails if every test skipped (a skipped gate is not a passing gate)"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--path <path>${NC}      Run ONE suite file instead of all parity suites"
    echo -e "  ${GREEN}-k <expr>${NC}          Pytest keyword filter"
    echo -e "  ${GREEN}--skip-build${NC}       Reuse the staged JAR (fast; only if Scala is unchanged)"
    echo -e "  ${GREEN}--log <path>${NC}       Write output to log file (filename → test-logs/<name>)"
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
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-parity.sh --log parity.log${NC}"
    echo ""
}

TEST_PATH=""
LOG_PATH=""
KEYWORD=""
SKIP_BUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --path)       TEST_PATH="$2"; shift 2 ;;
        -k)           KEYWORD="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --log)        LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        --help|-h)    show_help; exit 0 ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"; echo ""; show_help; exit 1 ;;
    esac
done

cd "$PROJECT_ROOT"

show_banner "⚖️  GeoBrix: Cross-Tier Parity Tests"
check_docker
setup_log_file "$LOG_PATH"

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
    BUILT_JAR=$(ls -t "$PROJECT_ROOT"/target/geobrix-*-jar-with-dependencies.jar 2>/dev/null | head -1)
    if [ -z "$BUILT_JAR" ]; then
        echo -e "${RED}❌ Build succeeded but no assembly JAR found under target/.${NC}"
        exit 1
    fi
    mkdir -p "$STAGE_DIR"
    # Keep ONLY the current version staged, so a stale JAR can never be picked instead.
    rm -f "$STAGE_DIR"/geobrix-*-jar-with-dependencies.jar
    cp "$BUILT_JAR" "$STAGE_DIR/"
    echo -e "${GREEN}📦 Staged $(basename "$BUILT_JAR") → python/geobrix/lib/${NC}"
fi

# --- Step 3: run each parity suite in its OWN process, integration ENABLED ------------
if [ -n "$TEST_PATH" ]; then
    RUN_FILES=("$TEST_PATH")
else
    RUN_FILES=("${PARITY_FILES[@]}")
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
