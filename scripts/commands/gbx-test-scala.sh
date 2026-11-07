#!/bin/bash
# gbx:test:scala - Run Scala unit tests (non-docs)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "🧪 GeoBrix: Scala Tests (Non-Docs)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:scala${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--by-package${NC}           Run tests per package in sequence (rasterx, gridx, vectorx, expressions, util); reports pass/fail per package"
    echo -e "  ${GREEN}--suite <pattern>${NC}       Run specific test suite (single pattern)"
    echo -e "  ${GREEN}--suites <list>${NC}        Run specific test suites (comma-separated class/package patterns)"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log file"
    echo -e "  ${GREEN}--tags-to-exclude <tag>${NC}  Override tagsToExclude (use NoOpExcludeSentinel to un-exclude a tag)"
    echo -e "  ${GREEN}--spark-version <v>${NC}    Build and test against another Spark (4.0.x default, 4.1.x, 4.2.x, 4.3.x); sets spark, scala and jackson versions"
    echo -e "  ${GREEN}--compile-only${NC}         Compile main and test sources only (mvn test-compile); no suites run"
    echo -e "  ${GREEN}--verbose${NC}              Increase Maven verbosity (-X flag)"
    print_lock_help
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Log Path Behavior:${NC}"
    echo -e "  ${YELLOW}filename.log${NC}           → test-logs/filename.log"
    echo -e "  ${YELLOW}subdir/file.log${NC}        → test-logs/subdir/file.log"
    echo -e "  ${YELLOW}/abs/path/file.log${NC}     → /abs/path/file.log"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:scala${NC}"
    echo -e "  ${YELLOW}gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.*'${NC}"
    echo -e "  ${YELLOW}gbx:test:scala --suites '...SpatialRefOpsTest,...GTiff_DataSourceTest'${NC}"
    echo -e "  ${YELLOW}gbx:test:scala --log scala-tests.log --verbose${NC}"
    echo -e "  ${YELLOW}gbx:test:scala --spark-version 4.3.0 --suites 'com.databricks.labs.gbx.gridx.*'${NC}"
    echo -e "  ${YELLOW}gbx:test:scala --spark-version 4.2.0 --compile-only${NC}"
    echo -e "  ${YELLOW}# Run RequiresProjIsolation tests in isolation:${NC}"
    echo -e "  ${YELLOW}gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.RST_TransformCrsGridSpec' --tags-to-exclude com.databricks.labs.gbx.bench.NoOpExcludeSentinel${NC}"
    echo ""
}

# The container has no LANG/LC_ALL, so the JVM's sun.jnu.encoding is ASCII and tests that create
# non-ASCII file names (NodeFilePathUtilTest) cancel on `assume`. Both runs below pass
# LC_ALL=C.UTF-8 to the container instead of weakening the test. The caller's GBX_NATIVE_ROOT (a native payload)
# reaches the container too, so the test JVM loads that payload's GDAL (GDALManager reads it), as the pytest runs do.

# Parse arguments
SUITE_PATTERN=""
LOG_PATH=""
VERBOSE=""
BY_PACKAGE=false
TAGS_TO_EXCLUDE=""
SPARK_VERSION=""
COMPILE_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --by-package)
            BY_PACKAGE=true
            shift
            ;;
        --suite)
            SUITE_PATTERN="$2"
            shift 2
            ;;
        --suites)
            SUITE_PATTERN="$2"
            shift 2
            ;;
        --tags-to-exclude)
            TAGS_TO_EXCLUDE="$2"
            shift 2
            ;;
        --spark-version)
            SPARK_VERSION="$2"
            shift 2
            ;;
        --compile-only)
            COMPILE_ONLY=true
            shift
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --verbose)
            VERBOSE="-X"
            shift
            ;;
        --no-wait)
            GBX_LOCK_NO_WAIT=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"

# --spark-version: the Maven properties that make the build follow that Spark (spark.version is read
# from the command line over the profile's 4.0.0). Scala and jackson are the values in the Spark
# release's own parent POM (spark-parent_2.13). The build output goes to target/spark-<v>/, so the
# default target/ and its JARs stay as they are.
SPARK_MVN_ARGS=""
if [ -n "$SPARK_VERSION" ]; then
    case "$SPARK_VERSION" in
        4.0.*) SPARK_SCALA="2.13.16"; SPARK_JACKSON="2.18.2" ;;
        4.1.*) SPARK_SCALA="2.13.17"; SPARK_JACKSON="2.20.0" ;;
        4.2.*|4.3.*) SPARK_SCALA="2.13.18"; SPARK_JACKSON="2.21.2" ;;
        *)
            echo -e "${RED}❌ --spark-version $SPARK_VERSION: no mapping (known: 4.0.x, 4.1.x, 4.2.x, 4.3.x). Add its scala and jackson versions from spark-parent_2.13 to this script.${NC}"
            exit 1
            ;;
    esac
    SPARK_MVN_ARGS="-Dspark.version=$SPARK_VERSION -Dscala.version=$SPARK_SCALA -Djackson.version=$SPARK_JACKSON"
    if [ "$SPARK_VERSION" != "4.0.0" ]; then
        SPARK_MVN_ARGS="$SPARK_MVN_ARGS -Dgbx.build.dir=/root/geobrix/target/spark-$SPARK_VERSION"
    fi
    echo -e "${CYAN}Spark $SPARK_VERSION (scala $SPARK_SCALA, jackson $SPARK_JACKSON)${NC}"
fi
MVN_GOAL="test"
[ "$COMPILE_ONLY" = true ] && MVN_GOAL="test-compile"

show_banner "🧪 GeoBrix: Scala Tests (Non-Docs)"
check_docker
setup_log_file "$LOG_PATH"
gbx_acquire_test_lock
gbx_native_env_args
SCALA_ENV_ARGS=(-e LC_ALL=C.UTF-8 ${GBX_NATIVE_ENV_ARGS[@]+"${GBX_NATIVE_ENV_ARGS[@]}"})

# Package list for --by-package (same as CI matrix)
GBX_PACKAGES="rasterx gridx vectorx expressions util"

if [ "$BY_PACKAGE" = true ]; then
    echo -e "${CYAN}🎯 Running tests by package (sequence): $GBX_PACKAGES${NC}"
    echo ""
    EXIT_CODE=0
    for pkg in $GBX_PACKAGES; do
        show_separator
        echo -e "${CYAN}Package: ${GREEN}$pkg${NC}"
        show_separator
        SUITES="com.databricks.labs.gbx.${pkg}.*"
        MVN_CMD="$DOCKER_MAVEN_ENV && cd /root/geobrix && mvn $MVN_GOAL -PskipScoverage -DskipTests=false $SPARK_MVN_ARGS -Dsuites='$SUITES' $VERBOSE"
        if docker exec "${SCALA_ENV_ARGS[@]}" "$GBX_DEV_CONTAINER" $GBX_LOCK_RUN /bin/bash -c "$MVN_CMD"; then
            echo -e "${GREEN}✅ $pkg passed${NC}"
        else
            echo -e "${RED}❌ $pkg failed${NC}"
            EXIT_CODE=1
        fi
        echo ""
    done
    show_separator
    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}✅ All packages passed!${NC}"
    else
        echo -e "${RED}❌ One or more packages failed${NC}"
    fi
    show_separator
else
    # Build Maven command (DOCKER_MAVEN_ENV sets MAVEN_OPTS for faster Maven in Docker)
    MVN_CMD="$DOCKER_MAVEN_ENV && cd /root/geobrix && mvn $MVN_GOAL -PskipScoverage -DskipTests=false $SPARK_MVN_ARGS"

    if [ -n "$SUITE_PATTERN" ]; then
        echo -e "${CYAN}🎯 Running suite: ${YELLOW}$SUITE_PATTERN${NC}"
        MVN_CMD="$MVN_CMD -Dsuites='$SUITE_PATTERN'"
    else
        echo -e "${CYAN}🎯 Running all Scala unit tests (src/test/scala; excludes docs)${NC}"
        MVN_CMD="$MVN_CMD -Dsuites='com.databricks.labs.gbx.*'"
    fi

    if [ -n "$TAGS_TO_EXCLUDE" ]; then
        MVN_CMD="$MVN_CMD -DtagsToExclude='$TAGS_TO_EXCLUDE'"
    fi

    if [ -n "$VERBOSE" ]; then
        MVN_CMD="$MVN_CMD $VERBOSE"
    fi

    echo ""
    show_separator
    echo -e "${CYAN}Running tests...${NC}"
    show_separator
    echo ""

    docker exec "${SCALA_ENV_ARGS[@]}" "$GBX_DEV_CONTAINER" $GBX_LOCK_RUN /bin/bash -c "$MVN_CMD"
    EXIT_CODE=$?

    echo ""
    show_separator
    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}✅ Scala tests passed!${NC}"
    else
        echo -e "${RED}❌ Scala tests failed (exit code: $EXIT_CODE)${NC}"
    fi
    show_separator
fi

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
