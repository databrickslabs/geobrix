#!/bin/bash

# ============================================================================
# GeoBrix: Python Databricks Runtime (DBR) Integration Tests
# ============================================================================
# 
# Runs Databricks-specific integration tests that require DBR SQL functions.
# These tests are ISOLATED from regular unit tests and excluded from coverage.
#
# Requirements:
# - Databricks Runtime environment (st_* SQL functions, spatial types)
# - GeoBrix JAR with all dependencies
# - Sample data mounted at /Volumes/
#
# Note: These tests are NOT run in CI/CD pipelines automatically.
# ============================================================================

set -e  # Exit on error

# Source common utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default values
TEST_PATH=""
VERBOSE=false
LOG_FILE=""
MARKERS=""
CLUSTER_ID=""
PROFILE=""
TIMEOUT_MIN=60

# ============================================================================
# Help Message
# ============================================================================

show_help() {
    cat << EOF
${BLUE}╔═══════════════════════════════════════════════════════╗${NC}
${BLUE}║${NC}  ${CYAN}📚 GeoBrix: Python DBR Integration Tests${NC}
${BLUE}╚═══════════════════════════════════════════════════════╝${NC}

${BOLD}USAGE:${NC}
    gbx:test:python-dbr [OPTIONS]

${BOLD}OPTIONS:${NC}
    --path PATH          Run specific test path (file or directory)
    --markers MARKERS    Run tests matching pytest markers
    --verbose, -v        Show verbose output
    --log FILE           Save output to log file
    --cluster-id ID      Run the tests on this running Databricks cluster (a DBR session) instead of the
                         local container. The cluster needs the geobrix wheel and JAR as libraries.
    --profile NAME       Databricks CLI profile for --cluster-id (required with it; never chosen for you)
    --timeout-min N      Time limit of the cluster run in minutes (default: 60)
    --help, -h           Show this help message

${BOLD}EXAMPLES:${NC}
    ${CYAN}# Run all DBR tests${NC}
    gbx:test:python-dbr

    ${CYAN}# Run specific test file${NC}
    gbx:test:python-dbr --path readers/test_dbr_examples.py

    ${CYAN}# Run tests with specific marker${NC}
    gbx:test:python-dbr --markers databricks

    ${CYAN}# Save output to log${NC}
    gbx:test:python-dbr --log dbr-tests.log

    ${CYAN}# Run the docs-test SQL on a DBR cluster${NC}
    gbx:test:python-dbr --cluster-id 0123-456789-abcdefgh --profile oauth-fe --path readers/

${BOLD}IMPORTANT:${NC}
    ${YELLOW}⚠️  These tests require Databricks Runtime environment${NC}
    ${YELLOW}⚠️  They are EXCLUDED from regular unit test runs${NC}
    ${YELLOW}⚠️  They are EXCLUDED from coverage reporting${NC}
    ${YELLOW}⚠️  They are EXCLUDED from CI/CD pipelines${NC}

${BOLD}MARKERS:${NC}
    databricks           Tests requiring DBR SQL functions
    spatial_sql          Tests using DBR spatial SQL functions
    integration          Full integration tests

${BOLD}LOCATION:${NC}
    Tests: docs/tests-dbr/python/
    Config: docs/tests-dbr/pytest.ini

EOF
}

# ============================================================================
# Parse Arguments
# ============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --path)
            TEST_PATH="$2"
            shift 2
            ;;
        --markers)
            MARKERS="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --log)
            LOG_FILE="$2"
            shift 2
            ;;
        --cluster-id)
            CLUSTER_ID="$2"
            shift 2
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --timeout-min)
            TIMEOUT_MIN="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# ============================================================================
# Main Execution
# ============================================================================

show_banner "GeoBrix: Python DBR Integration Tests"

# ----------------------------------------------------------------------------
# On a Databricks cluster: ship the tests in a generated notebook and run it with the notebook runner
# ----------------------------------------------------------------------------
if [ -n "$CLUSTER_ID" ]; then
    if [ -z "$PROFILE" ]; then
        echo -e "${RED}❌ --cluster-id needs --profile NAME (a profile is never chosen for you)${NC}"
        exit 1
    fi
    GEN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/gbx-dbr-tests.XXXXXX")"
    NOTEBOOK="$GEN_DIR/gbx_dbr_docs_tests.ipynb"
    python3 - "$PROJECT_ROOT/docs/tests-dbr" "$NOTEBOOK" "$TEST_PATH" "$MARKERS" "$VERBOSE" <<'PY'
import base64, io, json, sys, zipfile
from pathlib import Path

root, out, test_path, markers, verbose = Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.write(root / "pytest.ini", "tests-dbr/pytest.ini")
    for p in sorted((root / "python").rglob("*")):
        if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc":
            zf.write(p, "tests-dbr/" + p.relative_to(root).as_posix())
blob = base64.b64encode(buf.getvalue()).decode()
args = ["python/" + test_path if test_path else "python/"]
if markers:
    args += ["-m", markers]
if verbose == "true":
    args.append("-vv")
args.append("--tb=short")
src = [
    "import base64, io, os, sys, zipfile\n",
    f"BLOB = {blob!r}\n",
    "ROOT = '/tmp/gbx_dbr_tests'\n",
    "zipfile.ZipFile(io.BytesIO(base64.b64decode(BLOB))).extractall(ROOT)\n",
    "import subprocess\n",
    "subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', 'pytest'], check=True)\n",
    "import importlib\n",
    "importlib.invalidate_caches()\n",
    "import pytest\n",
    "# pytest runs in this process: the spark fixture's getOrCreate then returns the notebook's own session\n",
    "os.environ['GBX_DBR_CLUSTER'] = '1'\n",
    "os.chdir(ROOT + '/tests-dbr')\n",
    f"rc = pytest.main({args!r})\n",
    "assert rc == 0, f'pytest exited {int(rc)}'\n",
]
nb = {
    "cells": [{"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": src}],
    "metadata": {"language_info": {"name": "python"}},
    "nbformat": 4,
    "nbformat_minor": 5,
}
Path(out).write_text(json.dumps(nb, indent=1))
PY
    RUN_ARGS=(--existing-cluster-id "$CLUSTER_ID" --notebook "$NOTEBOOK" --profile "$PROFILE" --timeout-min "$TIMEOUT_MIN")
    if [ -n "$LOG_FILE" ]; then
        RUN_ARGS+=(--log "$LOG_FILE")
    fi
    echo -e "${CYAN}☁️  Running the DBR tests on cluster ${CLUSTER_ID} (profile ${PROFILE})${NC}"
    EXIT_CODE=0
    bash "$SCRIPT_DIR/gbx-test-notebooks-serverless.sh" "${RUN_ARGS[@]}" || EXIT_CODE=$?
    rm -rf "$GEN_DIR"
    exit $EXIT_CODE
fi

# Validate environment
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed or not in PATH${NC}"
    exit 1
fi

gbx_require_local_env
if ! docker ps --format '{{.Names}}' | grep -Fxq -- "$GBX_DEV_CONTAINER"; then
    echo -e "${RED}❌ $GBX_DEV_CONTAINER container is not running${NC}"
    echo -e "${YELLOW}ℹ️  Start it with: gbx:docker:start${NC}"
    exit 1
fi

# Build pytest command
PYTEST_CMD="python3 -m pytest /root/geobrix/docs/tests-dbr/python/"

# Add test path if specified
if [ -n "$TEST_PATH" ]; then
    PYTEST_CMD="${PYTEST_CMD}${TEST_PATH}"
    echo -e "${CYAN}📂 Running tests from: ${TEST_PATH}${NC}"
fi

# Add markers if specified
if [ -n "$MARKERS" ]; then
    PYTEST_CMD="${PYTEST_CMD} -m ${MARKERS}"
    echo -e "${CYAN}🏷️  Using markers: ${MARKERS}${NC}"
fi

# Add verbose flag
if [ "$VERBOSE" = true ]; then
    PYTEST_CMD="${PYTEST_CMD} -vv"
fi

# Add standard options
PYTEST_CMD="${PYTEST_CMD} --tb=short"

echo ""
echo -e "${CYAN}Running DBR Integration Tests${NC}"

# Execute tests
if [ -n "$LOG_FILE" ]; then
    LOG_PATH=$(resolve_log_path "$LOG_FILE")
    mkdir -p "$(dirname "$LOG_PATH")"
    : > "$LOG_PATH"
    echo -e "${CYAN}📝 Logging to: ${LOG_PATH}${NC}"
    docker exec "$GBX_DEV_CONTAINER" bash -c "$PYTEST_CMD" 2>&1 | tee "$LOG_PATH"
    EXIT_CODE=${PIPESTATUS[0]}
else
    docker exec "$GBX_DEV_CONTAINER" bash -c "$PYTEST_CMD"
    EXIT_CODE=$?
fi

# Report results
show_separator

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ DBR integration tests passed${NC}"
else
    echo -e "${RED}❌ DBR integration tests failed (exit code: $EXIT_CODE)${NC}"
fi

show_separator

exit $EXIT_CODE
