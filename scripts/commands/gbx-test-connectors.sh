#!/bin/bash
# gbx:test:connectors - Run the Lakeflow Connect connector tests (connectors/lakeflow/)
# in an isolated host venv (.venv-connectors/).
#
# The Lakeflow community-connector framework needs PySpark >= 4.1
# (pyspark.sql.streaming.datasource), newer than the 4.0 used by the other
# venvs and the dev container, so connectors get their own venv. Dependencies
# (pyspark, geobrix[light_env6] editable, the pinned framework wheel, pytest)
# are re-synced only when their inputs change; the connector itself is
# reinstalled as a regular wheel on every run (see below).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${SCRIPT_DIR}/common.sh"

INDEX_URL="https://pypi-proxy.dev.databricks.com/simple"
LFCC_COMMIT="70d16606fb2a7c32e9965af0c9c9452a0172008e"
LFCC_CACHE_DIR="${PROJECT_ROOT}/.cache/lfcc-wheels"
LFCC_WHEEL="${LFCC_CACHE_DIR}/lakeflow_community_connectors-0.1.0-py3-none-any.whl"
VENV_PATH="${PROJECT_ROOT}/.venv-connectors"
VENV_PYTHON="${VENV_PATH}/bin/python"
CONNECTORS_DIR="${PROJECT_ROOT}/connectors/lakeflow"

TEST_PATH=""
LOG_PATH="connectors-tests.log"
RESYNC=false

show_help() {
    show_banner "GeoBrix: Lakeflow connector tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:connectors${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--path <path>${NC}   Test path relative to connectors/lakeflow/ (default: sources/geobrix_vector/tests)"
    echo -e "  ${GREEN}--resync${NC}        Force re-install of the venv dependencies"
    echo -e "  ${GREEN}--log <path>${NC}    Log file (default: test-logs/connectors-tests.log)"
    echo -e "  ${GREEN}--help${NC}          Show this help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-connectors.sh${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-test-connectors.sh --path sources/geobrix_vector/tests/test_connector.py --log conn.log${NC}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) show_help; exit 0 ;;
        --log) LOG_PATH="$2"; shift 2 ;;
        --path) TEST_PATH="$2"; shift 2 ;;
        --resync) RESYNC=true; shift ;;
        *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
    esac
done

LOG_FILE=$(resolve_log_path "${LOG_PATH}")
setup_log_file "${LOG_FILE}"

command -v python3.12 >/dev/null || { echo "Error: python3.12 not found" >&2; exit 1; }

if [ ! -x "${VENV_PYTHON}" ]; then
    echo "Creating ${VENV_PATH} (Python 3.12)..."
    python3.12 -m venv "${VENV_PATH}"
    RESYNC=true
fi
PIP=("${VENV_PYTHON}" -m pip -q --disable-pip-version-check)

if [ ! -f "${LFCC_WHEEL}" ]; then
    echo "Building lakeflow-community-connectors wheel (commit ${LFCC_COMMIT:0:8})..."
    mkdir -p "${LFCC_CACHE_DIR}"
    "${PIP[@]}" wheel --no-deps --index-url "${INDEX_URL}" -w "${LFCC_CACHE_DIR}" \
        "lakeflow-community-connectors @ git+https://github.com/databrickslabs/lakeflow-community-connectors@${LFCC_COMMIT}"
    RESYNC=true
fi

# Re-sync dependencies only when their inputs change.
STAMP_FILE="${VENV_PATH}/.gbx-deps-stamp"
STAMP="$(cat "${PROJECT_ROOT}/python/geobrix/pyproject.toml" "${LFCC_WHEEL}" | shasum | cut -d' ' -f1)"
if [ "${RESYNC}" = true ] || [ "$(cat "${STAMP_FILE}" 2>/dev/null)" != "${STAMP}" ]; then
    echo "Syncing venv dependencies (pyspark>=4.1, geobrix[light_env6], framework, pytest)..."
    "${PIP[@]}" install --index-url "${INDEX_URL}" "pyspark>=4.1" pytest
    "${PIP[@]}" install --index-url "${INDEX_URL}" -e "${PROJECT_ROOT}/python/geobrix[light_env6]"
    "${PIP[@]}" install --index-url "${INDEX_URL}" --force-reinstall --no-deps "${LFCC_WHEEL}"
    echo "${STAMP}" > "${STAMP_FILE}"
fi

# The connector is installed as a REGULAR (non-editable) wheel on every run. The
# framework wheel ships no databricks.labs.community_connector.sources package, so our
# module must land in site-packages/.../community_connector/sources/geobrix_vector/
# (an implicit namespace subpackage) -- the same layout a pipeline gets from the built
# wheel. An editable finder maps only the leaf module and cannot serve it.
echo "Installing connector package (wheel layout)..."
"${PIP[@]}" install --index-url "${INDEX_URL}" --force-reinstall --no-deps "${CONNECTORS_DIR}"
"${VENV_PYTHON}" -c "import databricks.labs.gbx, databricks.labs.community_connector.sources.geobrix_vector as m; print('connector import OK:', m.__file__)"

TEST_TARGET="${CONNECTORS_DIR}/${TEST_PATH:-sources/geobrix_vector/tests}"
echo ""
echo "Running pytest on ${TEST_TARGET}..."
set +e
"${VENV_PYTHON}" -m pytest -v -p no:cacheprovider "${TEST_TARGET}"
PYTEST_EXIT=$?
set -e

echo ""
if [ ${PYTEST_EXIT} -eq 0 ]; then
    echo "✓ All connector tests passed"
else
    echo "✗ Connector tests failed (exit code: ${PYTEST_EXIT})"
fi
exit ${PYTEST_EXIT}
