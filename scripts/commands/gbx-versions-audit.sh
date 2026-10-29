#!/bin/bash
# gbx:versions:audit - Inventory pinned Python tooling versions across CI, Docker, init scripts, and venv setup.
# Use case 1: audit drift between locations.
# Use case 2: when bumping to a new DBR LTS, run this first to see every place that needs an update.
# Use case 3 (--native, --host): check the native stack (GDAL, PROJ, GEOS, Rust, the bindings) against
# scripts/native/pins.conf: the repository's consumers (--native) and the host venv and the dev container (--host).

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "Versions audit (CI / Docker / init / venv)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:versions:audit${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--package <name>${NC} Show only one package (e.g. pip, numpy, pyspark)."
    echo -e "  ${GREEN}--native${NC}         Audit every consumer of the native pins (scripts/native/pins.conf, rust-toolchain.toml,"
    echo -e "                      legacy consumers) instead of the package inventory."
    echo -e "  ${GREEN}--host${NC}           Check the host venv AND this checkout's dev container against the pins: Python patch"
    echo -e "                      version, the bound Python packages, and GDAL/PROJ/GEOS as rasterio/pyproj/shapely report them."
    echo -e "  ${GREEN}--check${NC}          With --native or --host: exit 1 on drift (without it the report is printed and the exit is 0)."
    echo -e "  ${GREEN}--log <path>${NC}     Write output to log file."
    echo -e "  ${GREEN}--help${NC}           Show this help."
    echo ""
    echo -e "${CYAN}When to use:${NC}"
    echo -e "  - Bumping to a new DBR LTS: run this, then ask Claude to apply the new pins"
    echo -e "    given the release-notes URL (e.g. docs.databricks.com/.../runtime/18.3lts)."
    echo -e "  - Suspect drift between Dockerfile / CI / init scripts: run to spot it."
    echo ""
}

PACKAGE_FILTER=""
LOG_PATH=""
NATIVE=false
HOST=false
CHECK=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --package)
            PACKAGE_FILTER="$2"; shift 2;;
        --native)
            NATIVE=true; shift;;
        --host)
            HOST=true; shift;;
        --check)
            CHECK=true; shift;;
        --log)
            LOG_PATH=$(resolve_log_path "$2"); shift 2;;
        --help|-h)
            show_help; exit 0;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"; show_help; exit 1;;
    esac
done

cd "$PROJECT_ROOT"
show_banner "Versions audit"
setup_log_file "$LOG_PATH"

# --native / --host: the native pins are checked, not inventoried (scripts/native/pins.py does the work).
if [ "$NATIVE" = true ] || [ "$HOST" = true ]; then
    CHECK_ARG=()
    [ "$CHECK" = true ] && CHECK_ARG=(--check)
    RC=0
    if [ "$NATIVE" = true ]; then
        python3 scripts/native/pins.py audit ${CHECK_ARG[@]+"${CHECK_ARG[@]}"} || RC=1
    fi
    if [ "$HOST" = true ]; then
        # The same collector gbx:venv:check uses, run by the host venv's interpreter and inside the container.
        REPORT="$PROJECT_ROOT/scripts/ci/venv_env_report.py"
        VENV_DIR="${GBX_PYRX_VENV:-$PROJECT_ROOT/.venv-pyrx}"
        VENV_PY="$VENV_DIR/bin/python"
        if [ ! -x "$VENV_PY" ]; then
            echo -e "${RED}ERROR: no host venv at $VENV_DIR. Run: bash scripts/commands/gbx-venv-sync.sh${NC}" >&2
            exit 1
        fi
        check_docker
        WORK="$(mktemp -d "${TMPDIR:-/tmp}/gbx-versions-audit.XXXXXX")"
        "$VENV_PY" - collect < "$REPORT" > "$WORK/host.json" \
            || { rm -rf "$WORK"; echo -e "${RED}ERROR: could not read the host venv${NC}" >&2; exit 1; }
        docker exec -i "$GBX_DEV_CONTAINER" python3 - collect < "$REPORT" > "$WORK/container.json" \
            || { rm -rf "$WORK"; echo -e "${RED}ERROR: could not read the container's environment${NC}" >&2; exit 1; }
        python3 scripts/native/pins.py env-check ${CHECK_ARG[@]+"${CHECK_ARG[@]}"} "host=$WORK/host.json" "container=$WORK/container.json" || RC=1
        rm -rf "$WORK"
    fi
    if [ -n "$LOG_PATH" ]; then
        echo -e "${CYAN}Log saved to: ${YELLOW}$LOG_PATH${NC}"
    fi
    exit $RC
fi

# Files where pinned versions live (the audit's scope). A new file that pins a version
# must be added here, or its pins are never audited.
FILES=(
    ".github/actions/scala_build/action.yml"
    ".github/actions/python_build/action.yml"
    ".github/workflows/build_main.yml"
    ".github/workflows/build_python.yml"
    ".github/workflows/build_scala.yml"
    ".github/workflows/build_scala_by_package.yml"
    ".github/workflows/codecov-scala-parallel.yml"
    "scripts/docker/Dockerfile"
    "scripts/docker/dcs/Dockerfile"
    "scripts/ci-local/Dockerfile.gha-runner"
    ".mvn/wrapper/maven-wrapper.properties"
    "scripts/geobrix-gdal-init.sh"
    "scripts/build-gdal-artifacts.sh"
    "pom.xml"
    "scripts/native/pins.conf"
    "rust-toolchain.toml"
    "scripts/commands/gbx-lint-python.sh"
    "python/geobrix/pyproject.toml"
    "examples/tests/requirements.txt"
)

# Packages we pin or track. Format: package_name:::grep_pattern:::source-of-truth
# grep_pattern is an extended regex (with alternation `|`) — that's why we use `:::`
# as the field delimiter rather than the conventional pipe.
# When DBR LTS bumps, every package marked "DBR LTS" should be re-checked against the new release notes.
PACKAGES=(
    # Core build/runtime — all DBR-aligned
    "pip:::(^|[^a-zA-Z0-9_-])pip(==|_VERSION=):::DBR LTS"
    "setuptools:::setuptools(==|_VERSION=):::DBR LTS"
    "wheel:::wheel(==|_VERSION=):::DBR LTS"
    "cython:::cython(==|_VERSION=):::DBR LTS"
    "numpy:::numpy(==|_VERSION=|>=|: \[ ?[0-9]):::DBR LTS"
    "pandas:::pandas(==|_VERSION=):::DBR LTS"
    "pyspark:::pyspark(==|_VERSION=|>=):::tracks Spark version in DBR"
    "spark:::SPARK_VERSION=|spark: ?\[:::DBR (Apache Spark)"
    "requests:::requests(==|_VERSION=):::DBR LTS"
    "python:::python: ?\[:::DBR Python"
    # Test tooling — pytest is intentionally NOT DBR (we don't run in DBR); pytest-cov is current stable
    "pytest:::(^|[^a-z-])pytest(==|_VERSION=|>=)[^-]:::CI matrix (intentionally NOT DBR's pytest)"
    "pytest-cov:::pytest-cov(==|_VERSION=):::current PyPI stable"
    "build:::build(==|_VERSION=):::current PyPI stable (not in DBR)"
    # Native — system + apt
    "gdal:::(gdal-config|gdal\[numpy\]|GDAL\[numpy\]|gdal: ?\[|GDAL_VERSION=|org\.gdal):::scripts/native/pins.conf (legacy consumers listed in scripts/native/legacy-consumers.txt)"
    "proj:::PROJ_VERSION=:::scripts/native/pins.conf"
    "geos:::GEOS_VERSION=:::scripts/native/pins.conf"
    # Jupyter / notebook stack — DBR-aligned (Dockerfile dev container)
    "ipython:::ipython(==|_VERSION=):::DBR LTS"
    "ipykernel:::ipykernel(==|_VERSION=):::DBR LTS"
    "ipywidgets:::ipywidgets(==|_VERSION=):::DBR LTS"
    "jupyterlab:::jupyterlab(==|_VERSION=):::DBR LTS"
    "jupyter_server:::jupyter_server(==|_VERSION=):::DBR LTS"
    "jupyter_core:::jupyter_core(==|_VERSION=):::DBR LTS"
    "jupyter_client:::jupyter_client(==|_VERSION=):::DBR LTS"
    "tornado:::tornado(==|_VERSION=):::DBR LTS"
    "nbformat:::nbformat(==|_VERSION=|>=):::DBR LTS"
    "nbconvert:::nbconvert(==|_VERSION=|>=):::DBR LTS"
    "pyzmq:::pyzmq(==|_VERSION=):::INTENTIONAL DBR DIVERGENCE — see Dockerfile ZMQ fixes"
    # Build tool — one Maven for CI (wrapper), the dev image, the local-CI image and the pom's enforcer floor
    "maven:::MAVEN_VERSION=|distributionUrl=|<version>\[3\.[0-9.]+,\)</version>:::.mvn/wrapper (CI); scripts/ci/test_maven_wrapper.py keeps them equal"
    # Geospatial — geopandas not in DBR; pinned to be pandas-2.2 compatible
    "geopandas:::geopandas(==|_VERSION=):::current stable, compatible with DBR pandas 2.2"
)

print_package() {
    local name="$1" pattern="$2" source="$3"
    show_separator
    echo -e "${CYAN}=== ${YELLOW}${name}${CYAN} ===${NC}  ${BLUE}(source: ${source})${NC}"
    local found=0
    for f in "${FILES[@]}"; do
        if [ ! -f "$f" ]; then continue; fi
        # -E for extended regex; -n for line numbers; -H for filename
        local matches
        matches=$(grep -EnH "$pattern" "$f" 2>/dev/null)
        if [ -n "$matches" ]; then
            found=1
            # printf (not echo -e) — file lines may end in `\` (Dockerfile continuations)
            # which would otherwise eat the trailing color-reset escape.
            while IFS= read -r line; do
                printf '  %b%s%b\n' "${GREEN}" "${line}" "${NC}"
            done <<< "$matches"
        fi
    done
    if [ "$found" -eq 0 ]; then
        echo -e "  ${YELLOW}(no occurrences — not pinned anywhere)${NC}"
    fi
}

for pkg_entry in "${PACKAGES[@]}"; do
    name="${pkg_entry%%:::*}"
    rest="${pkg_entry#*:::}"
    pattern="${rest%%:::*}"
    source="${rest#*:::}"
    if [ -n "$PACKAGE_FILTER" ] && [ "$PACKAGE_FILTER" != "$name" ]; then continue; fi
    print_package "$name" "$pattern" "$source"
done

show_separator
echo -e "${CYAN}Files audited:${NC}"
for f in "${FILES[@]}"; do
    if [ -f "$f" ]; then
        echo -e "  ${GREEN}✓${NC} $f"
    else
        echo -e "  ${RED}✗${NC} $f ${RED}(missing — update FILES list in this script)${NC}"
    fi
done
show_separator

echo -e "${CYAN}Workflow when bumping to a new DBR LTS:${NC}"
echo -e "  1. Find the DBR release notes URL (e.g. https://docs.databricks.com/aws/en/release-notes/runtime/18.3lts)"
echo -e "  2. Run ${YELLOW}gbx:versions:audit${NC} and capture output"
echo -e "  3. Ask Claude: \"Bump pinned versions to DBR 18.3 LTS from <URL>; here's the audit: <paste>\""
echo -e "  4. Verify nothing drifted — re-run ${YELLOW}gbx:versions:audit${NC} after edits"
echo -e "  5. Commit with subject: ${GREEN}chore(deps): align pins to DBR 18.3 LTS${NC}"
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi
