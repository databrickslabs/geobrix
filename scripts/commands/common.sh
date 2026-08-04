#!/bin/bash
# Common helper functions for GeoBrix Cursor commands

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Maven env for Docker runs: unset conflicting opts, set Jupyter dirs, and tune Maven/JVM for coverage
# MAVEN_OPTS speeds up builds and scoverage (G1GC + 4G heap when running in geobrix-dev)
export DOCKER_MAVEN_ENV="unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && export MAVEN_OPTS=\"-Xmx4G -XX:+UseG1GC\""

check_docker() {
    if ! docker ps &> /dev/null; then
        echo -e "${RED}❌ Error: Docker is not running${NC}"
        echo "Start Docker and try again."
        exit 1
    fi
    
    if ! docker ps -a --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
        echo -e "${RED}❌ Error: geobrix-dev container not found${NC}"
        echo "Start the development container first:"
        echo -e "  ${YELLOW}./scripts/docker/start_docker.sh${NC}"
        exit 1
    fi
    
    if ! docker ps --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
        echo -e "${YELLOW}⚠️  Container is not running. Starting...${NC}"
        docker start geobrix-dev
        sleep 2
    fi
}

resolve_log_path() {
    local log_arg="$1"
    
    if [ -z "$log_arg" ]; then
        echo ""
        return
    fi
    
    # Check if absolute path (starts with /)
    if [[ "$log_arg" == /* ]]; then
        echo "$log_arg"
        return
    fi
    
    # Check if it's just a filename (no directory separator)
    if [[ "$log_arg" != */* ]]; then
        echo "test-logs/$log_arg"
        return
    fi
    
    # It's a relative path - prepend test-logs/
    echo "test-logs/$log_arg"
}

# Central logging: truncate log on each run so every command gets a fresh file.
# Commands that use --log should call this (or setup_log); the only exception is
# scripts that tee a subprocess only—those must truncate explicitly (: > "$LOG_PATH").
#
# Tees all subsequent script output to BOTH the terminal and the log file, reliably under
# `bash` and `sh` alike. The previous implementation used bash-only process substitution
# (`exec > >(tee ...)`) which (a) is a parse error under POSIX sh, so it fell back to a
# file-only redirect that left the terminal silent, and (b) even under bash races the shell
# exit — bash does not wait for the tee in `>(...)`, so the last lines could be truncated.
#
# Mechanism here: a private FIFO drained by a backgrounded `tee`, plus an EXIT trap that
# closes the write end (so tee sees EOF and flushes) and waits for tee before the script
# exits. No process substitution → identical behavior in both shells, no lost tail output.
# Uses `printf '%b'` rather than `echo -e` (which prints a literal "-e" under /bin/sh).
setup_log_file() {
    local log_path="$1"
    [ -n "$log_path" ] || return 0

    mkdir -p "$(dirname "$log_path")"
    : > "$log_path"
    printf '%b\n' "${CYAN}📝 Logging to: ${YELLOW}${log_path}${NC}"

    # Private FIFO. If FIFOs are unavailable, degrade to file-only logging rather than fail.
    local fifo
    fifo="$(mktemp -u "${TMPDIR:-/tmp}/gbx-log.XXXXXX")" || { exec >>"$log_path" 2>&1; return 0; }
    if ! mkfifo "$fifo" 2>/dev/null; then
        exec >>"$log_path" 2>&1
        return 0
    fi

    exec 3>&1                            # save the real terminal stdout on fd 3
    tee -a "$log_path" <"$fifo" >&3 &    # tee drains the FIFO -> log file + terminal
    GBX_TEE_PID=$!                       # global on purpose: the EXIT trap below reads it
    exec >"$fifo" 2>&1                   # route all stdout+stderr into the FIFO
    rm -f "$fifo"                        # unlink now; open fds keep it alive until closed

    # Flush-safe teardown: capture the real exit code, restore stdout/stderr (closing the
    # FIFO write end so tee reaches EOF), wait for tee to finish, then exit with that code.
    # `exit` inside an EXIT trap does not re-run the trap, so this is not recursive.
    trap 'rc=$?; exec 1>&3 2>&3 3>&-; [ -n "${GBX_TEE_PID:-}" ] && wait "${GBX_TEE_PID}" 2>/dev/null; exit $rc' EXIT
}

show_banner() {
    local title="$1"
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}  ${CYAN}$title${NC}"
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════╝${NC}"
    echo ""
}

show_separator() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Print a clickable file:// URL for the report. Plain URL is clickable in most terminals/IDEs.
# Usage: print_report_link "/absolute/path/to/index.html"
print_report_link() {
    local report_path="$1"
    local abs_path
    abs_path="$(cd "$(dirname "$report_path")" 2>/dev/null && pwd)/$(basename "$report_path")"
    [ -n "$abs_path" ] || abs_path="$report_path"
    # file:// URL (three slashes for absolute path) - most UIs make this clickable
    printf 'file://%s\n' "$abs_path"
}

open_report() {
    local report_path="$1"
    
    if [ ! -f "$report_path" ]; then
        echo -e "${YELLOW}⚠️  Report file not found: $report_path${NC}"
        return 1
    fi
    
    echo -e "${CYAN}📊 Opening report: ${YELLOW}$report_path${NC}"
    
    # Detect OS and open accordingly
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open "$report_path"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        xdg-open "$report_path" &>/dev/null || echo -e "${YELLOW}⚠️  Could not open browser. Open manually: $report_path${NC}"
    else
        echo -e "${YELLOW}⚠️  Unsupported OS. Open manually: $report_path${NC}"
    fi
}

generate_timestamp() {
    date +%Y%m%d-%H%M%S
}

# Warn if the assembly JAR that Spark tests load via spark.jars is stale relative to Scala
# sources. A stale JAR silently tests old behavior and surfaces as UNRESOLVED_ROUTINE for
# functions added since the last `mvn package`. Non-fatal — prints a hint and returns.
# Usage: warn_if_jar_stale "$PROJECT_ROOT"
warn_if_jar_stale() {
    local project_root="$1"
    local rebuild='gbx:docker:exec "mvn clean package -PskipScoverage -DskipTests"'
    local jar
    jar=$(ls -t "$project_root"/target/geobrix-*-jar-with-dependencies.jar 2>/dev/null | head -n 1)
    if [ -z "$jar" ]; then
        echo -e "${YELLOW}⚠️  No assembly JAR in target/ — Spark tests load geobrix-*-jar-with-dependencies.jar via spark.jars.${NC}"
        echo -e "${YELLOW}   Build it first: ${rebuild}${NC}"
        echo ""
        return
    fi
    local newer
    newer=$(find "$project_root/src/main/scala" -name '*.scala' -newer "$jar" -print 2>/dev/null | head -n 1)
    if [ -n "$newer" ]; then
        echo -e "${YELLOW}⚠️  Assembly JAR is older than Scala sources — tests may fail with UNRESOLVED_ROUTINE on newly added functions.${NC}"
        echo -e "${YELLOW}   Stale JAR: $(basename "$jar")${NC}"
        echo -e "${YELLOW}   Rebuild:   ${rebuild}${NC}"
        echo ""
    fi
}

# Print the port(s) of any running gbx:docs:dev / serve server, one per line.
# Detects them via the /tmp/docusaurus-<port>.pid files written by the docs
# commands (only ports whose PID is still alive). Empty output = none running.
detect_docs_dev_servers() {
    local pid_file pid port
    for pid_file in /tmp/docusaurus-*.pid; do
        [ -f "$pid_file" ] || continue
        pid=$(cat "$pid_file" 2>/dev/null)
        port=$(echo "$pid_file" | sed 's/.*docusaurus-\([0-9]*\)\.pid/\1/')
        if [ -n "$pid" ] && ps -p "$pid" > /dev/null 2>&1; then
            echo "$port"
        fi
    done
}

# Warn (non-fatal) before a host-side `npm run build` when a docs server is
# running. A build rewrites the shared <siteDir>/.docusaurus cache the server
# relies on, so its rendered routes go stale/blank until it is restarted. Prefer
# gbx:docs:build, which stops the server for the build and restarts it after.
# Usage: warn_if_docs_server_running
warn_if_docs_server_running() {
    local ports
    ports=$(detect_docs_dev_servers)
    if [ -n "$ports" ]; then
        echo -e "${YELLOW}⚠️  A docs server is running on port(s): $(echo "$ports" | tr '\n' ' ')${NC}"
        echo -e "${YELLOW}   A host-side build rewrites the shared .docusaurus/ cache it uses, so its${NC}"
        echo -e "${YELLOW}   rendered routes will go stale until you restart it (gbx:docs:dev).${NC}"
        echo -e "${YELLOW}   To build safely, use: ${GREEN}gbx:docs:build${YELLOW} (stops → builds → restarts).${NC}"
        echo ""
    fi
}

# Aliases for backward compatibility
print_banner() { show_banner "$@"; }
print_separator() { show_separator "$@"; }
setup_log() { setup_log_file "$@"; }

# Validate a benchmark selection set ("core" or "full"). Exits non-zero on bad value.
# Usage: validate_set "$SET"
validate_set() {
    case "$1" in
        core|full) return 0 ;;
        *) echo "ERROR: --set must be 'core' or 'full' (got '$1')" >&2; return 1 ;;
    esac
}

# Run a command inside the isolated pyrx venv (host, no Docker).
# Usage: run_in_pyrx_venv "<command string>"
# Requires gbx:venv:sync to have been run (venv at $PROJECT_ROOT/.venv-pyrx).
run_in_pyrx_venv() {
    local _cmd="$1"
    local _venv="${PROJECT_ROOT}/.venv-pyrx"
    if [[ ! -x "${_venv}/bin/python" ]]; then
        echo "ERROR: pyrx venv not found at ${_venv}. Run: bash scripts/commands/gbx-venv-sync.sh" >&2
        return 1
    fi
    # shellcheck disable=SC1091
    source "${_venv}/bin/activate"
    bash -c "${_cmd}"
    local _rc=$?
    deactivate || true
    return $_rc
}

# Export helpers + color vars so they survive any subshell that doesn't re-source this file
# (observed on macOS bash 3.2: "show_separator: command not found" mid-script).
export RED GREEN YELLOW BLUE CYAN NC DOCKER_MAVEN_ENV
export -f check_docker resolve_log_path setup_log_file show_banner show_separator \
          print_report_link open_report generate_timestamp warn_if_jar_stale \
          print_banner print_separator setup_log run_in_pyrx_venv validate_set 2>/dev/null || true
