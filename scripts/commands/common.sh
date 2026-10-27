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

# Copy the newest built assembly JAR from target/ into python/geobrix/lib/ -- the dir the
# heavy Python tests glob (`geobrix-*-jar-with-dependencies.jar`) for spark.jars. This is
# the host-side twin of the pom's maven-resources-plugin `copy-files-on-build` execution
# (package phase), which is how CI stages it; use it when the Maven build already ran but the
# staging dir was emptied. Only the current JAR is kept staged, so a stale one can never be
# picked up instead. Returns 1 (prints nothing) when target/ holds no assembly JAR.
# Usage: stage_jar_from_target "$PROJECT_ROOT"
stage_jar_from_target() {
    local project_root="$1"
    local stage_dir="$project_root/python/geobrix/lib"
    local built
    built=$(ls -t "$project_root"/target/geobrix-*-jar-with-dependencies.jar 2>/dev/null | head -n 1)
    [ -n "$built" ] || return 1
    mkdir -p "$stage_dir"
    rm -f "$stage_dir"/geobrix-*-jar-with-dependencies.jar
    cp "$built" "$stage_dir/"
    echo -e "${GREEN}📦 Staged $(basename "$built") → python/geobrix/lib/${NC}"
}

# Guarantee the heavy Python tests can find a JAR: keep an already-staged one (re-staging
# only when target/ holds a newer build, as Maven's copy would), else stage the built one
# from target/. Never runs Maven. With neither, prints ONE
# `PRECONDITION MISSING` line and returns 1 -- a missing artifact, not a code defect --
# instead of letting every heavy module die at collection with `IndexError` on
# `candidates[-1]`.
# Usage: ensure_staged_jar "$PROJECT_ROOT"
ensure_staged_jar() {
    local project_root="$1"
    local built staged
    built=$(ls -t "$project_root"/target/geobrix-*-jar-with-dependencies.jar 2>/dev/null | head -n 1)
    staged=$(ls -t "$project_root"/python/geobrix/lib/geobrix-*-jar-with-dependencies.jar 2>/dev/null | head -n 1)
    if [ -n "$built" ] && { [ -z "$staged" ] || [ "$built" -nt "$staged" ]; }; then
        stage_jar_from_target "$project_root"
        return $?
    fi
    [ -n "$staged" ] && return 0
    echo -e "${RED}PRECONDITION MISSING: no built JAR in target/ and none staged in python/geobrix/lib/; heavy tests not run. Build it: gbx:docker:exec \"mvn clean package -PskipScoverage -DskipTests\"${NC}"
    return 1
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

# ---------------------------------------------------------------------------------------
# One container test run at a time.
#
# Maven and the Spark test JVMs of the gbx:test:*, gbx:coverage:* and build commands share the
# container's target/ directory and Maven repository, so two overlapping runs corrupt each
# other. Every such command calls gbx_acquire_test_lock once, right after check_docker (and
# setup_log_file), and sends its Maven/pytest work through $GBX_LOCK_RUN. The locks live
# INSIDE geobrix-dev (scripts/docker/gbx-lock.sh), so they cover every caller of the
# container, whichever host shell or agent started it. There are two, with different owners:
#
#   RUN lock   (/tmp/gbx-test.lock)  held for as long as THIS script, or a child of it, lives:
#              the container-side `hold` process blocks reading a FIFO whose write end this
#              shell keeps open, and releases the lock when every holder of that end is gone.
#              It makes whole runs take turns, with no gap between the execs of one run.
#   WORK lock  (/tmp/gbx-work.lock)  held by the WORK: `$GBX_LOCK_RUN <command>` runs the
#              command on a descriptor that Maven, Surefire, pytest and everything they start
#              inherit. Killing this script or its `docker exec` client (kill -9, a harness
#              tree-kill, a timeout) frees the RUN lock but NOT the container's work, which
#              keeps running; it keeps the WORK lock too, so the next run's acquisition waits
#              for it to end (exit 75 under --no-wait), naming the process still holding it.
#
# What this guarantees: gbx commands never overlap, including after a killed run. What it does
# not: Maven or pytest started in the container outside a gbx command (an interactive
# `docker exec geobrix-dev mvn ...`) is not seen; a work process that never ends blocks later
# runs until GBX_TEST_LOCK_TIMEOUT (the message names it, so it can be killed). A locked exec
# must never start another locked exec: it would wait for the lock its own parent holds.
#
#   default      wait for the lock, naming the holder once; give up after
#                $GBX_TEST_LOCK_TIMEOUT seconds (default 3600) with exit 75.
#   --no-wait    (GBX_LOCK_NO_WAIT=true) fail at once with exit 75, naming the holder.
#
# A gbx command run by a parent that already holds the lock (gbx:test:docs runs the three doc
# suites) skips taking it: GBX_TEST_LOCK_HELD_BY carries the parent's pid.
# ---------------------------------------------------------------------------------------
GBX_TEST_LOCK_FILE="${GBX_TEST_LOCK_FILE:-/tmp/gbx-test.lock}"        # RUN lock, a path INSIDE geobrix-dev
GBX_WORK_LOCK_FILE="${GBX_WORK_LOCK_FILE:-/tmp/gbx-work.lock}"        # WORK lock, likewise
GBX_TEST_LOCK_TIMEOUT="${GBX_TEST_LOCK_TIMEOUT:-3600}"
GBX_LOCK_NO_WAIT="${GBX_LOCK_NO_WAIT:-false}"
GBX_CONTAINER_LOCK="/root/geobrix/scripts/docker/gbx-lock.sh"        # the repo, as mounted in the container
# Prefix for a Maven/pytest `docker exec`, left UNQUOTED on purpose (it is several words):
#   docker exec geobrix-dev $GBX_LOCK_RUN /bin/bash -c "$CMD"
GBX_LOCK_RUN="/bin/bash $GBX_CONTAINER_LOCK run"
# Command line of the script that sourced this file (written into the lock info and the log).
GBX_LOCK_CMD="$(basename "${BASH_SOURCE[1]:-$0}") $*"
GBX_LOCK_CMD="${GBX_LOCK_CMD:0:200}"

# The --no-wait line for a command's show_help.
print_lock_help() {
    echo -e "  ${GREEN}--no-wait${NC}              Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it"
}

# Take the container test lock, or exit 75. Call once after check_docker. Prints this run's
# pid and the acquisition, so a duplicate launch is obvious in the log.
gbx_acquire_test_lock() {
    if [ -n "${GBX_TEST_LOCK_HELD_BY:-}" ]; then
        echo -e "${CYAN}🔒 test lock: held by the parent command (pid ${GBX_TEST_LOCK_HELD_BY}); this run is pid $$${NC}"
        return 0
    fi
    local info dir status fifo holder_pid line start="$SECONDS" next_note=30 waiting=false
    info="pid=$$ cmd=${GBX_LOCK_CMD} since=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    dir="$(mktemp -d "${TMPDIR:-/tmp}/gbx-lock.XXXXXX")" || return 1
    status="$dir/status"
    fifo="$dir/hold"
    mkfifo "$fifo" || { rm -rf "$dir"; return 1; }
    : > "$status"
    # fd 7 (read-write, so opening never blocks) is the write end the lock depends on; the
    # holder's stdin is a read-only open (fd 6) of the same FIFO, so it sees EOF only when
    # every holder of fd 7 -- this script and its children -- is gone.
    exec 7<>"$fifo" 6<"$fifo"
    rm -f "$fifo"
    docker exec -i \
        -e GBX_LOCK_FILE="$GBX_TEST_LOCK_FILE" \
        -e GBX_WORK_LOCK_FILE="$GBX_WORK_LOCK_FILE" \
        -e GBX_LOCK_TIMEOUT="$GBX_TEST_LOCK_TIMEOUT" \
        -e GBX_LOCK_NO_WAIT="$GBX_LOCK_NO_WAIT" \
        -e GBX_LOCK_INFO="$info" \
        geobrix-dev /bin/bash "$GBX_CONTAINER_LOCK" hold \
        <&6 >"$status" 2>&1 3>&- 7>&- 6<&- &
    holder_pid=$!
    exec 6<&-
    while :; do
        if grep -q '^ACQUIRED' "$status"; then
            break
        fi
        if line="$(grep -E '^(WORK_)?(BUSY|TIMEOUT) |^ERROR ' "$status" | head -1)" && [ -n "$line" ]; then
            wait "$holder_pid" 2>/dev/null
            rm -rf "$dir"
            exec 7<&-
            case "$line" in
                BUSY*)
                    echo -e "${RED}❌ Another test run holds the container lock: ${line#BUSY }${NC}"
                    echo -e "${YELLOW}   Not waiting (--no-wait). This run: pid $$ ${GBX_LOCK_CMD}${NC}"
                    exit 75 ;;
                WORK_BUSY*)
                    echo -e "${RED}❌ Work from an earlier run is still running in the container: ${line#WORK_BUSY }${NC}"
                    echo -e "${YELLOW}   Not waiting (--no-wait). This run: pid $$ ${GBX_LOCK_CMD}${NC}"
                    exit 75 ;;
                TIMEOUT*)
                    echo -e "${RED}❌ Timed out after ${GBX_TEST_LOCK_TIMEOUT}s waiting for the container lock held by: ${line#TIMEOUT }${NC}"
                    exit 75 ;;
                WORK_TIMEOUT*)
                    echo -e "${RED}❌ Timed out after ${GBX_TEST_LOCK_TIMEOUT}s waiting for earlier work still running in the container: ${line#WORK_TIMEOUT }${NC}"
                    exit 75 ;;
                *)
                    echo -e "${RED}❌ Could not take the container lock: ${line#ERROR }${NC}"
                    exit 70 ;;
            esac
        fi
        if ! kill -0 "$holder_pid" 2>/dev/null; then
            echo -e "${RED}❌ The container lock helper exited without taking the lock:${NC}"
            sed 's/^/   /' "$status"
            rm -rf "$dir"
            exec 7<&-
            exit 70
        fi
        if [ "$waiting" = false ] && line="$(grep -E '^(WORK_)?WAITING ' "$status" | head -1)" && [ -n "$line" ]; then
            waiting=true
            case "$line" in
                WORK_WAITING*)
                    echo -e "${YELLOW}⏳ Work from an earlier run is still running in the container (that run was killed or detached): ${line#WORK_WAITING }${NC}" ;;
                *)
                    echo -e "${YELLOW}⏳ Another test run holds the container lock: ${line#WAITING }${NC}" ;;
            esac
            echo -e "${YELLOW}   Waiting up to ${GBX_TEST_LOCK_TIMEOUT}s (--no-wait fails at once). This run: pid $$ ${GBX_LOCK_CMD}${NC}"
        fi
        if [ "$waiting" = true ] && [ $((SECONDS - start)) -ge "$next_note" ]; then
            echo -e "${YELLOW}   ...still waiting for the container lock ($((SECONDS - start))s)${NC}"
            next_note=$((next_note + 30))
        fi
        sleep 0.2
    done
    rm -rf "$dir"
    export GBX_TEST_LOCK_HELD_BY="$$"
    echo -e "${GREEN}🔒 test lock acquired: pid $$ ${GBX_LOCK_CMD} (waited $((SECONDS - start))s; ${GBX_TEST_LOCK_FILE} in geobrix-dev)${NC}"
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
export RED GREEN YELLOW BLUE CYAN NC DOCKER_MAVEN_ENV GBX_TEST_LOCK_FILE GBX_WORK_LOCK_FILE GBX_TEST_LOCK_TIMEOUT GBX_LOCK_NO_WAIT GBX_LOCK_CMD GBX_CONTAINER_LOCK GBX_LOCK_RUN
export -f check_docker resolve_log_path setup_log_file show_banner show_separator \
          print_report_link open_report generate_timestamp warn_if_jar_stale \
          print_banner print_separator setup_log run_in_pyrx_venv validate_set \
          print_lock_help gbx_acquire_test_lock 2>/dev/null || true
