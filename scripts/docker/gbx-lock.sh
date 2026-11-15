#!/bin/bash
# gbx-lock.sh: the container side of the gbx test lock. Runs INSIDE geobrix-dev
# (the repo is mounted at /root/geobrix); the host side is gbx_acquire_test_lock in
# scripts/commands/common.sh. Read that block for the whole design.
#
#   gbx-lock.sh hold         take the RUN lock, wait out orphaned work, print the status lines
#                            the host reads, then hold the lock until stdin reaches EOF
#   gbx-lock.sh run CMD...   run CMD holding the WORK lock
#
# Two locks, because they have different owners:
#   RUN lock  (/tmp/gbx-test.lock)  owned by the host script: `hold` runs for as long as the
#             script (or a child of it) keeps the pipe it reads open. It makes whole runs
#             take turns, with no gaps between the several execs of one run.
#   WORK lock (/tmp/gbx-work.lock)  owned by the work itself: `run` takes it on a descriptor
#             that the Maven or pytest process itself keeps for as long as it runs (bash
#             children and fork() children hold it too). Killing the host script or its
#             `docker exec` client does not stop that process, and does not free this lock
#             either. It is NOT inherited by children that Python's subprocess (close_fds) or
#             Java's ProcessBuilder start (a Surefire fork, pyspark's JVM); those normally exit
#             with their parent, and the main process is what the next run waits for.
#             `hold` waits for the lock before it reports ACQUIRED.
#
# Status lines on stdout (`hold`): ACQUIRED | WAITING <holder> | BUSY <holder> | TIMEOUT <holder>
# for the run lock, and WORK_WAITING / WORK_BUSY / WORK_TIMEOUT <holder> for the work lock.
# Exit 75 means busy or timed out; 70 means the helper itself failed (no flock, cannot open a
# lock file). The run-lock wait and the work-lock wait share ONE deadline (TIMEOUT seconds from
# the start of `hold`), and a waiting `hold` ends within a second of its stdin reaching EOF, i.e.
# of the host script (and every child of it) being gone, so a killed waiter does not linger.

RUN_LOCK="${GBX_LOCK_FILE:-/tmp/gbx-test.lock}"
WORK_LOCK="${GBX_WORK_LOCK_FILE:-/tmp/gbx-work.lock}"
TIMEOUT="${GBX_LOCK_TIMEOUT:-3600}"
DEADLINE=$((SECONDS + TIMEOUT))   # one deadline for every wait of a `hold`
WATCHER=""                        # pid of the stdin watcher of `hold`

# Stop with exit 70: the helper itself failed. `hold` reports it as a status line; `run` has
# the command's stdout, so it writes to stderr.
die() {
    if [ "${1:-}" = hold ]; then echo "ERROR $2"; else echo "gbx-lock: $2" >&2; fi
    exit 70
}

# 0 = lock taken, 1 = held by someone else. flock failing any other way (127 = not installed,
# which `if ! flock` would misread as "busy") stops the helper.
try_lock() {   # <mode> <fd>
    flock -n "$2"
    local rc=$?
    case $rc in 0 | 1) return $rc ;; esac
    die "$1" "flock failed (exit $rc; 127 means it is not installed in this container)"
}

# Wait for the lock on <fd> until DEADLINE: 0 = taken, 1 = timed out. In `hold`, give up quietly
# once the host side is gone (the stdin watcher has seen EOF): nobody is waiting for the answer.
wait_lock() {   # <mode> <fd>
    local rc
    while :; do
        flock -w 1 "$2"
        rc=$?
        case $rc in
            0) return 0 ;;
            1) ;;
            *) die "$1" "flock failed (exit $rc; 127 means it is not installed in this container)" ;;
        esac
        [ "$SECONDS" -ge "$DEADLINE" ] && return 1
        if [ -n "$WATCHER" ] && ! kill -0 "$WATCHER" 2>/dev/null; then exit 0; fi
    done
}

# The holder of <lock>, as the holder wrote it to <lock>.info (an older helper wrote it into
# the lock file itself). The info is published a moment after the flock succeeds, so retry
# briefly before saying the holder is not named yet.
holder_of() {
    local i
    for i in 1 2 3 4 5; do
        [ -s "$1.info" ] && { tr '\n' ' ' < "$1.info"; return; }
        sleep 0.1
    done
    [ -s "$1" ] && { tr '\n' ' ' < "$1"; return; }
    printf '(holder not named yet) '
}

# Up to three processes that hold <lock> open, as "pid(command line)". This is what a stuck
# lock needs: the work outlives the script that started it.
users_of() {
    local p n=0
    for p in /proc/[0-9]*; do
        [ "${p#/proc/}" = "$$" ] && continue
        if ls -l "$p/fd" 2>/dev/null | grep -qF -- "$1"; then
            printf '%s(%s) ' "${p#/proc/}" "$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null | cut -c1-90)"
            n=$((n + 1))
            [ "$n" -ge 3 ] && break
        fi
    done
}

# Publish the holder atomically: a reader sees the old text or the new, never a half-written one.
publish_info() {   # <lock> <text>
    printf '%s\n' "$2" > "$1.info.$$" && mv -f "$1.info.$$" "$1.info"
}

describe() {   # <lock>: holder, and who holds it open
    printf '%sheld open by: %s' "$(holder_of "$1")" "$(users_of "$1")"
}

# Wait until no work started by an earlier (killed or detached) run is still going.
work_barrier() {
    exec 8>>"$WORK_LOCK" || return 0
    if try_lock hold 8; then
        rm -f "$WORK_LOCK.info"   # nobody holds it, so a holder named there is a dead one
        exec 8>&-
        return 0
    fi
    exec 8>&-   # describe the holder without holding the descriptor ourselves
    local what
    what="$(describe "$WORK_LOCK")"
    if [ "$GBX_LOCK_NO_WAIT" = true ]; then echo "WORK_BUSY $what"; exit 75; fi
    echo "WORK_WAITING $what"
    exec 8>>"$WORK_LOCK"
    wait_lock hold 8 || { exec 8>&-; echo "WORK_TIMEOUT $(describe "$WORK_LOCK")"; exit 75; }
    rm -f "$WORK_LOCK.info"   # held by us now: whatever it named has ended
    exec 8>&-
}

hold() {
    GBX_LOCK_NO_WAIT="${GBX_LOCK_NO_WAIT:-false}"
    # EOF on stdin means the host script and every child of it are gone. A watcher reads it
    # from the start, so a waiter notices at once instead of at TIMEOUT. (`<&5`, because a
    # background command would otherwise get /dev/null for stdin.)
    exec 5<&0
    cat <&5 > /dev/null &
    WATCHER=$!
    trap 'kill "$WATCHER" 2>/dev/null' EXIT
    exec 9>>"$RUN_LOCK" || die hold "cannot open $RUN_LOCK"
    chmod 666 "$RUN_LOCK" 2>/dev/null || true
    if ! try_lock hold 9; then
        if [ "$GBX_LOCK_NO_WAIT" = true ]; then echo "BUSY $(holder_of "$RUN_LOCK")"; exit 75; fi
        echo "WAITING $(holder_of "$RUN_LOCK")"
        wait_lock hold 9 || { echo "TIMEOUT $(holder_of "$RUN_LOCK")"; exit 75; }
    fi
    publish_info "$RUN_LOCK" "$GBX_LOCK_INFO container_pid=$$"
    trap 'kill "$WATCHER" 2>/dev/null; rm -f "$RUN_LOCK.info"' EXIT
    work_barrier
    echo ACQUIRED
    wait "$WATCHER"   # until the host script (and every child of it) has closed the pipe
}

# At exit of `run`: the holder named in <WORK_LOCK>.info is this process, which is ending. Drop the
# name unless something else still holds the lock: a child of the work may have kept descriptor 9
# and outlive this process, and the next waiter should be told who started it.
clear_own_info() {
    exec 9>&-
    exec 8>>"$WORK_LOCK" || return 0
    if flock -n 8; then rm -f "$WORK_LOCK.info"; fi
    exec 8>&-
}

run() {
    [ "$#" -gt 0 ] || { echo "usage: gbx-lock.sh run CMD [ARG...]" >&2; exit 64; }
    exec 9>>"$WORK_LOCK" || die run "cannot open $WORK_LOCK"
    chmod 666 "$WORK_LOCK" 2>/dev/null || true
    if ! try_lock run 9; then
        exec 9>&-
        echo "gbx-lock: waiting for earlier work still running in this container: $(describe "$WORK_LOCK")"
        exec 9>>"$WORK_LOCK"
        wait_lock run 9 || { echo "gbx-lock: timed out waiting for earlier work: $(describe "$WORK_LOCK")"; exit 75; }
    fi
    publish_info "$WORK_LOCK" "pid=$$ cmd=$* since=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    trap clear_own_info EXIT
    # Not `exec`: this process stays as the parent. The command inherits descriptor 9 and
    # holds it while it runs, whoever dies; bash and fork() children hold it too.
    "$@"
}

case "${1:-}" in
    hold) hold ;;
    run) shift; run "$@" ;;
    *) echo "usage: gbx-lock.sh hold | run CMD [ARG...]" >&2; exit 64 ;;
esac
