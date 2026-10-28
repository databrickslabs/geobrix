"""Tests for the container test lock in scripts/commands/common.sh.

Two gbx:test:* runs in geobrix-dev at once put two Maven or Spark JVMs on one target/. Every
command that runs Maven or a Spark test JVM there takes one flock (inside the container, on a
fixed path) first: it waits for the holder by default, and with --no-wait it fails at once
with exit 75, naming the holder. The work itself holds a second flock (the WORK lock), so a
run whose host script was killed while Maven kept running in the container still blocks the
next run until that work ends.

The commands run from a scratch copy of scripts/commands/ against a fake `docker` (see
conftest.py's ContainerSandbox): no daemon, no Maven. The lock itself is real.
"""

import fcntl
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from conftest import REPO_ROOT

DEMO = """#!/bin/bash
# A trivial locked command: take the lock, record start/end, hold for --hold seconds.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/common.sh"
HOLD=0
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-wait) GBX_LOCK_NO_WAIT=true; shift ;;
        --hold) HOLD="$2"; shift 2 ;;
        *) exit 2 ;;
    esac
done
check_docker
gbx_acquire_test_lock
echo "start $$" >> "$GBX_DEMO_EVENTS"
i=0
while [ "$i" -lt $((HOLD * 10)) ]; do sleep 0.1; i=$((i + 1)); done
echo "end $$" >> "$GBX_DEMO_EVENTS"
"""


def _events(sandbox):
    return (
        sandbox.events.read_text().split() if sandbox.events.exists() else []
    )  # ["start", pid, "end", pid, ...]


HOLD_CMD = ["/bin/bash", "/root/geobrix/scripts/docker/gbx-lock.sh", "hold"]
LOCK_SCRIPT = "/root/geobrix/scripts/docker/gbx-lock.sh"


def _under_work_lock(command):
    """True for `[env KEY=VALUE ...] /bin/bash gbx-lock.sh run ...`: the work lock's wrapper."""
    if LOCK_SCRIPT not in command:
        return False
    i = command.index(LOCK_SCRIPT)
    return i > 0 and command[i - 1] == "/bin/bash" and command[i + 1 : i + 2] == ["run"]


def _only_the_lock_helper(sandbox):
    return all(c == HOLD_CMD for c in sandbox.execs())


def _work_execs(sandbox):
    """The execs that send Maven or pytest into the container."""
    return [
        c
        for c in sandbox.execs()
        if c != HOLD_CMD and re.search(r"\bmvn\b|\bpytest\b", " ".join(c))
    ]


def _wait_for(predicate, timeout=20.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition not reached in time")


@pytest.fixture
def demo(sandbox):
    sandbox.write_command("lockdemo.sh", DEMO)
    return sandbox


def test_second_run_waits_for_the_first_and_names_the_holder(demo):
    first = demo.popen("lockdemo.sh", "--hold", "2")
    _wait_for(lambda: "start" in _events(demo))
    second = demo.popen("lockdemo.sh")

    out2, _ = second.communicate(timeout=30)
    out1, _ = first.communicate(timeout=30)

    assert first.returncode == 0 and second.returncode == 0
    ev = _events(demo)
    assert [ev[0], ev[2], ev[4], ev[6]] == ["start", "end", "start", "end"], ev
    assert ev[1] == ev[3] and ev[5] == ev[7] and ev[1] != ev[5]  # no interleaving
    # The waiter names the holder (its pid and command) once, then reports the acquisition.
    assert "holds the container lock" in out2
    assert f"pid={ev[1]}" in out2 and "lockdemo.sh --hold 2" in out2
    assert out2.count("holds the container lock") == 1
    # Both runs print their own pid with the acquisition.
    assert re.search(rf"test lock acquired: pid {ev[1]} lockdemo.sh", out1)
    assert re.search(rf"test lock acquired: pid {ev[5]} lockdemo.sh", out2)


def test_no_wait_fails_fast_with_exit_75_naming_the_holder(demo):
    first = demo.popen("lockdemo.sh", "--hold", "3")
    _wait_for(lambda: "start" in _events(demo))
    holder_pid = _events(demo)[1]

    started = time.monotonic()
    res = demo.run("lockdemo.sh", "--no-wait")
    elapsed = time.monotonic() - started
    first.communicate(timeout=30)

    assert res.returncode == 75
    assert elapsed < 2.5  # did not wait for the 3 s holder
    assert f"pid={holder_pid}" in res.output and "lockdemo.sh --hold 3" in res.output
    assert "Not waiting" in res.output
    assert _events(demo).count("start") == 1  # the refused run did no work


def test_wait_gives_up_after_the_timeout_with_exit_75(demo):
    first = demo.popen("lockdemo.sh", "--hold", "4")
    _wait_for(lambda: "start" in _events(demo))

    res = demo.run("lockdemo.sh", GBX_TEST_LOCK_TIMEOUT="1")
    first.communicate(timeout=30)

    assert res.returncode == 75
    assert "Timed out after 1s" in res.output
    assert _events(demo).count("start") == 1


def test_lock_is_released_when_the_holder_is_killed(demo):
    first = demo.popen("lockdemo.sh", "--hold", "60")
    _wait_for(lambda: "start" in _events(demo))
    first.kill()
    first.communicate()

    res = demo.run("lockdemo.sh", timeout=30)  # waits only until the holder sees EOF

    assert res.returncode == 0
    assert _events(demo).count("end") == 1  # the killed run never finished


def test_the_lock_file_is_cleared_when_the_holder_finishes(demo):
    demo.run("lockdemo.sh")

    _wait_for(lambda: demo.lock_file.read_text() == "")


def test_a_child_command_skips_the_lock_its_parent_holds(demo):
    with demo.lock_held_by("pid=1 cmd=parent"):
        res = demo.run("lockdemo.sh", GBX_TEST_LOCK_HELD_BY="1")

    assert res.returncode == 0
    assert "held by the parent command (pid 1)" in res.output
    assert demo.execs() == []  # no holder was started


# ------------------------------------------------------------- every locked command

# command -> arguments (before --no-wait) that reach the lock without failing earlier
LOCKED = {
    "gbx-test-scala.sh": [],
    "gbx-test-python.sh": [],
    "gbx-test-parity.sh": [],
    "gbx-test-python-docs.sh": [],
    "gbx-test-sql-docs.sh": [],
    "gbx-test-scala-docs.sh": [],
    "gbx-test-docs.sh": [],
    "gbx-test-pyrx.sh": ["--docker"],
    "gbx-test-light.sh": [
        "--docker",
        "--path",
        "python/geobrix/test/zarr/test_keys.py",
    ],
    "gbx-test-function-info.sh": [],
    "gbx-coverage-scala.sh": [],
    "gbx-coverage-scala-package.sh": ["rasterx"],
    "gbx-coverage-scala-docs.sh": [],
    "gbx-coverage-python.sh": [],
    "gbx-coverage-python-docs.sh": [],
    "gbx-bench-heavyweight.sh": [],
    "gbx-lint-scalastyle.sh": [],
    "gbx-docker-exec.sh": ["mvn -version"],
    "gbx-data-stage-jar.sh": [],
    "gbx-data-stage-wheel.sh": [],
}

# Locked commands whose --docker run is a call to another command, which takes the lock.
LOCK_TAKEN_BY = {"gbx-test-light.sh": "gbx-test-pyrx.sh"}

# Commands that send Maven or pytest into the container but are not locked, and why.
EXEMPT = {
    "gbx-test-python-dbr.sh": "tests against a remote DBR cluster; no local Maven or Spark JVM",
    "gbx-test-notebooks.sh": "runs fixture/sample notebooks in an isolated venv and temp workdir; "
    "no Maven, no Spark JVM on target/. Its editable install of geobrix into the shared "
    "site-packages happens only when geobrix is missing (pip show first), so a routine run "
    "never rewrites it under a locked pytest",
}


@pytest.mark.parametrize("command", sorted(LOCKED))
def test_a_locked_command_refuses_with_exit_75_while_the_lock_is_held(sandbox, command):
    with sandbox.lock_held_by("pid=4242 cmd=gbx-test-scala.sh --suite x since=now"):
        res = sandbox.run(command, *LOCKED[command], "--no-wait")

    assert res.returncode == 75, res.output
    assert "pid=4242 cmd=gbx-test-scala.sh --suite x" in res.output
    # Nothing but the lock helper reached the container: no Maven, no pytest.
    assert _only_the_lock_helper(sandbox)


@pytest.mark.parametrize("command", sorted(LOCKED))
def test_a_locked_command_documents_no_wait(command):
    commands = REPO_ROOT / "scripts" / "commands"
    doc = (commands / command.replace(".sh", ".md")).read_text()
    assert "--no-wait" in doc and "exit 75" in doc, f"{command}: document the lock"
    script = (commands / LOCK_TAKEN_BY.get(command, command)).read_text()
    assert "--no-wait" in script and "gbx_acquire_test_lock" in script


@pytest.mark.parametrize("command", sorted(LOCKED))
def test_a_locked_command_lists_no_wait_in_its_help(sandbox, command):
    res = sandbox.run(command, "--help")

    assert res.returncode == 0 and "--no-wait" in res.output


# Commands that run Maven through a Python helper (which wraps its own exec).
VIA_PYTHON = {"gbx-data-stage-jar.sh", "gbx-data-stage-wheel.sh"}


def test_every_command_that_runs_maven_or_pytest_in_the_container_is_locked_or_exempt():
    commands = REPO_ROOT / "scripts" / "commands"
    found = set()
    for path in commands.glob("gbx-*.sh"):
        text = path.read_text()
        if "docker exec" in text and re.search(r"\bmvn\b|\bpytest\b", text):
            found.add(path.name)
    unaccounted = sorted(found - set(LOCKED) - set(EXEMPT))
    assert unaccounted == [], (
        "these commands send Maven or pytest into geobrix-dev: take the lock "
        f"(gbx_acquire_test_lock) or list them in EXEMPT with a reason: {unaccounted}"
    )


# ------------------------------------------- Maven hidden in a Python helper is covered too


def _python_helpers():
    """Python files a command may call: examples/tests, scripts/** and docs/scripts."""
    roots = [
        REPO_ROOT / "examples" / "tests",
        REPO_ROOT / "scripts",
        REPO_ROOT / "docs" / "scripts",
    ]
    out = []
    for root in roots:
        for path in root.rglob("*.py"):
            name = path.name
            if name.startswith(("test_", "_fake", "_flock")) or name == "conftest.py":
                continue
            if any(
                part in ("node_modules", "m2", "__pycache__") for part in path.parts
            ):
                continue
            out.append(path)
    return out


DOCKER_EXEC_PY = re.compile(r"docker exec|[\"']docker[\"'],\s*[\"']exec[\"']")
MAVEN_OR_PYTEST = re.compile(r"\bmvn\b|\bpytest\b")


def _maven_helpers():
    """Python helpers that send Maven/pytest into the container, directly or via another one."""
    texts = {p: p.read_text() for p in _python_helpers()}
    direct = {
        p
        for p, t in texts.items()
        if DOCKER_EXEC_PY.search(t) and MAVEN_OR_PYTEST.search(t)
    }
    found = set(direct)
    changed = True
    while changed:
        changed = False
        for p, t in texts.items():
            if p not in found and any(h.name in t for h in found):
                found.add(p)
                changed = True
    return direct, found


def test_python_helpers_that_run_maven_in_the_container_go_through_the_work_lock():
    direct, _ = _maven_helpers()
    assert direct, "expected stage_jar_to_volume.py to be found"
    unwrapped = sorted(p.name for p in direct if "gbx-lock.sh" not in p.read_text())
    assert (
        unwrapped == []
    ), f"these run Maven in geobrix-dev without the work lock: {unwrapped}"


def test_every_command_that_calls_a_maven_helper_is_locked():
    _, all_helpers = _maven_helpers()
    names = {p.name for p in all_helpers}
    commands = REPO_ROOT / "scripts" / "commands"
    callers = {
        path.name
        for path in commands.glob("gbx-*.sh")
        if any(n in path.read_text() for n in names)
    }
    assert callers, "expected gbx-data-stage-jar.sh to call stage_jar_to_volume.py"
    assert sorted(callers - set(LOCKED) - set(EXEMPT)) == []


@pytest.mark.parametrize(
    "command, skip",
    [
        ("gbx-data-stage-jar.sh", "GBX_BUNDLE_SKIP_JAR"),
        ("gbx-data-stage-wheel.sh", "GBX_BUNDLE_SKIP_JAR"),
        ("gbx-data-stage-wheel.sh", "GBX_BUNDLE_SKIP_JAR_UPLOAD"),
    ],
)
def test_staging_without_a_maven_build_does_not_take_the_lock(sandbox, command, skip):
    with sandbox.lock_held_by("pid=4242 cmd=gbx-test-scala.sh since=now"):
        res = sandbox.run(command, "--no-wait", **{skip: "1"})

    assert res.returncode != 75 and "container lock" not in res.output


# ------------------------------- the work holds a lock of its own: it outlives a killed run

WORK = 'echo mvn {mark} >> "$GBX_DEMO_EVENTS"; sleep {secs}; echo mvn {end} >> "$GBX_DEMO_EVENTS"'


def _lines(sandbox):
    return sandbox.events.read_text().splitlines() if sandbox.events.exists() else []


def _can_lock(path: Path) -> bool:
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError:
        return False
    finally:
        os.close(fd)


def _kill_host_side(proc, clients=2):
    """kill -9 a gbx script and its `docker exec` clients; what runs 'in the container' lives on."""
    # `ww`: never cut the command column. Linux ps truncates it to the terminal width
    # (COLUMNS), which on the CI runner hid the `_fake_docker_local.py` marker behind
    # its long interpreter path.
    out = subprocess.run(
        ["ps", "-axww", "-o", "pid=,ppid=,command="], capture_output=True, text=True
    ).stdout
    rows = []
    for line in out.splitlines():
        parts = line.split(None, 2)
        if len(parts) == 3:
            rows.append((int(parts[0]), int(parts[1]), parts[2]))
    tree, changed = {proc.pid}, True
    while changed:
        changed = False
        for pid, ppid, _ in rows:
            if ppid in tree and pid not in tree:
                tree.add(pid)
                changed = True
    victims = [
        pid
        for pid, _, cmd in rows
        if pid in tree and (pid == proc.pid or "_fake_docker_local.py" in cmd)
    ]
    assert len(victims) >= 1 + clients, rows  # the script and its docker clients
    for pid in victims:
        os.kill(pid, signal.SIGKILL)
    proc.wait()
    proc.stdout.close()


def _kill_a_run_that_started_work(sandbox, secs):
    run = sandbox.popen(
        "gbx-docker-exec.sh",
        WORK.format(mark="start-A", end="end-A", secs=secs),
        FAKE_DOCKER_RUN_ALL="1",
    )
    _wait_for(lambda: "mvn start-A" in _lines(sandbox))
    _kill_host_side(run)
    # What Task 43 hit: the host-side lock is gone ...
    _wait_for(lambda: _can_lock(sandbox.lock_file))
    # ... but the work is still running in the container, and still holds its own lock.
    assert not _can_lock(sandbox.work_lock_file)


def test_work_left_running_by_a_killed_run_keeps_the_next_run_waiting(sandbox):
    _kill_a_run_that_started_work(sandbox, secs=5)

    res = sandbox.run(
        "gbx-docker-exec.sh",
        WORK.format(mark="start-B", end="end-B", secs=0),
        FAKE_DOCKER_RUN_ALL="1",
    )

    assert res.returncode == 0, res.output
    assert "still running in the container" in res.output
    assert "start-A" in res.output  # names the work it is waiting for
    assert _lines(sandbox) == ["mvn start-A", "mvn end-A", "mvn start-B", "mvn end-B"]


def test_no_wait_refuses_while_work_of_a_killed_run_is_still_running(sandbox):
    _kill_a_run_that_started_work(sandbox, secs=4)

    res = sandbox.run(
        "gbx-docker-exec.sh",
        WORK.format(mark="start-B", end="end-B", secs=0),
        "--no-wait",
        FAKE_DOCKER_RUN_ALL="1",
    )

    assert res.returncode == 75, res.output
    assert "still running in the container" in res.output and "start-A" in res.output
    assert "start-B" not in "".join(_lines(sandbox))  # the refused run did no work
    _wait_for(lambda: _can_lock(sandbox.work_lock_file))  # and the orphan then finishes


def test_the_work_wrapper_passes_the_exit_code_and_stdin_through(sandbox):
    script = sandbox.root / "scripts" / "docker" / "gbx-lock.sh"
    env = sandbox.env()

    failed = subprocess.run(
        ["bash", str(script), "run", "bash", "-c", "exit 7"],
        env=env,
        capture_output=True,
    )
    piped = subprocess.run(
        ["bash", str(script), "run", "cat"],
        env=env,
        input=b"from stdin",
        capture_output=True,
    )
    bare = subprocess.run(["bash", str(script), "run"], env=env, capture_output=True)

    assert failed.returncode == 7
    assert piped.returncode == 0 and piped.stdout == b"from stdin"
    assert bare.returncode == 64


# ------------------------------------------ the holder is published atomically (and named)


def test_the_holder_is_published_next_to_the_lock_file_not_inside_it(demo):
    first = demo.popen("lockdemo.sh", "--hold", "3")
    _wait_for(lambda: "start" in _events(demo))

    info = Path(str(demo.lock_file) + ".info")
    assert info.read_text().startswith(
        f"pid={first.pid} cmd=lockdemo.sh --hold 3 since="
    )
    assert (
        demo.lock_file.read_text() == ""
    )  # replacing the lock file would break the flock
    assert (
        list(demo.lock_file.parent.glob("gbx-test.lock.info.*")) == []
    )  # no temp file left
    first.communicate(timeout=30)
    _wait_for(lambda: not info.exists())  # cleared when the holder is done


def test_a_holder_written_into_the_lock_file_by_the_first_helper_is_still_named(demo):
    with demo.lock_held_by("pid=77 cmd=old-helper.sh since=then", legacy=True):
        res = demo.run("lockdemo.sh", "--no-wait")

    assert res.returncode == 75 and "pid=77 cmd=old-helper.sh" in res.output


def test_a_helper_that_cannot_take_the_lock_exits_70_not_75(demo):
    broken = str(demo.lock_file.parent / "no-such-dir" / "gbx-test.lock")

    res = demo.run("lockdemo.sh", GBX_TEST_LOCK_FILE=broken)

    assert res.returncode == 70, res.output
    assert "Could not take the container lock" in res.output


# ------------------------------------------------- waits, deadlines and what the helper leaves


def _holds(sandbox):
    """pids of this sandbox's container-side `gbx-lock.sh hold` helpers."""
    out = subprocess.run(
        ["ps", "-axww", "-o", "pid=,command="], capture_output=True, text=True
    ).stdout
    marker = f"{sandbox.root}/scripts/docker/gbx-lock.sh hold"
    return [int(x.split(None, 1)[0]) for x in out.splitlines() if marker in x]


def test_a_killed_waiter_does_not_linger_until_the_timeout(demo):
    # kill -9 of a run that is still WAITING left its container-side helper blocked in
    # `flock -w` for the whole timeout (an hour by default), a ghost holder when the lock freed.
    first = demo.popen("lockdemo.sh", "--hold", "60")
    _wait_for(lambda: "start" in _events(demo))
    second = demo.popen("lockdemo.sh")
    _wait_for(lambda: len(_holds(demo)) == 2)  # the waiter's helper is up and waiting

    _kill_host_side(second, clients=1)

    _wait_for(
        lambda: len(_holds(demo)) == 1, timeout=10
    )  # only the real holder is left
    first.kill()
    first.communicate()


@pytest.mark.parametrize(
    "sig, code", [(signal.SIGTERM, 143), (signal.SIGINT, 130), (signal.SIGHUP, 129)]
)
def test_a_signalled_waiter_exits_with_its_signals_status_and_cleans_up(
    demo, tmp_path, sig, code
):
    # Waiting for the lock, the script stops its container-side helper and removes its status dir
    # (under $TMPDIR), and exits 128 + the signal number, not a fixed 143.
    scratch = tmp_path / "tmp"
    scratch.mkdir()
    first = demo.popen("lockdemo.sh", "--hold", "30")
    _wait_for(lambda: "start" in _events(demo))
    second = demo.popen("lockdemo.sh", TMPDIR=str(scratch))
    _wait_for(lambda: len(_holds(demo)) == 2)  # the second one is waiting

    second.send_signal(sig)
    second.communicate(timeout=30)

    assert second.returncode == code
    assert list(scratch.glob("gbx-lock.*")) == []
    _wait_for(lambda: len(_holds(demo)) == 1)  # its helper went with it
    first.kill()
    first.communicate()


def test_the_callers_own_signal_trap_is_back_once_the_lock_is_held(demo):
    # The wait phase traps TERM/INT/HUP; holding the lock, the caller's traps must be restored,
    # not cleared.
    demo.write_command(
        "trapdemo.sh",
        DEMO.replace(
            "check_docker\n",
            "trap 'echo caller-term >> \"$GBX_DEMO_EVENTS\"; exit 3' TERM\ncheck_docker\n",
        ),
    )
    run = demo.popen("trapdemo.sh", "--hold", "30")
    _wait_for(lambda: "start" in _events(demo))  # holds the lock now

    run.send_signal(signal.SIGTERM)
    run.communicate(timeout=30)

    assert run.returncode == 3 and "caller-term" in _events(demo)


def test_the_run_and_work_lock_waits_share_one_deadline(demo):
    # The run-lock wait used the whole timeout and the work-lock wait started a fresh one:
    # a run could wait twice as long as GBX_TEST_LOCK_TIMEOUT says.
    first = demo.popen("lockdemo.sh", "--hold", "4")
    _wait_for(lambda: "start" in _events(demo))

    with demo.lock_held_by("pid=9 cmd=orphan since=then", lock=demo.work_lock_file):
        started = time.monotonic()
        res = demo.run(
            "lockdemo.sh", GBX_TEST_LOCK_TIMEOUT="5"
        )  # ~4 s run wait, then work
        elapsed = time.monotonic() - started
    first.communicate(timeout=30)

    assert res.returncode == 75, res.output
    assert "Timed out after 5s waiting for earlier work" in res.output  # WORK_TIMEOUT
    assert elapsed < 8, elapsed  # about 5 s in all, not 4 + 5


def test_the_work_locks_holder_is_cleared_when_the_work_ends_or_is_stale(demo):
    info = Path(str(demo.work_lock_file) + ".info")
    script = demo.root / "scripts" / "docker" / "gbx-lock.sh"
    subprocess.run(
        ["bash", str(script), "run", "true"], env=demo.env(), capture_output=True
    )
    assert not info.exists()  # was left naming a finished process

    info.write_text(
        "pid=1 cmd=killed-long-ago since=then\n"
    )  # a kill -9 leaves this behind
    demo.run("lockdemo.sh")  # the next run finds the lock free
    assert not info.exists()


def test_a_container_without_flock_stops_the_helper_not_mistakes_it_for_busy(tmp_path):
    script = REPO_ROOT / "scripts" / "docker" / "gbx-lock.sh"
    nothing = tmp_path / "empty-bin"
    nothing.mkdir()

    res = subprocess.run(
        ["/bin/bash", str(script), "hold"],
        env={"PATH": str(nothing), "GBX_LOCK_FILE": str(tmp_path / "run.lock")},
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert res.returncode == 70, res.stdout
    assert res.stdout.startswith("ERROR flock failed (exit 127")


def test_work_that_outlasts_the_wait_names_itself_in_the_timeout(demo):
    with demo.lock_held_by("pid=9 cmd=orphan since=then", lock=demo.work_lock_file):
        res = demo.run("lockdemo.sh", GBX_TEST_LOCK_TIMEOUT="1")

    assert res.returncode == 75
    assert "Timed out after 1s waiting for earlier work" in res.output
    assert "pid=9 cmd=orphan" in res.output
    assert _events(demo) == []  # no work was started


def test_a_helper_that_exits_without_a_status_is_reported_with_its_output(demo):
    # Also pins that the helper's path comes from GBX_CONTAINER_ROOT (not a literal).
    res = demo.run("lockdemo.sh", GBX_CONTAINER_ROOT="/nowhere/geobrix")

    assert res.returncode == 70, res.output
    assert "exited without taking the lock" in res.output
    assert any(
        "/nowhere/geobrix/scripts/docker/gbx-lock.sh" in c for c in demo.execs()[0]
    )


def test_the_work_wrapper_carries_this_shells_lock_overrides_into_the_container(
    sandbox,
):
    # `docker exec` does not pass the host environment; the work lock file and the timeout
    # have to ride in the command, or an override reaches only the run lock.
    sandbox.run("gbx-docker-exec.sh", "mvn -version", GBX_TEST_LOCK_TIMEOUT="123")

    (work,) = _work_execs(sandbox)
    assert f"GBX_WORK_LOCK_FILE={sandbox.work_lock_file}" in work
    assert "GBX_LOCK_TIMEOUT=123" in work and _under_work_lock(work)


def test_a_docker_exec_command_that_is_not_maven_or_pytest_skips_the_lock(sandbox):
    with sandbox.lock_held_by("pid=4242 cmd=gbx-test-scala.sh since=now"):
        res = sandbox.run("gbx-docker-exec.sh", "ls /root", "--no-wait")

    assert res.returncode == 0 and "container lock" not in res.output
    assert sandbox.execs() == [["bash", "-c", "set -o pipefail; ls /root"]]


# ----------------------------------- every locked command sends its work through the wrapper


# These run no Maven or pytest exec themselves: gbx:test:docs runs three locked doc-suite
# children, and gbx:test:light calls gbx:test:pyrx.
DELEGATES = {"gbx-test-docs.sh", "gbx-test-light.sh"}

# `docker exec` statements in a locked command that are NOT Maven/pytest work, so they run
# without the work lock: (command, regex that must match the WHOLE statement; re.S). Keep it
# short; a new exec that is not listed here must go through $GBX_LOCK_RUN (or $LOCK_PREFIX).
# A listed statement that names mvn or pytest anywhere is never exempt (see _unwrapped_execs).
NOT_WORK = [
    # editable-install check
    (
        "gbx-test-python.sh",
        r'docker exec geobrix-dev /bin/bash -c \\\n"pip show geobrix .*--quiet"',
    ),
    # test-deps probe
    (
        "gbx-test-python.sh",
        r'docker exec geobrix-dev /bin/bash -c \\\n"python3 -c \'import importlib\.metadata as m, rio_cogeo.*anywidget==[\d.]+"',
    ),
    # zarr extra pin check
    (
        "gbx-test-python.sh",
        r"docker exec -i geobrix-dev python3 - >/dev/null 2>&1 <<'PY'",
    ),
    # zarr extra
    (
        "gbx-test-python.sh",
        r'docker exec geobrix-dev /bin/bash -c \\\n"pip3 install --break-system-packages --quiet -e \'/root/geobrix/python/geobrix\[zarr_dbr19\]\'" \\\n\|\| echo -e .*',
    ),
    # reads the pytest summary
    (
        "gbx-test-parity.sh",
        r'SUMMARY=\$\(docker exec geobrix-dev /bin/bash -c \\\n"tail -6 \$REPORT 2>/dev/null \| grep -E \'passed\|failed\|skipped\|no tests ran\'; grep -E \'\^SKIPPED\' \$REPORT 2>/dev/null" 2>/dev/null\)',
    ),
    (
        "gbx-test-python-docs.sh",
        r"if ! docker exec geobrix-dev test -d /Volumes 2>/dev/null; then",
    ),
    (
        "gbx-test-python-docs.sh",
        r'if ! docker exec geobrix-dev test -d "\$SAMPLE_DATA_ROOT/geobrix-examples" 2>/dev/null; then',
    ),
    (
        "gbx-test-sql-docs.sh",
        r"if ! docker exec geobrix-dev test -d /Volumes 2>/dev/null; then",
    ),
    (
        "gbx-test-docs.sh",
        r"if ! docker exec geobrix-dev test -d /Volumes 2>/dev/null; then",
    ),
    (
        "gbx-coverage-scala.sh",
        r'docker exec geobrix-dev /bin/bash -c "mkdir -p /root/geobrix/coverage-artifacts"',
    ),
    (
        "gbx-coverage-scala.sh",
        r'docker exec geobrix-dev /bin/bash -c "cp -f /root/geobrix/target/scoverage\.xml /root/geobrix/coverage-artifacts/scoverage-\$\{pkg\}\.xml 2>/dev/null \|\| true"',
    ),
    (
        "gbx-docker-exec.sh",
        r"docker exec -it geobrix-dev (spark-shell|pyspark|python3|scala|bash)",
    ),
    # the lock helper itself (common.sh): holds the RUN lock, runs no work
    (
        "common.sh",
        r'docker exec -i \\\n(-e [A-Z_]+="[^"]*" \\\n)+geobrix-dev /bin/bash "\$GBX_CONTAINER_LOCK" hold \\\n<&6 >"\$status" 2>&1 3>&- 7>&- 6<&- &',
    ),
]

# A `docker exec` at the start of a statement, or after an operator or a keyword that begins one.
DOCKER_EXEC_CALL = re.compile(
    r"(^|[=(&|;!{]|\bif\b|\bthen\b|\belse\b|\belif\b|\bdo\b)\s*docker exec\b"
)
WRAPPERS = ("$GBX_LOCK_RUN", "$LOCK_PREFIX")


def _docker_exec_statements(text):
    """(line number, statement) for every `docker exec` call; a statement runs on through
    lines that end with a backslash."""
    lines = text.splitlines()
    out, i = [], 0
    while i < len(lines):
        line = lines[i]
        if not line.lstrip().startswith("#") and DOCKER_EXEC_CALL.search(line):
            j, stmt = i, [line]
            while lines[j].rstrip().endswith("\\") and j + 1 < len(lines):
                j += 1
                stmt.append(lines[j])
            out.append((i + 1, "\n".join(s.strip() for s in stmt)))
            i = j
        i += 1
    return out


def _unwrapped_execs(command, text):
    """The `docker exec` calls in `text` that neither use the work lock nor are in NOT_WORK."""
    allowed = [rx for cmd, rx in NOT_WORK if cmd == command]
    return [
        (lineno, stmt.splitlines()[0])
        for lineno, stmt in _docker_exec_statements(text)
        if not any(w in stmt for w in WRAPPERS)
        and not (
            any(re.fullmatch(rx, stmt, re.S) for rx in allowed)
            and not MAVEN_OR_PYTEST.search(stmt)
        )
    ]


def _command_text(command):
    return (REPO_ROOT / "scripts" / "commands" / command).read_text()


@pytest.mark.parametrize("command", sorted(set(LOCKED) - VIA_PYTHON))
def test_every_docker_exec_of_a_locked_command_is_under_the_work_lock(command):
    """EACH exec, not 'somewhere in the file': one bare Maven exec leaves that path unfenced."""
    text = _command_text(command)

    assert (
        _unwrapped_execs(command, text) == []
    ), f"{command}: wrap Maven/pytest execs in $GBX_LOCK_RUN, or list a non-work exec in NOT_WORK"
    if command not in DELEGATES:
        assert any(
            any(w in stmt for w in WRAPPERS)
            for _, stmt in _docker_exec_statements(text)
        ), f"{command}: no exec goes through the work lock"


def test_every_not_work_entry_still_matches_an_exec():
    for command, rx in NOT_WORK:
        stmts = [stmt for _, stmt in _docker_exec_statements(_command_text(command))]
        assert any(
            re.fullmatch(rx, stmt, re.S) for stmt in stmts
        ), f"stale NOT_WORK: {command} {rx}"


def test_the_legacy_doc_tests_script_sends_nothing_to_the_container_itself():
    """scripts/ci/run-doc-tests.sh ran `mvn test` with a raw docker exec, unlocked; its local runs
    now call the locked gbx commands, so it has no docker exec of its own."""
    text = (REPO_ROOT / "scripts" / "ci" / "run-doc-tests.sh").read_text()

    assert _docker_exec_statements(text) == []
    assert "gbx-test-scala-docs.sh" in text and "gbx-test-python-docs.sh" in text


def test_common_sh_runs_no_work_exec_outside_the_lock_helper():
    """The guard read only the top-level commands; the helper library is held to it too."""
    assert _unwrapped_execs("common.sh", _command_text("common.sh")) == []


def test_every_docker_exec_in_a_python_helper_is_under_the_work_lock():
    """Per exec, as for the scripts: a helper with one wrapped and one bare exec fails."""
    direct, _ = _maven_helpers()
    assert direct
    for path in direct:
        execs = re.findall(r'"docker",\s*"exec"(.*?)\]', path.read_text(), re.S)
        assert execs and all(
            "LOCK_RUN" in e or "gbx-lock.sh" in e for e in execs
        ), f"{path.name}: an exec outside the work lock"


@pytest.mark.parametrize(
    "line",
    [
        "else docker exec geobrix-dev mvn test",
        "for x in a; do docker exec geobrix-dev mvn test; done",
        "{ docker exec geobrix-dev mvn test; }",
        "elif docker exec geobrix-dev mvn test; then",
    ],
    ids=["else", "do", "brace", "elif"],
)
def test_the_guard_sees_a_docker_exec_after_else_do_brace_and_elif(line):
    assert len(_docker_exec_statements(line)) == 1


def test_a_listed_probe_does_not_exempt_a_longer_exec_that_does_other_work(monkeypatch):
    """NOT_WORK entries match the WHOLE statement. With a short entry (what a careless edit adds),
    `test -d /Volumes && rm -rf target` must still be flagged: a search would exempt it.
    """
    monkeypatch.setattr(
        sys.modules[__name__],
        "NOT_WORK",
        [("gbx-x.sh", r"docker exec geobrix-dev test -d /Volumes")],
    )
    probe = "docker exec geobrix-dev test -d /Volumes"
    longer = probe + " && rm -rf /root/geobrix/target"

    assert _unwrapped_execs("gbx-x.sh", probe) == []
    assert [stmt for _, stmt in _unwrapped_execs("gbx-x.sh", longer)] == [longer]


def test_the_guard_catches_an_unwrapped_pytest_exec_in_gbx_test_python():
    """Mutation: the main pytest exec of gbx:test:python loses its wrapper."""
    wrapped = 'docker exec geobrix-dev $GBX_LOCK_RUN /bin/bash -c "$PYTEST_CMD"'
    text = _command_text("gbx-test-python.sh")
    assert wrapped in text
    mutated = text.replace(wrapped, wrapped.replace("$GBX_LOCK_RUN ", ""))

    bad = _unwrapped_execs("gbx-test-python.sh", mutated)

    assert [stmt for _, stmt in bad] == [
        'docker exec geobrix-dev /bin/bash -c "$PYTEST_CMD"'
    ]


def test_the_guard_catches_the_unwrapped_by_package_maven_exec_in_gbx_coverage_scala():
    """Mutation: the --by-package Maven exec is bare, as it was before the fix."""
    wrapped = 'docker exec geobrix-dev $GBX_LOCK_RUN /bin/bash -c "$MVN_PKG"'
    text = _command_text("gbx-coverage-scala.sh")
    assert wrapped in text
    mutated = text.replace(wrapped, wrapped.replace("$GBX_LOCK_RUN ", ""))

    bad = _unwrapped_execs("gbx-coverage-scala.sh", mutated)

    assert [stmt.split(" ||")[0] for _, stmt in bad] == [
        'docker exec geobrix-dev /bin/bash -c "$MVN_PKG"'
    ]


def test_the_guard_catches_the_delta_jar_fetch_outside_the_work_lock():
    """Mutation: fetch-delta-test-jars.sh runs `mvn dependency:get` in the container."""
    wrapped = "docker exec geobrix-dev $GBX_LOCK_RUN /bin/bash /root/geobrix/scripts/ci/fetch-delta-test-jars.sh"
    text = _command_text("gbx-test-python.sh")
    assert wrapped in text
    mutated = text.replace(wrapped, wrapped.replace("$GBX_LOCK_RUN ", ""))

    bad = _unwrapped_execs("gbx-test-python.sh", mutated)

    assert len(bad) == 1 and "fetch-delta-test-jars.sh" in bad[0][1]


# ---- the two execs the per-exec guard found, run end to end against the fake docker


def test_coverage_by_package_sends_each_maven_run_through_the_work_lock(sandbox):
    sandbox.run("gbx-coverage-scala.sh", "--by-package")

    maven = [c for c in sandbox.execs() if "scoverage:test" in c[-1]]
    assert len(maven) >= 6  # one per package
    assert all(_under_work_lock(c) for c in maven), maven


def test_the_delta_jar_fetch_runs_under_the_work_lock(sandbox):
    lib = sandbox.root / "python" / "geobrix" / "lib"
    lib.mkdir(parents=True)
    (lib / "geobrix-0.0.0-jar-with-dependencies.jar").write_text("jar")

    sandbox.run("gbx-test-python.sh", "--path", "python/geobrix/test/zarr/")

    fetches = [
        c for c in sandbox.execs() if any("fetch-delta-test-jars.sh" in a for a in c)
    ]
    assert len(fetches) == 1
    assert _under_work_lock(fetches[0])


@pytest.mark.parametrize("rc", [75, 70])
def test_a_work_lock_failure_during_the_delta_fetch_is_not_called_a_missing_jar(
    sandbox, rc
):
    lib = sandbox.root / "python" / "geobrix" / "lib"
    lib.mkdir(parents=True)
    (lib / "geobrix-0.0.0-jar-with-dependencies.jar").write_text("jar")

    res = sandbox.run(
        "gbx-test-python.sh",
        *("--path", "python/geobrix/test/zarr/"),
        FAKE_DOCKER_EXEC_RC=str(rc),
    )

    assert res.returncode == rc, res.output
    assert "Delta jars unavailable" not in res.output
    assert "work lock" in res.output
    assert not [
        c for c in sandbox.execs() if "python3 -m pytest" in c[-1]
    ]  # never started


def test_the_exempt_notebook_runner_installs_geobrix_only_when_it_is_missing(sandbox):
    # It takes no lock, so its `pip install -e` into the shared site-packages must not run on
    # every call (that rewrites the editable install a locked pytest is importing).
    sandbox.run("gbx-test-notebooks.sh")

    (run,) = [c[-1] for c in sandbox.execs() if "run_notebooks_cell_by_cell" in c[-1]]
    assert re.search(r"pip show geobrix [^\n]*\|\| pip install --no-deps -e", run)
