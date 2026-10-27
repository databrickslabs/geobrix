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
RUN_PREFIX = ["/bin/bash", "/root/geobrix/scripts/docker/gbx-lock.sh", "run"]


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

# Commands that send Maven or pytest into the container but are not locked, and why.
EXEMPT = {
    "gbx-test-python-dbr.sh": "tests against a remote DBR cluster; no local Maven or Spark JVM",
    "gbx-test-notebooks.sh": "runs fixture/sample notebooks in an isolated venv and temp workdir; "
    "no Maven, no Spark JVM on target/",
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
    script = (commands / command).read_text()
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


def _kill_host_side(proc):
    """kill -9 a gbx script and its `docker exec` clients; what runs 'in the container' lives on."""
    out = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,command="], capture_output=True, text=True
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
    assert len(victims) >= 3, rows  # the script and its two docker clients
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


# ----------------------------------- every locked command sends its work through the wrapper


# gbx:test:docs runs no Maven itself: its three doc-suite children are locked commands.
DELEGATES = {"gbx-test-docs.sh"}


@pytest.mark.parametrize("command", sorted(set(LOCKED) - VIA_PYTHON - DELEGATES))
def test_a_locked_command_runs_its_maven_or_pytest_under_the_work_lock(command):
    text = (REPO_ROOT / "scripts" / "commands" / command).read_text()
    assert "$GBX_LOCK_RUN" in text, f"{command}: wrap the work exec in $GBX_LOCK_RUN"
