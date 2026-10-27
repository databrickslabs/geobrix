"""Tests for the container test lock in scripts/commands/common.sh.

Two gbx:test:* runs in geobrix-dev at once put two Maven or Spark JVMs on one target/. Every
command that runs Maven or a Spark test JVM there takes one flock (inside the container, on a
fixed path) first: it waits for the holder by default, and with --no-wait it fails at once
with exit 75, naming the holder.

The commands run from a scratch copy of scripts/commands/ against a fake `docker` (see
conftest.py's ContainerSandbox): no daemon, no Maven. The lock itself is real.
"""

import re
import time

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
    assert all("GBX_TEST_LOCK_HOLDER" in " ".join(c) for c in sandbox.execs())


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
