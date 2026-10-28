"""A stand-in for the `docker` CLI in the scripts/ci tests of the gbx:test:* commands.

It never reaches a Docker daemon. `docker ps` lists a running container named $FAKE_DOCKER_CONTAINER
(default geobrix-dev); an `exec` naming another container records an empty command. `docker exec`
appends the command (one JSON line) to $FAKE_DOCKER_EXEC_LOG -- and its `-e KEY=VALUE` pairs
to $FAKE_DOCKER_EXEC_LOG.env, same line number -- and then either

- runs it on THIS machine (bash, flock and the lock files are real), when it is the lock
  holder (`gbx-lock.sh hold`), or $FAKE_DOCKER_RUN_ALL=1, or its last argument matches the
  regex $FAKE_DOCKER_RUN_MATCHING (so a test can run one command string for real, such as a
  command that reads a file the test wrote, and leave the rest recorded); or
- does nothing, prints $FAKE_DOCKER_EXEC_STDOUT (default nothing) and exits
  $FAKE_DOCKER_EXEC_RC (default 0): the work a command would send into the container (Maven,
  pytest) is recorded, not run.

When $FAKE_DOCKER_EXEC_LOG is set, every call's whole argv is also appended to
$FAKE_DOCKER_EXEC_LOG.argv (one JSON line each), so a test can see which container each call named.

A path under /root/geobrix is run from $FAKE_DOCKER_ROOT instead (the scratch repo), so the
real scripts/docker/gbx-lock.sh runs as it does in the container. Every other docker command
succeeds and does nothing.
"""

import json
import os
import re
import subprocess
import sys

CONTAINER = os.environ.get("FAKE_DOCKER_CONTAINER", "geobrix-dev")
CONTAINER_REPO = "/root/geobrix"
LOCK_SCRIPT = "gbx-lock.sh"
OPTS_WITH_VALUE = {"-e", "--env", "-w", "--workdir", "-u", "--user"}


def main(args):
    if not args:
        return 0
    if os.environ.get("FAKE_DOCKER_EXEC_LOG"):
        with open(os.environ["FAKE_DOCKER_EXEC_LOG"] + ".argv", "a") as fh:
            fh.write(json.dumps(args) + "\n")
    if args[0] == "ps":
        print(CONTAINER)
        return 0
    if args[0] != "exec":
        return 0

    env = dict(os.environ)
    passed = {}  # the -e KEY=VALUE pairs of this call
    i = 1
    while i < len(args) and args[i] != CONTAINER:
        opt = args[i]
        if opt in ("-e", "--env"):
            key, _, value = args[i + 1].partition("=")
            env[key] = value
            passed[key] = value
        i += 2 if opt in OPTS_WITH_VALUE else 1
    command = args[i + 1 :]

    with open(os.environ["FAKE_DOCKER_EXEC_LOG"], "a") as fh:
        fh.write(json.dumps(command) + "\n")
    with open(os.environ["FAKE_DOCKER_EXEC_LOG"] + ".env", "a") as fh:
        fh.write(json.dumps(passed) + "\n")  # line N = the -e pairs of exec call N

    holder = (
        any(part.endswith(LOCK_SCRIPT) for part in command) and command[-1] == "hold"
    )
    pattern = os.environ.get("FAKE_DOCKER_RUN_MATCHING")
    matched = bool(pattern) and re.search(pattern, command[-1] if command else "")
    if holder or matched or os.environ.get("FAKE_DOCKER_RUN_ALL") == "1":
        root = os.environ.get("FAKE_DOCKER_ROOT")
        if root:
            command = [part.replace(CONTAINER_REPO, root) for part in command]
        return subprocess.call(command, env=env)
    print(os.environ.get("FAKE_DOCKER_EXEC_STDOUT", ""), end="")
    return int(os.environ.get("FAKE_DOCKER_EXEC_RC", "0"))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
