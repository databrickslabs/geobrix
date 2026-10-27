"""A stand-in for the `docker` CLI in the scripts/ci tests of the gbx:test:* commands.

It never reaches a Docker daemon. `docker ps` lists a running geobrix-dev. `docker exec`
appends the command (one JSON line) to $FAKE_DOCKER_EXEC_LOG and then either

- runs it on THIS machine (bash, flock and the lock file are real), when it is the test-lock
  holder (it carries the GBX_TEST_LOCK_HOLDER marker) or $FAKE_DOCKER_RUN_ALL=1; or
- does nothing and exits $FAKE_DOCKER_EXEC_RC (default 0): the work a command would send into
  the container (Maven, pytest) is recorded, not run.

Every other docker command succeeds and does nothing.
"""

import json
import os
import subprocess
import sys

CONTAINER = "geobrix-dev"
OPTS_WITH_VALUE = {"-e", "--env", "-w", "--workdir", "-u", "--user"}


def main(args):
    if not args:
        return 0
    if args[0] == "ps":
        print(CONTAINER)
        return 0
    if args[0] != "exec":
        return 0

    env = dict(os.environ)
    i = 1
    while i < len(args) and args[i] != CONTAINER:
        opt = args[i]
        if opt in ("-e", "--env"):
            key, _, value = args[i + 1].partition("=")
            env[key] = value
        i += 2 if opt in OPTS_WITH_VALUE else 1
    command = args[i + 1 :]

    with open(os.environ["FAKE_DOCKER_EXEC_LOG"], "a") as fh:
        fh.write(json.dumps(command) + "\n")

    holder = any("GBX_TEST_LOCK_HOLDER" in part for part in command)
    if holder or os.environ.get("FAKE_DOCKER_RUN_ALL") == "1":
        return subprocess.call(command, env=env)
    return int(os.environ.get("FAKE_DOCKER_EXEC_RC", "0"))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
