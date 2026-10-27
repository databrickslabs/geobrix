# Docker Exec

Execute commands or launch interactive shells in geobrix-dev container

## Usage

```bash
bash scripts/commands/gbx-docker-exec.sh [MODE|COMMAND] [OPTIONS]
```

## Interactive Shell Modes

- `--spark` - Launch Spark shell (spark-shell)
- `--pyspark` - Launch PySpark shell
- `--python` - Launch Python 3 shell
- `--scala` - Launch Scala REPL
- `--bash` - Launch interactive bash shell

## Command Execution

- `<command>` - Execute bash command and exit
- `--command <cmd>` - Execute bash command and exit (explicit)

## Options

- `--interactive` - Run command in interactive mode (keep TTY)
- `--log <path>` - Write output to log file (non-interactive only)
- `--no-wait` - Fail at once (exit 75) if another test run holds the container lock, instead of waiting for it (see "One test run at a time")
- `--help` - Display help message

## One test run at a time

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first. It is shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, `gbx:data:stage-jar`/`stage-wheel`, and `gbx:docker:exec` with an `mvn` or `pytest` command). Only a command that runs `mvn` or `pytest` takes it; anything else (a shell, `ls`, a version check) runs at once.

- **What it guarantees.** gbx commands never overlap in the container, including after a killed run. There are two locks inside the container (`scripts/docker/gbx-lock.sh`): the *run* lock, held while this command or any child of it is alive, which makes whole runs take turns; and the *work* lock, held by the Maven or pytest process itself for as long as it runs (the lock is not passed on to children that Python's `subprocess` or Java's `ProcessBuilder` start, such as a Surefire fork; they normally exit with their parent). If this command is killed (`kill -9`, a harness stop, a timeout), the run lock frees but the container's Maven or pytest keeps running, and keeps the work lock, so the next run waits for it to end and names the process still holding it.
- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock's `.info` file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder (or the earlier work still running).
- **What it does not cover.** Maven or pytest started in the container outside a gbx command (an interactive `docker exec geobrix-dev mvn ...`) is not seen. A work process that never ends keeps the lock until it is killed; the message names it.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

## Examples

```bash
# Interactive shells
bash scripts/commands/gbx-docker-exec.sh --spark
bash scripts/commands/gbx-docker-exec.sh --pyspark
bash scripts/commands/gbx-docker-exec.sh --python
bash scripts/commands/gbx-docker-exec.sh --scala
bash scripts/commands/gbx-docker-exec.sh --bash

# Execute commands
bash scripts/commands/gbx-docker-exec.sh "ls -la /root/geobrix"
bash scripts/commands/gbx-docker-exec.sh "mvn -version"
bash scripts/commands/gbx-docker-exec.sh --command "python3 --version"

# Execute with logging
bash scripts/commands/gbx-docker-exec.sh "mvn test" --log maven-test.log
```

## Notes

- Requires geobrix-dev container to be running
- Interactive shells use `-it` flag (TTY)
- Command execution uses standard `docker exec`
- Logging only available for non-interactive commands
