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

This command runs Maven or a Spark test JVM in `geobrix-dev`, so it takes the container test lock first: a `flock` on `/tmp/gbx-test.lock` inside the container, shared by every command that runs Maven or a test JVM there (`gbx:test:*`, `gbx:coverage:*`, `gbx:lint:scalastyle`, `gbx:bench:heavyweight`, and `gbx:docker:exec` with an `mvn` or `pytest` command), so two runs never share `target/`. Only a command that runs `mvn` or `pytest` takes it; anything else (a shell, `ls`, a version check) runs at once.

- **Default: wait.** A second run prints one line naming the holder (its command and PID, read from the lock file) and waits, giving up after `GBX_TEST_LOCK_TIMEOUT` seconds (default `3600`) with exit 75.
- **`--no-wait`: fail fast.** Exit 75 at once, naming the holder.
- The log starts with this run's PID and the lock acquisition (`test lock acquired: pid <pid> <command>`), so a duplicate launch shows up.
- The lock is released when the command exits, and also when it is killed. Do not detach a run with `&` or `nohup`: use the harness's `run_in_background`, which is tracked.

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
