# Docker Exec

Execute commands or launch interactive shells in geobrix-dev container

## Usage

```bash
bash .cursor/commands/gbx-docker-exec.sh [MODE|COMMAND] [OPTIONS]
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
- `--help` - Display help message

## Examples

```bash
# Interactive shells
bash .cursor/commands/gbx-docker-exec.sh --spark
bash .cursor/commands/gbx-docker-exec.sh --pyspark
bash .cursor/commands/gbx-docker-exec.sh --python
bash .cursor/commands/gbx-docker-exec.sh --scala
bash .cursor/commands/gbx-docker-exec.sh --bash

# Execute commands
bash .cursor/commands/gbx-docker-exec.sh "ls -la /root/geobrix"
bash .cursor/commands/gbx-docker-exec.sh "mvn -version"
bash .cursor/commands/gbx-docker-exec.sh --command "python3 --version"

# Execute with logging
bash .cursor/commands/gbx-docker-exec.sh "mvn test" --log maven-test.log
```

## Notes

- Requires geobrix-dev container to be running
- Interactive shells use `-it` flag (TTY)
- Command execution uses standard `docker exec`
- Logging only available for non-interactive commands
