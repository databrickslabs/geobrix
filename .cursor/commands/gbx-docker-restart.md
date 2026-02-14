# Restart Docker Container

Restart geobrix-dev container

## Usage

```bash
bash .cursor/commands/gbx-docker-restart.sh [OPTIONS]
```

## Options

- `--timeout <seconds>` - Timeout before force stop (default: 10)
- `--attach` - Attach to container after restart
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## Examples

```bash
# Restart container
bash .cursor/commands/gbx-docker-restart.sh

# Restart with custom timeout
bash .cursor/commands/gbx-docker-restart.sh --timeout 30

# Restart and attach
bash .cursor/commands/gbx-docker-restart.sh --attach
```

## Notes

- Default timeout is 10 seconds
- **Maven setup**: After restart, runs the same Maven setup as `gbx:docker:start` (project `.m2` + `skipScoverage` default) so `mvn` uses the project-local repository and doesn’t re-download into a wiped `.m2`.
- Preserves volume mounts and configuration
- Creates container if it doesn't exist (via start)
- Faster than stop + start for existing container
