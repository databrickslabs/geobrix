# Restart Docker Container

Restart geobrix-dev container

## Usage

```bash
bash scripts/commands/gbx-docker-restart.sh [OPTIONS]
```

## Options

- `--timeout <seconds>` - Timeout before force stop (default: 10)
- `--attach` - Attach to container after restart
- `--privileged` - Recreate the container in privileged mode (stop, remove, create). It is recreated on the architecture of the container it replaces, read from that container's image, never the host default or `GBX_DOCKER_PLATFORM`; the image for that architecture must exist, checked before anything is stopped. To change architecture use `gbx:docker:start --recreate --platform <p>`.
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## Examples

```bash
# Restart container
bash scripts/commands/gbx-docker-restart.sh

# Restart with custom timeout
bash scripts/commands/gbx-docker-restart.sh --timeout 30

# Restart and attach
bash scripts/commands/gbx-docker-restart.sh --attach
```

## Notes

- Default timeout is 10 seconds
- **Maven setup**: After restart, runs the same Maven setup as `gbx:docker:start` (project `.m2` + `skipScoverage` default) so `mvn` uses the project-local repository and doesn’t re-download into a wiped `.m2`.
- Preserves volume mounts and configuration
- Creates container if it doesn't exist (via start)
- Faster than stop + start for existing container
