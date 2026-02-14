# Start Docker Container

Start geobrix-dev container with proper volume mounts

## Usage

```bash
bash .cursor/commands/gbx-docker-start.sh [OPTIONS]
```

## Options

- `--attach` - Attach to container after start
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## Examples

```bash
# Start container
bash .cursor/commands/gbx-docker-start.sh

# Start and attach
bash .cursor/commands/gbx-docker-start.sh --attach

# Start with logging
bash .cursor/commands/gbx-docker-start.sh --log docker-start.log
```

## Volume Mounts

- `sample-data/Volumes` → `/Volumes` (Unity Catalog volumes)
- Project root → `/root/geobrix`
- `scripts/docker/m2` → `/root/geobrix/scripts/docker/m2` (Maven cache)

## Notes

- Uses `scripts/docker/start_docker_with_volumes.sh`
- **Maven setup**: On every start (or when already running), runs `scripts/docker/extras/docker_maven_setup.sh` so Maven uses project-local `.m2` (`scripts/docker/m2`) and `skipScoverage` as the default profile. Avoids a wiped `.m2` on each launch.
- Checks if container already running
- Starts existing container if stopped
- Creates new container if doesn't exist
