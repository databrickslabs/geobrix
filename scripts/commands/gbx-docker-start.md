# Start Docker Container

Start this checkout's dev container with proper volume mounts

## Usage

```bash
bash scripts/commands/gbx-docker-start.sh [OPTIONS]
```

## Options

- `--platform <p>` - `linux/arm64` or `linux/amd64` (default: `$GBX_DOCKER_PLATFORM`, else this host's architecture): which per-architecture image a **new** container is created from.
- `--recreate` - Stop and remove an existing container first, then create a new one from the requested platform's image. This is how to switch architecture.
- `--attach` - Attach to container after start
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## Examples

```bash
# Start container
bash scripts/commands/gbx-docker-start.sh

# Start and attach
bash scripts/commands/gbx-docker-start.sh --attach

# Start with logging
bash scripts/commands/gbx-docker-start.sh --log docker-start.log

# Switch an existing container to the native arm64 image (stops it: do it at a quiet point)
bash scripts/commands/gbx-docker-start.sh --recreate --platform linux/arm64

# Fall back to the amd64 image (emulated on an arm64 host)
GBX_DOCKER_PLATFORM=linux/amd64 bash scripts/commands/gbx-docker-start.sh --recreate
```

## Per-worktree container

Each checkout has its own dev container, so a command run in one never stops, recreates or remounts another's.

- **Main checkout:** no setup. The container is `geobrix-dev` on host ports 5005 (JDWP), 8888 (Jupyter) and 4040 (Spark UI), with `sample-data/Volumes` at `/Volumes`.
- **A linked git worktree** (`git worktree add`) must name its own container in an untracked `<worktree>/.gbx-local.env`. Copy `scripts/docker/gbx-local.env.example` there and set `GBX_DEV_CONTAINER` and the three `GBX_DEV_PORT_*` values (they must differ from every other running container's). Without the file, every command that uses the container refuses to run and says so; commands that need no container are not affected. The file must name a container, and not `geobrix-dev` (the main checkout's): a worktree never defaults to it.
- **What the file can set:** the container name and ports; `GBX_DEV_VOLUMES_DIR` (the host directory mounted at `/Volumes`); `GBX_DEV_EXTRA_MOUNTS` (more `host:container[:ro]` bind mounts, space separated); `GBX_DEV_DOCKER_ENV` (extra `KEY=VALUE` container environment); `GBX_DEV_IMAGE_TAG`; and `GBX_CARGO_REGISTRY` (a crates mirror for the Rust build). Unknown keys are refused, and values are never printed. The file is gitignored and must never be committed.
- **Seeding a new worktree:** the container mounts the worktree's own `scripts/docker/m2` (the Maven cache and settings) and `sample-data/Volumes`. Copy them from the main checkout once (`cp -cR` makes cheap APFS clones on macOS), then run this command from the worktree.
- **A worktree only touches a container that mounts it.** If the container named in `.gbx-local.env` exists but its `/root/geobrix` is not this worktree (two worktrees that chose the same name, or a stale container), every container command (`gbx:docker:exec`, `gbx:docker:start` with or without `--recreate`, `gbx:docker:restart`, `gbx:docker:rebuild`, the test commands) refuses and leaves it untouched. Pick another name, or remove the container by hand once you know no other checkout uses it.
- **Mount and checkout are compared as directories**, with symlinks resolved: Docker reports a bind mount as it was given (`/tmp/<x>` on macOS) while the checkout's root is its physical path (`/private/tmp/<x>`), and the two are one worktree, so its container is kept. Another directory, even one of the same name, is still another worktree.
- Every `gbx:*` command run from a worktree targets that worktree's container, including the test lock, `gbx:docker:exec` and `gbx:venv:sync --match-container`.

## Volume Mounts

- `sample-data/Volumes` → `/Volumes` (Unity Catalog volumes; `GBX_DEV_VOLUMES_DIR` in `.gbx-local.env` changes the host directory)
- Project root → `/root/geobrix`
- Anything in `GBX_DEV_EXTRA_MOUNTS`
- `scripts/docker/m2` → `/root/geobrix/scripts/docker/m2` (Maven cache)

## Notes

- Uses `scripts/docker/start_docker_with_volumes.sh`
- **Image and platform:** `geobrix-dev:ubuntu24-gdal311-spark-<arch>`, with the platform from `--platform`, else `$GBX_DOCKER_PLATFORM`, else this host's architecture. If that image does not exist the command stops with the build command (`gbx:docker:rebuild --platform ...`) and the fallback (`GBX_DOCKER_PLATFORM=linux/amd64`), rather than trying to pull a local-only tag. The old un-suffixed tag (the amd64 image) is used only as an amd64 fallback.
- A container recreated only because it is mounted to another worktree keeps the architecture of the container it replaces, unless `--platform` or `GBX_DOCKER_PLATFORM` is given.
- An existing container keeps the architecture it was created with, whatever is requested. When that differs, the command warns and keeps it (other runs may be using it); `--recreate` replaces it, but only after checking that the requested platform's image exists: with no such image it stops with the build command and leaves the container running (the same check guards the worktree-mismatch recreate and `gbx:docker:restart --privileged`).
- **Maven version check (warn only):** the pom refuses a Maven older than the version `.mvn/wrapper/maven-wrapper.properties` pins, and the container cannot fetch that Maven itself, so an image built before the pin moved gives a container whose first build fails. The command warns (never blocks) when the image behind an existing container, or the one a new or recreated container would use, has a different Maven (read from the `MAVEN_VERSION` build arg in `docker history`), and names `gbx:docker:rebuild` as the fix. `gbx:docker:restart --privileged` warns the same way before it recreates.
- **Maven setup**: On every start (or when already running), runs `scripts/docker/extras/docker_maven_setup.sh` so Maven uses project-local `.m2` (`scripts/docker/m2`) and `skipScoverage` as the default profile. Avoids a wiped `.m2` on each launch.
- Checks if container already running
- Starts existing container if stopped
- Creates new container if doesn't exist
