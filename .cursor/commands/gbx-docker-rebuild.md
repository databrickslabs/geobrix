# Rebuild Docker Image

Rebuild the geobrix-dev Docker image using **scripts/docker/build_smart.sh** (multi-stage build). Optionally start the container after rebuild.

---

## Usage

```bash
bash .cursor/commands/gbx-docker-rebuild.sh [OPTIONS] [-- DOCKER_FLAGS]
```

## Options

**Command options**

- `--no-cache` – Build without Docker cache (passed to build_smart.sh).
- `--no-pull` – Do not force-pull latest ubuntu:24.04 (faster, may miss base updates).
- `--start` – Start container after rebuild.
- `--attach` – Attach to container after start (implies `--start`).
- `--log <path>` – Write output to log file (filename → `test-logs/<name>`).
- `--help` – Display help message.

**Docker flags (after `--`)**

Arguments after `--` are passed through to `docker build`. Examples:

- `--progress=plain` – Show full build output.
- `--build-arg CORES=4` – Override CPU cores used in build (default=2).
- `--build-arg BUILDPLATFORM=<?>` – Override platform (default=linux/amd64).

## Help from build_smart.sh

The underlying script **scripts/docker/build_smart.sh** provides:

- **Usage:** `build_smart.sh [OPTIONS] [-- DOCKER_FLAGS]`
- **Options:** `-h, --help` (help); `-p, --pull` (force pull ubuntu:24.04, default on); `--no-pull` (do not pull base image).
- **Docker flags:** Any arguments after `--` (or unknown args) are passed to `docker build` (e.g. `--no-cache`, `--progress=plain`, `--build-arg CORES=4`).
- **Examples (from script):**
  - `build_smart.sh --no-cache` – Build everything from scratch.
  - `build_smart.sh --progress=plain` – Show detailed build output.
  - `build_smart.sh --build-arg CORES=4` – Override the CPU cores used (default=2).
  - `build_smart.sh --build-arg BUILDPLATFORM=<?>` – Override platform (default=linux/amd64).

## Default behavior

- Uses **scripts/docker/build_smart.sh** (multi-stage build; stages: base, system-deps, hadoop-builder, gdal-builder, pdal-builder, final).
- Final image tag: **geobrix-dev:ubuntu24-gdal311-spark** (same as start_docker_with_volumes.sh).
- Stops and removes the existing **geobrix-dev** container before rebuilding so the next start uses the new image.
- By default pulls latest ubuntu:24.04; use `--no-pull` to skip and speed up rebuilds.

## Examples

```bash
# Rebuild image (multi-stage, with pull)
bash .cursor/commands/gbx-docker-rebuild.sh

# Rebuild without cache
bash .cursor/commands/gbx-docker-rebuild.sh --no-cache

# Rebuild without pulling base image (faster)
bash .cursor/commands/gbx-docker-rebuild.sh --no-pull

# Rebuild and start container
bash .cursor/commands/gbx-docker-rebuild.sh --start

# Rebuild, start, and attach
bash .cursor/commands/gbx-docker-rebuild.sh --start --attach

# Rebuild with detailed build output
bash .cursor/commands/gbx-docker-rebuild.sh -- --progress=plain

# Rebuild with log
bash .cursor/commands/gbx-docker-rebuild.sh --log rebuild.log
```

## Notes

- **Dockerfile / build:** `scripts/docker/` (build_smart.sh runs from that directory).
- Rebuild can take several minutes; use `--no-cache` for a clean rebuild when needed.
- Start uses the same image: **geobrix-dev:ubuntu24-gdal311-spark**.
