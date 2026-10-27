# Rebuild Docker Image

Rebuild the geobrix-dev Docker image using **scripts/docker/build_smart.sh** (multi-stage build). Optionally start the container after rebuild.

---

## Usage

```bash
bash scripts/commands/gbx-docker-rebuild.sh [OPTIONS] [-- DOCKER_FLAGS]
```

## Options

**Command options**

- `--platform <p>` – `linux/arm64` or `linux/amd64`. Default: `$GBX_DOCKER_PLATFORM`, else this host's architecture. Each platform builds its own image tag, so the arm64 and amd64 images coexist.
- `--no-cache` – Build without Docker cache (passed to build_smart.sh).
- `--no-pull` – Do not force-pull latest ubuntu:24.04 (faster, may miss base updates).
- `--keep-container` – Do not stop or remove the running geobrix-dev container.
- `--start` – Start container after rebuild.
- `--attach` – Attach to container after start (implies `--start`).
- `--log <path>` – Write output to log file (filename → `test-logs/<name>`). **Not for a build with a package index:** pip prints the index host into the build output, so the log contains it. Do not share that log.
- `--help` – Display help message.

**Docker flags (after `--`)**

Arguments after `--` are passed through to `docker build`. Examples:

- `--progress=plain` – Show full build output.
- `--build-arg CORES=4` – Override CPU cores used in build (default=2).

Do not pass `--platform` or a `PIP_INDEX_URL` build-arg here: the platform is the `--platform` option above, and the package index is passed as a secret (see below). `build_smart.sh` refuses both.

## Help from build_smart.sh

The underlying script **scripts/docker/build_smart.sh** provides:

- **Usage:** `build_smart.sh [OPTIONS] [-- DOCKER_FLAGS]`
- **Options:** `-h, --help` (help); `--platform <p>` (linux/arm64 or linux/amd64); `-p, --pull` (force pull ubuntu:24.04, default on); `--no-pull` (do not pull base image).
- **Docker flags:** Any arguments after `--` (or unknown args) are passed to `docker build` (e.g. `--no-cache`, `--progress=plain`, `--build-arg CORES=4`).
- **Examples (from script):**
  - `build_smart.sh --no-cache` – Build everything from scratch.
  - `build_smart.sh --progress=plain` – Show detailed build output.
  - `build_smart.sh --build-arg CORES=4` – Override the CPU cores used (default=2).
  - `build_smart.sh --platform linux/amd64` – Build the amd64 image (default: the host's architecture).

## Default behavior

- Uses **scripts/docker/build_smart.sh** (multi-stage build; stages: base, system-deps, hadoop-builder, gdal-builder, final).
- **Platform:** `--platform`, else `$GBX_DOCKER_PLATFORM`, else this host's architecture (`uname -m`). An arm64 host builds a native arm64 image; an amd64 image on an arm64 Mac runs under emulation, about 10x slower. There is no amd64 default.
- Final image tag, one per architecture (same as start_docker_with_volumes.sh): **geobrix-dev:ubuntu24-gdal311-spark-arm64** and **geobrix-dev:ubuntu24-gdal311-spark-amd64**. Both coexist, so falling back to the other is instant. The old un-suffixed **geobrix-dev:ubuntu24-gdal311-spark** was the amd64 image; it is no longer written, and start reads it only as an amd64 fallback.
- Stage checkpoints are tagged `geobrix-checkpoint:<stage>-<arch>`.
- **Package index:** `export PIP_INDEX_URL=<your index>` before running to build through a private mirror. It reaches the build as a BuildKit secret (a private temp file removed afterwards), never a build-arg, so it is not recorded in the image history; after the build the script checks the history and config for it. Unset builds against public PyPI. pip and uv inside the image still use the index: the build writes it to `pip.conf` and `uv.toml`, so **the image holds the index: never push, `docker save` or share it.** BuildKit does not key its layer cache on a secret, so the build also passes a non-secret fingerprint of the index (12 hex characters of an HMAC-SHA256 of it, keyed with a random per-machine salt kept 0600 in `~/.cache/geobrix/pip-index-fpr-salt` and never printed; build-arg `GBX_PIP_INDEX_FPR`, which `docker history` shows but which cannot confirm a guessed index) to the final stage only: changing the index rebuilds that stage and none before it (GDAL stays cached), without `--no-cache`. After the build the script checks the image history and config for the index (the URL, its host, and any `PIP_INDEX_URL=<value>`) and fails the build, untagged, if it finds one or cannot read them.
- **The Python lock follows the architecture:** `python/geobrix/requirements-dev-container.txt` (x86_64) or `requirements-dev-container-aarch64.txt` (arm64), chosen in the image by `dpkg`. They pin the same versions; `python3 scripts/ci/check_dev_container_locks.py` checks that.
- **Native-stack probe:** after the index check, `build_smart.sh` runs `scripts/docker/gbx-native-probe.sh` in a throwaway container of the finished image (never the running `geobrix-dev`) and tags the image only if it reports GDAL 3.11.4 (from the Dockerfile), PROJ 9.4.0, GEOS 3.12.1, a `libgdalalljni.so` that resolves `libgdal.so.37` with nothing unresolved, rasterio from a manylinux wheel (not built from source) at its vendored GDAL 3.12.1, and pyogrio, pyproj and shapely at the versions in the architecture's lock. A difference lists what is wrong and leaves the image untagged (the `geobrix-checkpoint:final-<arch>` image is kept to inspect).
- Stops and removes the existing **geobrix-dev** container before rebuilding so the next start uses the new image, **but only when that container runs the same architecture as the rebuild**. A container of the other architecture, and any container with `--keep-container`, is left alone; switch it later with `gbx:docker:start --recreate`.
- By default pulls latest ubuntu:24.04; use `--no-pull` to skip and speed up rebuilds.

## Examples

```bash
# Rebuild image (multi-stage, with pull) for this host's architecture
bash scripts/commands/gbx-docker-rebuild.sh

# Build the native arm64 image next to a running amd64 container, leaving that container alone
# (export PIP_INDEX_URL first when the build network needs a private mirror)
bash scripts/commands/gbx-docker-rebuild.sh --platform linux/arm64 --keep-container

# Build the amd64 image (fallback; runs under emulation on an arm64 host)
bash scripts/commands/gbx-docker-rebuild.sh --platform linux/amd64

# Rebuild without cache
bash scripts/commands/gbx-docker-rebuild.sh --no-cache

# Rebuild without pulling base image (faster)
bash scripts/commands/gbx-docker-rebuild.sh --no-pull

# Rebuild and start container
bash scripts/commands/gbx-docker-rebuild.sh --start

# Rebuild, start, and attach
bash scripts/commands/gbx-docker-rebuild.sh --start --attach

# Rebuild with detailed build output
bash scripts/commands/gbx-docker-rebuild.sh -- --progress=plain
```

## Notes

- **Dockerfile / build:** `scripts/docker/` (build_smart.sh runs from that directory).
- Rebuild can take several minutes; use `--no-cache` for a clean rebuild when needed.
- Start uses the same per-architecture image (`geobrix-dev:ubuntu24-gdal311-spark-<arch>`).
