# Rebuild Docker Image

Rebuild a dev image using **scripts/docker/build_smart.sh**. The default builds the base image, which has no GDAL of its own. `--overlay gbxnative` builds the native overlay on it, which the dev container runs. Optionally start the container after rebuild.

---

## Usage

```bash
bash scripts/commands/gbx-docker-rebuild.sh [OPTIONS] [-- DOCKER_FLAGS]
```

## Options

**Command options**

- `--platform <p>` – `linux/arm64` or `linux/amd64`. Default: `$GBX_DOCKER_PLATFORM`, else this host's architecture. Each platform builds its own image tag, so the arm64 and amd64 images coexist.
- `--overlay gbxnative` – Build the **native overlay** instead of the base image. It layers the env6 wheel's natives (GDAL, PROJ, GEOS, the bindings built against them and the JNI) over the **existing** base image. It also layers the rasterio distribution built against that GDAL and the pinned Rust toolchain (`rustc`, `cargo`, `maturin`). The overlay is tagged `geobrix-dev:ubuntu24-gbxnative-spark-<arch>`, the image `gbx:docker:start` runs.
The running container is never stopped or removed (the overlay implies `--keep-container`; `--start` is refused).
To run the new overlay, run `gbx:docker:start --recreate` at a quiet point.
It needs the base image (`gbx:docker:rebuild`). It needs the env6 wheel built (`gbx:build:wheel --env 6`, which extracts `build/env-wheel/env6/payload`).
It also needs the bindings built (`build/native/out/<arch>/bindings/dist/`).
And `scripts/native/prefetch.sh --arch <arch>` must have run (the pinned `rustup-init` and `maturin` wheel). Each is checked against its pin. A missing one stops the build with `PRECONDITION MISSING`. The build context is assembled in `build/devimage/<arch>/`. No package index is used; the only network use is rustup's verified toolchain download.
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
- **Options:** `-h, --help` (help); `--platform <p>` (linux/arm64 or linux/amd64); `-p, --pull` (force pull ubuntu:24.04, default on); `--no-pull` (do not pull base image); `--overlay gbxnative` (the native overlay, above).
- **Docker flags:** Any arguments after `--` (or unknown args) are passed to `docker build` (e.g. `--no-cache`, `--progress=plain`, `--build-arg CORES=4`).
- **Examples (from script):**
  - `build_smart.sh --no-cache` – Build everything from scratch.
  - `build_smart.sh --progress=plain` – Show detailed build output.
  - `build_smart.sh --build-arg CORES=4` – Override the CPU cores used (default=2).
  - `build_smart.sh --platform linux/amd64` – Build the amd64 image (default: the host's architecture).

## Default behavior

- Uses **scripts/docker/build_smart.sh** (multi-stage build; stages: base, system-deps, hadoop-builder, final).
- **Platform:** `--platform`, else `$GBX_DOCKER_PLATFORM`, else this host's architecture (`uname -m`). An arm64 host builds a native arm64 image; an amd64 image on an arm64 Mac runs under emulation, about 10x slower. There is no amd64 default.
- Base image tag, one per architecture: **geobrix-dev:ubuntu24-base-spark-arm64** and **geobrix-dev:ubuntu24-base-spark-amd64**. Both coexist.
- The base image has **no GDAL of its own**: no system libgdal, no `libgdalalljni.so`, no `osgeo`. The start scripts run the native overlay on it (`geobrix-dev:ubuntu24-gbxnative-spark-<arch>`). After a base rebuild, rebuild the overlay (`--overlay gbxnative`), then `gbx:docker:start --recreate`.
- Stage checkpoints are tagged `geobrix-checkpoint:<stage>-<arch>`.
- **Package index:** `export PIP_INDEX_URL=<your index>` before running to build through a private mirror. It reaches the build as a BuildKit secret (a private temp file removed afterwards), never a build-arg. So it is not recorded in the image history. After the build the script checks the history and config for it. Unset builds against public PyPI. pip and uv inside the image still use the index. The build writes it to `pip.conf` and `uv.toml`, so **the image holds the index: never push, `docker save` or share it.** BuildKit does not key its layer cache on a secret. So the build also passes a non-secret fingerprint of the index to the final stage only. The fingerprint is 12 hex characters of an HMAC-SHA256 of the index. It is keyed with a random per-machine salt kept 0600 in `~/.cache/geobrix/pip-index-fpr-salt` and never printed. It is the build-arg `GBX_PIP_INDEX_FPR`, which `docker history` shows but which cannot confirm a guessed index. Changing the index rebuilds that stage and none before it, without `--no-cache`. After the build the script checks the image history and config for the index (the URL, its host, and any `PIP_INDEX_URL=<value>`). It fails the build, untagged, if it finds one or cannot read them.
- **The Python lock follows the architecture:** `python/geobrix/requirements-dev-container.txt` (x86_64) or `requirements-dev-container-aarch64.txt` (arm64), chosen in the image by `dpkg`. They pin the same versions; `python3 scripts/ci/check_dev_container_locks.py` checks that.
- **Native-stack probe:** after the index check, `build_smart.sh` runs `scripts/docker/gbx-native-probe.sh` in a throwaway container of the finished image (never the running `geobrix-dev`; `docker run --network none`). It tags the image only if the probe reports no system GDAL. The probe must find none of these:
  - `gdalinfo`;
  - a libgdal in a system library directory or the loader cache;
  - `libgdalalljni.so`;
  - `osgeo`.

  A probe whose output lacks one of these checks fails. It must report PROJ 9.4.0 and GEOS 3.12.1 (noble apt). It must report rasterio from a manylinux wheel (not built from source) at its vendored GDAL 3.12.1. It must also report pyogrio, pyproj and shapely at the versions in the architecture's lock. A difference lists what is wrong and leaves the image untagged (the `geobrix-checkpoint:final-<arch>` image is kept to inspect). A probe that cannot run prints the tail of its stderr. The expected rasterio GDAL was measured for the rasterio release the dev lock pins (`EXPECT_RASTERIO_GDAL_VERIFIED_FOR` in `build_smart.sh`). When the lock moves to another release the build stops before building with "re-verify the probe constants".
- **Native overlay probe:** `--overlay gbxnative` runs the same probe with `--payload /opt/gbx-native` and compares with `scripts/native/pins.conf`. It checks the payload's GDAL, PROJ and GEOS and the native revision. It checks osgeo, rasterio, pyogrio, pyproj and shapely, each imported from the payload at the pinned library versions. It checks the rasterio distribution `<RASTERIO_VERSION>+gbx<revision>`. It checks a JNI whose libgdal (the soname read from the file, listed in the payload's `MANIFEST.json`) resolves inside the payload. It checks `/usr/lib/libgdalalljni.so` linked to the payload's JNI. It also checks that no PyPI wheel's `*.libs` is mapped into the process.
- Stops and removes the existing **geobrix-dev** container before rebuilding so the next start uses the new image. **It does this only when that container runs the same architecture as the rebuild**. A container of the other architecture, and any container with `--keep-container`, is left alone; switch it later with `gbx:docker:start --recreate`.
- By default pulls latest ubuntu:24.04; use `--no-pull` to skip and speed up rebuilds.

## Examples

```bash
# Rebuild image (multi-stage, with pull) for this host's architecture
bash scripts/commands/gbx-docker-rebuild.sh

# Build the native arm64 image next to a running amd64 container, leaving that container alone
# (export PIP_INDEX_URL first when the build network needs a private mirror)
bash scripts/commands/gbx-docker-rebuild.sh --platform linux/arm64 --keep-container

# Build the amd64 base image (runs under emulation on an arm64 host)
bash scripts/commands/gbx-docker-rebuild.sh --platform linux/amd64

# Layer the env wheel's natives and Rust over the existing base image (the container is not touched)
bash scripts/commands/gbx-docker-rebuild.sh --overlay gbxnative

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
- Start runs the native overlay (`geobrix-dev:ubuntu24-gbxnative-spark-<arch>`). When it is missing, start prints the steps that build it, the base image first.
