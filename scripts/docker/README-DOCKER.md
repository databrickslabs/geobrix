For tests to run in docker we need to copy the jar to site-packages/pyspark/jars.
Example: /root/geobrix/python/dev/lib/python3.12/site-packages/pyspark/jars/
spark.addArtifact does not work due to permissions issues.
Also there could be annoying warnings about jupyter_client which isn't something relevant to the pytest we do.
If those warnings occur use:  export JUPYTER_PLATFORM_DIRS=1

## Platform (arm64 and amd64)

The image is built and run natively for the host: `geobrix-dev:ubuntu24-gdal311-spark-arm64` on an arm64 Mac, `...-amd64` on an x86 host. An amd64 image on an arm64 Mac runs under Rosetta and is about 10x slower (one 131-test file took 1,410 s in the container against 134 s natively), so the default follows the host; there is no amd64 default.

- Build: `bash scripts/commands/gbx-docker-rebuild.sh [--platform linux/arm64|linux/amd64]`.
- Run: `scripts/docker/start_docker*.sh` and `gbx:docker:start` choose the platform and tag from the host. `GBX_DOCKER_PLATFORM=linux/amd64` overrides, e.g. to fall back to the amd64 image; `gbx:docker:start` and `start_docker_with_volumes.sh` also take `--platform <p>` (`start_docker.sh` takes no flags, only the environment variable). A Rosetta-launched shell on an Apple Silicon Mac is still detected as arm64.
- Both images can exist at once. Switch a running container with `gbx:docker:start --recreate --platform <p>`; the target image must exist, and is checked before the running container is stopped, so a missing image leaves the container as it is.
- The Hadoop native libraries follow the architecture too (the `-aarch64` release for arm64, SHA-512 pinned); `hadoop checknative` reports `hadoop: true` on arm64.
- The Python dependencies are hash-pinned per architecture: `python/geobrix/requirements-dev-container.txt` (x86_64) and `requirements-dev-container-aarch64.txt`, compiled from the same `.in` with the same versions (`python3 scripts/ci/check_dev_container_locks.py`).
- A private package index: `export PIP_INDEX_URL=...` before the build. It is passed as a BuildKit secret, not a build-arg, and is not in the image history or environment.
  - **The image holds the index** in its pip and uv configuration files (`/etc/pip.conf`, `/root/.pip/pip.conf`, `/etc/uv/uv.toml`) so pip and uv keep working inside the container. Never push, `docker save` or share the image.
  - A BuildKit secret is not part of the layer cache key, so the build also passes a non-secret fingerprint of the index (12 hex characters of its sha256, build-arg `GBX_PIP_INDEX_FPR`) to the final stage. Changing the index rebuilds that stage and no earlier one (GDAL and PDAL stay cached); no `--no-cache` needed.
  - pip prints the index host into the build output, so a `--log` file of a build with an index contains it. Do not share that log.
- `scripts/docker/gbx-platform.sh` holds the platform and tag logic that the build and start scripts share.

## Ports and binding

- The image sets **JUPYTER_IP=0.0.0.0** so the kernel ZMQ binds to all interfaces and works reliably in Docker (for **testbook** and notebook tests). No security requirement in this dev image.
- The container exposes **5005** (JDWP remote debug), **8888** (Jupyter's default port, for an ad-hoc `jupyter` server), **4040** (Spark UI). JDWP is opt-in: no `JAVA_TOOL_OPTIONS` is set by default; debug a test run with `mvn test -DagentLib='${jdwp.agent}' ...` and attach to `localhost:5005` (see the comment in `start_docker.sh` to attach the agent to every JVM instead). Jupyter Lab is installed only as a transitive dependency of `ipywidgets` 7 and is not otherwise used; the goal is that **testbook** can start and talk to the kernel (`ipykernel`). Example-specific packages (e.g. `keplergl`) are installed per example when needed, not baked into the image.

## Optional: privileged mode

If you hit kernel or ZMQ issues (e.g. entropy or device access), you can try running the container with `--privileged`. It is not required for normal use.
  ```bash
  docker run ... --privileged ...
  ```

> See scripts in the 'extras' dir for more.