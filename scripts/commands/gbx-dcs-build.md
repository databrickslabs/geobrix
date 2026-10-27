# gbx:dcs:build

Builds the GeoBrix Databricks Container Services (DCS) image for dedicated clusters: DCS environment 6 plus native GDAL 3.11.4 with its Java (JNI) bindings, the GeoBrix wheel and exactly one GeoBrix JAR in `/databricks/jars`. The release target expects one built `target/geobrix-<ver>-jar-with-dependencies.jar` (it refuses a JAR that bundles classes the runtime provides: Scala, Spark or log4j) and one `python/geobrix/dist/geobrix-<ver>-py3-none-any.whl`, builds the image locally, then verifies its contents; it never pushes (publishing is the release owner's `gbx:dcs:publish`).

## Usage

```bash
bash scripts/commands/gbx-dcs-build.sh [--arch arm64|amd64] [--target base|release] [--log <path>]
```

## Options

- `--arch arm64|amd64` - `arm64` (default; Graviton, `databricksruntime/environment:v6-standard-arm`) or `amd64` (x86, `databricksruntime/environment:v6-standard`)
- `--target base|release` - `release` (default): base + wheel + JAR, tagged `<repo>:<ver>-env6-<arm|x86>-rc`. `base`: DCS env 6 + GDAL 3.11.4 only, tagged `<repo>:base-env6-<arm|x86>`
- `--log <path>` - Write output to a log file (filename goes to `test-logs/<name>`, relative paths go under `test-logs/`, absolute paths are used as-is)
- `--help` - Display help message

An option that needs a value (`--arch`, `--target`, `--log`) given without one prints `<option> needs a value` and the usage, and exits 2.

## Environment

- `GBX_DCS_REPO` - image repository (default `mjohns797/geobrix`)
- `PIP_INDEX_URL` - optional package index for the wheel install (unset uses public PyPI). It reaches the build as a BuildKit secret, never as an ARG or ENV, so it is not recorded in the image history

## Examples

```bash
# Release image from the built JAR + wheel (arm64)
bash scripts/commands/gbx-dcs-build.sh

# GDAL-only base image, with a log
bash scripts/commands/gbx-dcs-build.sh --target base --log dcs-base.log
```

## Notes

- The GDAL compile takes roughly 15-30 minutes natively; run it in the background with `--log` and tail the log.
- The image is built under a temporary local tag (`gbx-dcs-build-tmp:<pid>`) and every check runs against it. Only when all of them pass is it tagged `<repo>:<ver>-env6-<arm|x86>-rc` (or `<repo>:base-env6-<arm|x86>`) and the temporary tag removed, so a failed check leaves no new `-rc` or `base` tag and never moves an existing one. The failed image is removed with its temporary tag.
- The last two output lines are `IMAGE=<final tag>` and `IMAGE_ID=<sha256:...>`. Any `VERIFY FAILED` line means the image is not usable and was not tagged.
- Refusals (exit 1, before `docker build`): `python3` not on `PATH` (the release target uses it to check and hash the JAR), not exactly one JAR or wheel, JAR/wheel version mismatch, or a JAR that bundles classes the runtime provides (Scala, Spark or log4j) or cannot be read. The JAR is copied into the build context first and the copy is what is checked and hashed, so it is the file the image gets. Rebuild the JAR with `gbx:data:stage-jar` (set `GBX_BUNDLE_SKIP_JAR_UPLOAD=1` to skip the upload).
- Ctrl-C or SIGTERM removes the build context, the package-index file (mode 0600) and the temporary tag, then exits 130 or 143, with or without `--log`. With `--log` a group signal (Ctrl-C, or a kill of the whole process group) also kills the `tee` behind the log, so the closing `Interrupted: ...` line may not reach the log or the terminal; the clean-up and the exit status do not depend on it (the script ignores SIGPIPE for this). A SIGTERM sent to the script alone is acted on when the running `docker` command returns; Ctrl-C reaches that command too.
- The first verify step runs `ldd` on `/usr/lib/libgdalalljni.so` and fails when any library it needs is `not found` (the file existing is not enough; the heavyweight JAR loads it).
- The in-image `pip install` runs with `-q`, so the build log does not carry pip's `Looking in indexes: ...` line. Its warnings and errors (for example `Could not fetch URL ...` on a failing build) are printed through `scripts/docker/dcs/redact_pip_log.py`, which replaces the index URL, its host and its credentials (taken from `PIP_INDEX_URL` inside the build, never printed) with `<package-index>`; the install's exit status still fails the build.
- The wheel is installed with `geobrix[light_env6,vizx]` (core plus visualization) under the base image's pinned packages. The release verify fails unless the visualization stack and the `tippecanoe` executables are present, `requests` is still the base image's version, and `pystac_client` (the `[stac]` extra) is absent.
- After the build the command checks `docker history` of the new image and fails with `VERIFY FAILED` if a `PIP_INDEX_URL` value, or the configured index host, appears in the layers this build added (the image is meant for a public registry). It also fails with `VERIFY FAILED: could not isolate this build's layers from the base image` when it cannot tell which layers are this build's, rather than checking nothing and passing.
- Dockerfile: `scripts/docker/dcs/Dockerfile`.
