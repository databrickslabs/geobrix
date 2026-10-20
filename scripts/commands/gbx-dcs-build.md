# gbx:dcs:build

Builds the GeoBrix Databricks Container Services (DCS) image for dedicated clusters: DCS environment 6 plus native GDAL 3.11.4 with its Java (JNI) bindings, the GeoBrix wheel and exactly one GeoBrix JAR in `/databricks/jars`. The release target expects one built `target/geobrix-<ver>-jar-with-dependencies.jar` (it refuses a JAR that bundles the Scala library) and one `python/geobrix/dist/geobrix-<ver>-py3-none-any.whl`, builds the image locally, then verifies its contents; it never pushes (publishing is the release owner's `gbx:dcs:publish`).

## Usage

```bash
bash scripts/commands/gbx-dcs-build.sh [--arch arm64|amd64] [--target base|release] [--log <path>]
```

## Options

- `--arch arm64|amd64` - `arm64` (default; Graviton, `databricksruntime/environment:v6-standard-arm`) or `amd64` (x86, `databricksruntime/environment:v6-standard`)
- `--target base|release` - `release` (default): base + wheel + JAR, tagged `<repo>:<ver>-env6-<arm|x86>-rc`. `base`: DCS env 6 + GDAL 3.11.4 only, tagged `<repo>:base-env6-<arm|x86>`
- `--log <path>` - Write output to a log file (filename goes to `test-logs/<name>`, relative paths go under `test-logs/`, absolute paths are used as-is)
- `--help` - Display help message

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
- The last two output lines are `IMAGE=<tag>` and `IMAGE_ID=<sha256:...>`. Any `VERIFY FAILED` line means the image is not usable.
- Refusals (exit 1, before `docker build`): not exactly one JAR or wheel, JAR/wheel version mismatch, or a JAR that bundles the Scala library. Rebuild the JAR with `gbx:data:stage-jar` (set `GBX_BUNDLE_SKIP_JAR_UPLOAD=1` to skip the upload).
- After the build the command checks `docker history` of the new image and fails with `VERIFY FAILED` if a `PIP_INDEX_URL` value, or the configured index host, appears in the layers this build added (the image is meant for a public registry).
- Dockerfile: `scripts/docker/dcs/Dockerfile`.
