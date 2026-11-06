# gbx:dcs:build

Builds the GeoBrix Databricks Container Services (DCS) image for dedicated clusters. The image is DCS environment 6 plus the env6 wheel. The wheel's native payload is the image's one GDAL, PROJ and GEOS stack, with its bindings and the Java (JNI) library. The image also holds the wheel's declared and `[vizx]` dependencies from a hashed per-architecture lock, with the base image's packages held (click included). It holds GeoBrix's own rasterio build as a distribution. It holds exactly one GeoBrix JAR in `/databricks/jars`. No GDAL is compiled and the image build needs no package index. It builds the image locally, then verifies it; it never pushes (publishing is the release owner's `gbx:dcs:publish`).

Inputs:

- one `dist/env6/geobrix-<ver>+env6-py3-none-any.whl` (`gbx:build:wheel --env 6`). The image's JAR is the wheel's own `_jar/` copy. The copy is checked (it refuses a JAR that bundles classes the runtime provides: Scala, Spark or log4j) and staged mode 0644;
- `python/geobrix/envs/env6/dcs-<arch>.lock` and `dcs-base-<arch>.txt` (the committed lock and the base image's package list), made from `dcs.in`;
- the rasterio wheel `scripts/native` built for the architecture (`build/native/out/<arch>/bindings/dist/`), whose sha256 must be the lock's.

## Usage

```bash
bash scripts/commands/gbx-dcs-build.sh [--arch arm64|amd64] [--relock] [--log <path>]
```

## Options

- `--arch arm64|amd64` - `arm64` (default; Graviton, `databricksruntime/environment:v6-standard-arm`) or `amd64` (x86, `databricksruntime/environment:v6-standard`; emulated on an arm64 host)
- `--relock` - first regenerate `dcs-<arch>.lock` and `dcs-base-<arch>.txt` for the architecture. It runs `scripts/docker/dcs/dcs_lock.py relock`.
It takes the base image's package list, then runs `uv pip compile --generate-hashes`.
The base stays held, and the built rasterio wheel is offered as a local candidate. Needs `uv` and Docker. Run it after a change to `core.txt`, `extra-vizx.txt`, `dcs.in`, the rasterio pin or the rasterio wheel
- `--log <path>` - Write output to a log file (filename goes to `test-logs/<name>`, relative paths go under `test-logs/`, absolute paths are used as-is)
- `--help` - Display help message

An option that needs a value (`--arch`, `--log`) given without one prints `<option> needs a value` and the usage, and exits 2. The old `--target base|release` is gone: with no GDAL compile there is no base stage.

## Environment

- `GBX_DCS_REPO` - image repository (default `mjohns797/geobrix`). Use another name (for example `gbx-local/geobrix`) for a build that must not carry the release repository's name
- `PIP_INDEX_URL` - optional; the locked wheels are fetched on this machine through the package index pip is configured with (this variable, else pip's own configuration). It is never passed to the image build

## Examples

```bash
# Release image from the built env6 wheel and the committed lock (arm64)
bash scripts/commands/gbx-dcs-build.sh

# x86 image (emulated on an arm64 host), with a log
bash scripts/commands/gbx-dcs-build.sh --arch amd64 --log dcs-amd64.log

# Regenerate the lock first
bash scripts/commands/gbx-dcs-build.sh --relock
```

## Notes

- The build takes about a minute natively and a few minutes emulated: it installs binary wheels, nothing is compiled. The locked wheels beyond the base image (about 55) are cached under `build/dcs/wheelhouse/<arch>/` and bind-mounted for the install, so no layer of the image holds them.
- The image is built under a temporary local tag (`gbx-dcs-build-tmp:<pid>`) and every check runs against it. Only when all of them pass is it tagged `<repo>:<ver>-env6-<arm|x86>-rc` and the temporary tag removed. A failed check therefore leaves no new `-rc` tag and never moves an existing one. The failed image is removed with its temporary tag.
- The last two output lines are `IMAGE=<final tag>` and `IMAGE_ID=<sha256:...>`. Any `VERIFY FAILED` line means the image is not usable and was not tagged.
- Refusals (exit 1, before `docker build`) have these causes. `python3` is not on `PATH`, or there is not exactly one env6 wheel. The wheel holds no JAR or several JARs. A JAR's version is not the wheel's, or a JAR bundles classes the runtime provides. The lock is missing, or its rasterio line is not the built wheel's sha256 (relock). The lock moves a base package other than the declared idna move. Locked wheels cannot be fetched.
- Verify (`scripts/docker/dcs/verify_image.py`, run in the image with `--network none`; the script, the lock and the base list are mounted read-only, so none is in the image):
  - the payload's stack: every binding, `osgeo` and `osgeo_utils` load from the payload; GDAL, PROJ and GEOS equal `_build_info`. One libgdal is mapped and none from outside the payload; `light()` passes;
  - `/usr/lib/libgdalalljni.so` is the payload's JNI and loads (`ldd`: nothing `not found`, libgdal resolved into the payload). `/usr/local/hdf5/lib/plugin` is a link to the payload's HDF5 filter plugins. The JVM cannot register a plugin directory. The link is how the heavyweight tier reads zstd, blosc and bzip2 netCDF-4 with no cluster setting;
  - `osgeo` writes and reads a FileGDB;
  - the lock: every locked package is installed at its version, and every other base package is unchanged (click included). The `rasterio` distribution is GeoBrix's build. No package the build installed carries a `linux_<arch>` wheel tag (the base image's own `cryptography` and `PyYAML` do, and are not held to this);
  - OpenSSL 1.1 is gone: no `libssl`/`libcrypto` 1.x under site-packages. With `GBX_NATIVE_DISABLE=1` the installed rasterio distribution imports on the payload's GDAL (`ldd` of its `_base*.so`), and that GDAL links the payload's libssl. A process importing geopandas, contextily, rasterio and geobrix maps no OpenSSL 1.x library;
  - `[vizx]` imports, the `tippecanoe` executables exist, and the `[stac]` extra is absent.
  - Then the contents: exactly one GeoBrix JAR outside the wheel. Its sha256 equals the wheel's copy (and the one this command hashed). The JAR has mode 0644. The wheel version is installed.
- After the build the command checks `docker history` of the new image. It fails with `VERIFY FAILED` if a `PIP_INDEX_URL` value, or the configured index host, appears in the layers this build added. The image is meant for a public registry. It also fails with `VERIFY FAILED: could not isolate this build's layers from the base image` when it cannot tell which layers are this build's, rather than checking nothing and passing.
- Dockerfile: `scripts/docker/dcs/Dockerfile`. Lock tool: `scripts/docker/dcs/dcs_lock.py` (`relock`, `wheelhouse`, `check`, `moves`).
