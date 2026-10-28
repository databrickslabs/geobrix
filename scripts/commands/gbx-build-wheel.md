# gbx:build:wheel

Builds an env wheel, `geobrix-<version>+env5-py3-none-any.whl` (DBR 17.3, DBR 18, Serverless environment 5, Lakeflow CURRENT) or `geobrix-<version>+env6-py3-none-any.whl` (DBR 19, Serverless environment 6, SQL warehouses, UC functions). Each carries both architectures' native payload: `gdal-native` (GDAL, PROJ, GEOS, HDF5, netCDF and the libraries they need, the JNI) and rasterio, pyogrio, pyproj, shapely and netCDF4 built against it, plus the JAR, the tripwire extras, the runtime guard and the startup activation. The work is `scripts/native/build_env_wheel.py`, run inside `uv run --no-project --with-requirements python/geobrix/requirements-build.txt` (the hashed build requirements). The committed `python/geobrix/pyproject.toml` is not used: the env wheels have their own, `python/geobrix/envs/pyproject.env.toml` (ruling NP-3).

## Usage

```bash
bash scripts/commands/gbx-build-wheel.sh --env 5|6 [--jar <path>] [--extra-payload <dir>] [--log <path>]
```

## Options

- `--env 5|6` - the environment (required).
- `--jar <path>` - the GeoBrix JAR to ship in `_jar/` (default `target/geobrix-<version>-jar-with-dependencies.jar`). It must hold the Spark `DataSourceRegister` service file.
- `--extra-payload <dir>` - a tree merged into the payload, only under `_native/<arch>/` beside `lib/` (for the Rust kernels: `<dir>/_native/<arch>/rs/_gbxrs.abi3.so`). A file that is already in the payload fails the build. Repeatable.
- `--log <path>` - Write output to a log file (filename goes to `test-logs/<name>`, relative paths go under `test-logs/`, absolute paths are used as-is)
- `--help` - Display help message

An option that needs a value given without one prints `<option> needs a value` and the usage, and exits 2.

## Inputs

| Path | What |
|---|---|
| `build/native/out/<arch>/stage-native/gdal-native/` | `gbx:build:native --target stage-native`, both architectures: `lib/`, `share/`, `licenses/`, `MANIFEST.json` |
| `build/native/out/<arch>/bindings/` | `gbx:build:native --target bindings`: `vendor/` (the five bindings, no dist-info), `wheels/` (their metadata and licences), `build.txt` (which toolchain image built them) |
| `python/geobrix/envs/env<N>/core.txt` | the wheel's default dependencies: the runtime's preinstalled ranges, the light tier, and what the vendored packages need at run time |
| `python/geobrix/envs/env<N>/extra-<name>.txt` | one extra each (`[vizx]`, `[viewshed]`) |
| `python/geobrix/envs/env<N>/vendor-pure.txt` | rio-tiler, morecantile and mapbox-vector-tile, exact versions with their wheel's sha256. They require rasterio, pyproj or shapely by distribution name, which the vendored builds do not have, so they are vendored too. The command fetches them on the host through the configured pip (`--require-hashes`, every URL in the output redacted) into `build/env-wheel/vendor-pure/env<N>/`, and keeps files that already have the pinned sha256 |

## What the wheel holds

| Path in the wheel | Holds |
|---|---|
| `databricks/labs/gbx/_native/<arch>/lib/` | `gdal-native/lib/` without `pkgconfig/` and `cmake/`: one file per hashed SONAME, and `libgdalalljni.so` |
| `databricks/labs/gbx/_native/<arch>/MANIFEST.json` | the native build record (versions, the original -> hashed SONAME map). Library names come from it, never from a fixed name |
| `databricks/labs/gbx/_native/share/{gdal,proj}` | GDAL and PROJ data (the build fails if the architectures' differ) |
| `databricks/labs/gbx/_native/licenses/` | every native library's licence and the five bindings' (from their wheels' dist-info) |
| `databricks/labs/gbx/_vendor/<arch>/` | rasterio, pyogrio, pyproj, shapely, netCDF4 |
| `databricks/labs/gbx/_vendor/common/`, `_vendor/licenses/` | the vendor-pure.txt packages without their dist-info; their licences |
| `databricks/labs/gbx/_jar/geobrix-<version>-jar-with-dependencies.jar` | the JAR |
| `databricks/labs/gbx/_build_info.py` | `ENV`, `VERSION`, the GDAL/PROJ/GEOS versions, the native revision, the git sha and whether the tree was dirty |
| `geobrix-native.pth`, `_gbx_native_path.py` (site-packages root) | the startup activation |

`METADATA`: version `<version>+env<N>`; unmarked `Requires-Dist` = `core.txt`; one extra per `extra-*.txt`; and the tripwire: the env6 wheel declares `light_dbr17`, `light_dbr18`, `light_env5` and their `_all` forms as `geobrix==<version>+env5`, the env5 wheel `light_dbr19`, `light_env6` and their `_all` forms as `geobrix==<version>+env6`. An install that asks for the other environment's old name fails at resolution (pip: `ResolutionImpossible`); the same environment's old names are not declared (pip warns that the extra does not exist, and installs).

## At run time

- **Guard** (`databricks/labs/gbx/_env_guard.py`, at `import databricks.labs.gbx`): on Databricks, the env wheel of the other environment refuses to import, naming the file to install. Signals in order: `DATABRICKS_ENV_VERSION`, `DATABRICKS_RUNTIME_VERSION`, `/databricks/DBR_VERSION`, the protobuf major for an unparseable version. Off Databricks it never raises.
- **Activation** (`databricks/labs/gbx/_native/__init__.py`): on Linux x86_64 or aarch64, `_vendor/<arch>` and then `_vendor/common` go to the front of `sys.path`, so `import rasterio` (and the others) serves the payload's build. `GDAL_DATA` and `PROJ_DATA` point at `_native/share`, and the GDAL and PROJ CA bundles at the system store (else certifi's), each only where the user has set nothing; `CURL_CA_BUNDLE` is never set. It runs at interpreter start from `geobrix-native.pth` and again at `import databricks.labs.gbx`; `_native.activated_by()` says which came first (`"pth"` or `"import"`). `GBX_NATIVE_DISABLE=1` turns it off, `GBX_NATIVE_PTH=0` only the startup part, and `GBX_NATIVE_ROOT=<dir>` points it at a payload outside the package (a source checkout: `build/env-wheel/env<N>/payload`). Off Linux or on another architecture nothing is activated, and the env wheel has no bindings there.
- **The oracle** (`databricks.labs.gbx._native._selfcheck`): `stack()`, `light()`, `light_sql(spark)`, `heavy_sql(spark, scratch_dir)`, `install_paths(spark)`, `payload_size_mb()`, `installed_closure_mb()`.

## Steps and checks

Each step prints `== <n> ...`; a failure prints `BUILD FAILED: <what>`, a missing input `PRECONDITION MISSING: <what>`, and the command exits 1.

1. A scratch tree in `build/env-wheel/env<N>/tree/`: the package source, `envs/pyproject.env.toml` with one `optional-dependencies.<name>` line per extra and per tripwire name, `env/`, and `__version__` stamped `<version>+env<N>` (exactly one replacement). The template's `name`, `license`, `requires-python` and `authors` must equal the committed pyproject's.
2. `_build_info.py`; the two architectures' `MANIFEST.json` must agree on GDAL, PROJ, GEOS and the revision.
3. The payload. `lib/` must hold exactly the libraries `MANIFEST.json` names; no symlinks anywhere; `share/` and the licences must be identical for both architectures; every hashed library name a vendored binding records must be in that architecture's `lib/` (bindings built against another `gdal-native` fail here).
4. The vendored pure-Python wheels (sha256 against `vendor-pure.txt`, `py3-none-any` only). Every `Requires-Dist` of every vendored package (the five bindings' too) must be another vendored package at a version it accepts, a `core.txt` name, or `click`.
5. `python -m build --wheel --no-isolation` (log: `build/env-wheel/env<N>/build.log`).
6. `wheel unpack`, `geobrix-native.pth` (`import _gbx_native_path`) added, `wheel pack`.
7. The wheel against the env files (version, `Requires-Dist`, extras, tripwires, no same-environment old name), both architectures' libraries and vendored rasterio, the JAR, the `.pth`, `_build_info.py`, `proj.db`, no `pkgconfig`/`cmake`, no `__init__.py` inside the payload, no library name matching the licence guard of `scripts/native/verify-native.sh`, no host in `METADATA` other than the project's URLs, and the size (budget 100 MB; the wheels measured 80 MB). The payload is extracted to `build/env-wheel/env<N>/payload/`.
8. Per architecture, in the toolchain image that built the bindings (from `bindings/build.txt`), `--network none`: `auditwheel show` on a throwaway `cp312-cp312-linux_<arch>` wheel of `_native/<arch>` + `_vendor/<arch>` reports `manylinux_2_28_<arch>` and no external library.

Only then is the wheel moved to `dist/env<N>/`, which keeps no other wheel. The last line is `WHEEL_BUILT env=<N> wheel=<path> mb=<size> sha256=<sum> payload=<dir>`.

## Examples

```bash
bash scripts/commands/gbx-build-wheel.sh --env 6 --jar target/geobrix-0.6.0-jar-with-dependencies.jar
bash scripts/commands/gbx-build-wheel.sh --env 5 --extra-payload build/rs/payload --log env5-wheel.log
```
