# gbx:build:wheel

Builds an env wheel, `geobrix-<version>+env5-py3-none-any.whl` (DBR 17.3, DBR 18, Serverless environment 5, Lakeflow CURRENT) or `geobrix-<version>+env6-py3-none-any.whl` (DBR 19, Serverless environment 6, SQL warehouses, UC functions), or checks a built one's install. Each carries both architectures' native payload: `gdal-native` (GDAL, PROJ, GEOS, HDF5, netCDF and the libraries they need, the JNI), GDAL's own Python bindings from the same build (`osgeo`, `osgeo_utils`), and rasterio, pyogrio, pyproj, shapely and netCDF4 built against it, plus the JAR, the tripwire extras, the runtime guard and the startup activation. The env wheels require CPython 3.12 (`Requires-Python: >=3.12,<3.13`: the bindings are cp312 builds). The work is `scripts/native/build_env_wheel.py`, run inside `uv run --no-project --with-requirements python/geobrix/requirements-build.txt` (the hashed build requirements). The committed `python/geobrix/pyproject.toml` is not used: the env wheels have their own, `python/geobrix/envs/pyproject.env.toml`.

## Usage

```bash
bash scripts/commands/gbx-build-wheel.sh --env 5|6 [--jar <path>] [--extra-payload <dir>] [--allow-dirty] [--log <path>]
bash scripts/commands/gbx-build-wheel.sh --env 5|6 --verify x86_64|aarch64|both [--log <path>]
```

## Options

- `--env 5|6` - the environment (required).
- `--jar <path>` - the GeoBrix JAR to ship in `_jar/` (default `target/geobrix-<version>-jar-with-dependencies.jar`). It must hold the Spark `DataSourceRegister` service file.
- `--extra-payload <dir>` - a tree merged into the payload, only under `_native/<arch>/` beside `lib/` (for the Rust kernels: `<dir>/_native/<arch>/rs/_gbxrs.abi3.so`). A file that is already in the payload fails the build. Repeatable. The licence notices of each Rust kernel's crates (every package in its `Cargo.lock`, the crate found by its `[lib] name`) go to `_native/licenses/rust/<crate>/` with an `INDEX.txt`: from the crate's sources vendored to `build/native/vendor/<crate>` (`scripts/native/cargo-vendor.sh` runs when they are absent), or `scripts/native/rust-licenses/<crate>/` for a package that ships no licence file. A crate with none fails the build, and so does a kernel built against another gdal-native (a hashed NEEDED that `lib/` lacks).
- `--allow-dirty` - build although the inputs under git (`python/geobrix/src`, `python/geobrix/envs`, the README, the committed pyproject, `requirements-build.txt`, `scripts/native`) have uncommitted changes. Without it such a build is refused, so that the wheel's `GIT_SHA` names what it was built from; with it the wheel records `GIT_DIRTY = True`.
- `--verify x86_64|aarch64|both` - build nothing: check the install of `dist/env<N>/`'s wheel in that architecture's toolchain image (below). `both` runs aarch64, then x86_64 (emulated on an Apple Silicon host).
- `--log <path>` - Write output to a log file (filename goes to `test-logs/<name>`, relative paths go under `test-logs/`, absolute paths are used as-is)
- `--help` - Display help message

An option that needs a value given without one prints `<option> needs a value` and the usage, and exits 2. Docker, `uv` and a `python3` with `pip` are checked before anything runs (`PRECONDITION MISSING: ...`, exit 1).

## Inputs

| Path | What |
|---|---|
| `build/native/out/<arch>/stage-native/gdal-native/` | `gbx:build:native --target stage-native`, both architectures: `lib/`, `share/`, `licenses/`, `python/` (`osgeo`, `osgeo_utils`), `MANIFEST.json` |
| `build/native/out/<arch>/bindings/` | `gbx:build:native --target bindings`: `vendor/` (the five bindings, no dist-info), `wheels/` (their metadata and licences), `build.txt` (which toolchain image built them) |
| `python/geobrix/envs/env<N>/core.txt` | the wheel's default dependencies: the runtime's preinstalled ranges, the light tier, and what the vendored packages need at run time |
| `python/geobrix/envs/env<N>/base.txt` | what the runtimes preinstall (exact versions) of the packages core.txt bounds: checked against core.txt and the vendored packages' requirements, and the constraints of the `--verify` install |
| `python/geobrix/envs/env<N>/extra-<name>.txt` | one extra each (`[vizx]`) |
| `python/geobrix/envs/env<N>/vendor-pure.txt` | rio-tiler, morecantile and mapbox-vector-tile, exact versions with their wheel's sha256. They require rasterio, pyproj or shapely by distribution name, which the vendored builds do not have, so they are vendored too. The command fetches them on the host through the configured pip (`--require-hashes`) into `build/env-wheel/vendor-pure/env<N>/`, and keeps files that already have the pinned sha256 |

Every line pip, uv and the builder print goes through `scripts/native/redact.py`: URLs and host names (with or without a scheme) are replaced, because the configured index may be an internal one.

## What the wheel holds

| Path in the wheel | Holds |
|---|---|
| `databricks/labs/gbx/_native/<arch>/lib/` | `gdal-native/lib/` without `pkgconfig/` and `cmake/`: one file per hashed SONAME, and `libgdalalljni.so` |
| `databricks/labs/gbx/_native/<arch>/hdf5-plugins/` | `gdal-native/hdf5-plugins/`: netCDF-C's HDF5 filter plugins (zstd, blosc, bzip2, ...) |
| `databricks/labs/gbx/_native/<arch>/bin/projsync` | PROJ's grid downloader, executable (`crs_grids.projsync_path()` finds it) |
| `databricks/labs/gbx/_native/<arch>/MANIFEST.json` | the native build record (versions, the original -> hashed SONAME map, the PROJ grids, the build-id of every native file). Library names come from it, never from a fixed name |
| `databricks/labs/gbx/_native/share/{gdal,proj}` | GDAL and PROJ data, with the OSTN15, OSGM15 and EGM96 grids in `proj/` (the build fails if the architectures' differ) |
| `databricks/labs/gbx/_native/licenses/` | every native library's licence (one directory per library `MANIFEST.json` names, checked), `proj-grids/NOTICE.txt`, the five bindings' (from their wheels' dist-info), and `osgeo/` and `osgeo_utils/` (GDAL's licence); with a Rust kernel, `rust/<crate>/` (its crates' and the Rust standard library's notices) |
| `databricks/labs/gbx/_vendor/<arch>/` | rasterio, pyogrio, pyproj, shapely, netCDF4, and `osgeo` (gdal-native's SWIG modules, their RUNPATH set to `_native/<arch>/lib` by `set-rpath.py` in the architecture's toolchain image) |
| `databricks/labs/gbx/_vendor/common/`, `_vendor/licenses/` | the vendor-pure.txt packages without their dist-info, and `osgeo_utils` (identical for both architectures, or the build fails; without `gdal_merge.py`, which is LGPL-licensed, and `gdal2tiles.py`, which embeds JavaScript under other licences: both unused); their licences |
| `databricks/labs/gbx/_jar/geobrix-<version>-jar-with-dependencies.jar` | the JAR |
| `databricks/labs/gbx/_build_info.py` | `ENV`, `VERSION`, the GDAL/PROJ/GEOS versions, the native revision, the git sha, whether the tree was dirty, `SOURCE_DATE_EPOCH` |
| `geobrix-native.pth`, `_gbx_native_path.py` (site-packages root) | the startup activation |

`METADATA`: version `<version>+env<N>` (a pre-release such as `0.6.0rc1` keeps its suffix: `0.6.0rc1+env6`); `Requires-Python: >=3.12,<3.13`; unmarked `Requires-Dist` = `core.txt`; one extra per `extra-*.txt`; and the tripwire: the env6 wheel declares `light_dbr17`, `light_dbr18`, `light_env5` and their `_all` forms as `geobrix==<version>+env5`, the env5 wheel `light_dbr19`, `light_env6` and their `_all` forms as `geobrix==<version>+env6`. An install that asks for the other environment's old name fails at resolution (pip: `ResolutionImpossible`); the same environment's old names are not declared (pip warns that the extra does not exist, and installs).

The build is deterministic: setuptools and `wheel pack` run with `SOURCE_DATE_EPOCH` = HEAD's commit time, so every zip member carries that timestamp and one tree gives one sha256.

## At run time

- **Guard** (`databricks/labs/gbx/_env_guard.py`, at `import databricks.labs.gbx`): on Databricks, the env wheel of the other environment refuses to import, naming the file to install. Signals in order: `DATABRICKS_ENV_VERSION`, `DATABRICKS_RUNTIME_VERSION`, `/databricks/DBR_VERSION`, the protobuf major for an unparseable version. Off Databricks it never raises.
- **Activation** (one set of rules, `databricks/labs/gbx/_native/_rules.py`, applied at interpreter start by `geobrix-native.pth` and again at `import databricks.labs.gbx`; `_native.activated_by()` says which came first, `"pth"` or `"import"`):
  - only on Linux x86_64 or aarch64 and CPython 3.12; anywhere else nothing is activated, and the env wheel has no bindings there;
  - `_vendor/<arch>` and then `_vendor/common` go just before the first site-packages entry of `sys.path`: after the standard library, and ahead of every installed package, so `import rasterio` (and the others) serves the payload's build, whose libraries match its GDAL, even when another copy is installed. `GBX_NATIVE_DISABLE=1` turns the payload off (the installed copies are then used), `GBX_NATIVE_PTH=0` only the startup part, and `GBX_NATIVE_ROOT=<dir>` points it at a payload outside the package (a source checkout: `build/env-wheel/env<N>/payload`);
  - `GDAL_DATA` and `PROJ_DATA` point at `_native/share` when unset, and `PROJ_DATA` is not set over a user's `PROJ_LIB`;
  - `HDF5_PLUGIN_PATH` points at `_native/<arch>/hdf5-plugins` when unset, so netCDF-4 / HDF5 variables compressed with zstd, blosc or bzip2 read and write;
  - `GDAL_HTTP_VERSION` is never set: unset, the payload's curl offers HTTP/2 to an https server by ALPN (HTTP/1.1 where the server has no HTTP/2);
  - CA: a user's `GDAL_CURL_CA_BUNDLE` or `PROJ_CURL_CA_BUNDLE` is never touched; otherwise both are set to the user's `CURL_CA_BUNDLE`, else the user's `SSL_CERT_FILE`, else the system store (`/etc/ssl/certs/ca-certificates.crt`, `/etc/pki/tls/certs/ca-bundle.crt`, `/etc/ssl/cert.pem`), else certifi's (import-time only). This happens before any binding import, because rasterio and pyogrio set both variables to certifi's bundle at their own import when they are unset. `CURL_CA_BUNDLE` is never set.
- **The oracle** (`databricks.labs.gbx._native._selfcheck`): `stack()`, `light()`, `natives()` (the build options: netCDF filters, PROJ grids, JXL, REGEXP, muparser, JPEG SIMD, HTTP/2, projsync), `light_sql(spark)`, `heavy_sql(spark, scratch_dir)`, `install_paths(spark)`, `payload_size_mb()`, `installed_closure_mb()`.

## Steps and checks

Each step prints `== <n> ...`; a failure prints `BUILD FAILED: <what>`, a missing input `PRECONDITION MISSING: <what>`, and the command exits 1. Before step 1: the inputs, Docker and both toolchain images, and a clean tree (or `--allow-dirty`).

1. A scratch tree in `build/env-wheel/env<N>/tree/`: the package source, `envs/pyproject.env.toml` with one `optional-dependencies.<name>` line per extra and per tripwire name, `env/`, and `__version__` stamped `<version>+env<N>` (exactly one replacement). The template's `name`, `license` and `authors` must equal the committed pyproject's, and its `requires-python` must admit only Pythons the committed range admits.
2. `_build_info.py`; the two architectures' `MANIFEST.json` must agree on GDAL, PROJ, GEOS and the revision.
3. The payload. `lib/` must hold exactly the libraries `MANIFEST.json` names; no symlinks anywhere; `share/` and the licences must be identical for both architectures; every hashed library name a vendored binding records must be in that architecture's `lib/` (bindings built against another `gdal-native` fail here); no `osgeo` or `osgeo_utils` file may name a GPL-family licence.
4. The vendored pure-Python wheels (sha256 against `vendor-pure.txt`, `py3-none-any` only). Every `Requires-Dist` of every vendored package (the five bindings' too) must be another vendored package at a version it accepts, or a `core.txt` (or preinstalled `click`) package whose base version (`base.txt`) or `core.txt` floor it accepts and whose `core.txt` cap is no higher than its own. Every `base.txt` version must satisfy `core.txt`.
5. `python -m build --wheel --no-isolation` (log: `build/env-wheel/env<N>/build.log`).
6. `wheel unpack`, `geobrix-native.pth` (`import _gbx_native_path`) added, `wheel pack`.
7. The wheel against the env files (version, `Requires-Python`, `Requires-Dist`, extras, tripwires, no same-environment old name), both architectures' libraries, HDF5 filter plugins, executable `projsync` and vendored rasterio and `osgeo`, the PROJ grids `MANIFEST.json` names, a licence directory for every library it names, `osgeo_utils.gdal_calc`, the `osgeo` and `osgeo_utils` licences, no `gdal_merge.py` or `gdal2tiles.py`, the JAR, the `.pth`, `_build_info.py`, `_native/_rules.py`, `proj.db`, no `pkgconfig`/`cmake`, no `__init__.py` inside the payload, no library name matching the licence guard of `scripts/native/verify-native.sh`, no host in `METADATA` other than the project's URLs, every member stamped `SOURCE_DATE_EPOCH`, and the size (budget 100 MB; the wheels measured 97.0 MB with both architectures' payload, the PROJ grids and the HDF5 filter plugins). The payload is extracted to `build/env-wheel/env<N>/payload/`.
8. Per architecture, in the toolchain image that built the bindings (from `bindings/build.txt`), `--network none`: `auditwheel show` on a throwaway `cp312-cp312-linux_<arch>` wheel of `_native/<arch>` + `_vendor/<arch>` reports `manylinux_2_28_<arch>` and no external library.

Only then is the wheel moved to `dist/env<N>/`, which keeps no other wheel. The last line is `WHEEL_BUILT env=<N> wheel=<path> mb=<size> sha256=<sum> source_date_epoch=<t> git_dirty=<bool> payload=<dir>`.

## `--verify`: the install check

The core.txt wheelhouse for that architecture (with `base.txt` as constraints) is fetched on the host into `build/env-wheel/wheelhouse/env<N>/<arch>/`, kept while core.txt and base.txt are unchanged. Then, in the architecture's toolchain image (AlmaLinux 8, the manylinux_2_28 floor), `--network none`, `scripts/native/check-env-wheel.sh` prints `ok: <check>` or `CHECK FAILED: <what>` for:

1. a fresh CPython 3.12 venv installs the wheel offline, and the runtime's base versions do not move;
2. `import rasterio, shapely, pyproj, pyogrio, netCDF4, osgeo.gdal` with no geobrix import serves the payload's builds (the `.pth`), placed just before site-packages;
3. with `GBX_NATIVE_PTH=0`, `import databricks.labs.gbx` first activates the payload (`activated_by == "import"`);
4. a user `SSL_CERT_FILE` survives `import rasterio` and `pyogrio` for GDAL and PROJ; without one, the system store;
5. the tripwire: two of the other environment's old extra names fail with `ResolutionImpossible` naming the other wheel;
6. CPython 3.11: pip refuses the wheel, and a forced install (`--ignore-requires-python`) leaves the payload inactive;
7. the payload self-check: `_selfcheck.light()` passes, all five bindings and `osgeo` come from the payload, `osgeo.gdal_array` imports, `osgeo_utils.gdal_calc` imports from the payload, the process maps one libgdal (rasterio's and `osgeo`'s, same version), and no GDAL-stack library from outside the payload is mapped (the line `P8_RESULT {...}` has the numbers);
8. the build options: `_selfcheck.natives()` passes (netCDF-4 zstd, blosc and bzip2 variables written by netCDF4 read back the same through netCDF4 and GDAL; OSTN15 against Ordnance Survey's test points, OSGM15 and EGM96 heights both ways, PROJ network off; GTiff JXL lossless; REGEXP and `gdal raster calc --dialect=muparser`; libjpeg-turbo's SIMD dispatcher; curl with nghttp2 and `GDAL_HTTP_VERSION` unset; projsync, its CDN listing skipped here for want of a network) (the line `N5_RESULT {...}`).

The report is `build/env-wheel/env<N>/verify-<arch>.txt`; the last line is `ENV_WHEEL_VERIFIED env=<N> arch=<arch> ...`, and the command exits 1 on any failed check.

## Examples

```bash
bash scripts/commands/gbx-build-wheel.sh --env 6 --jar target/geobrix-0.6.0-jar-with-dependencies.jar
bash scripts/commands/gbx-build-wheel.sh --env 5 --extra-payload build/rs/payload --log env5-wheel.log
bash scripts/commands/gbx-build-wheel.sh --env 6 --verify aarch64
```
