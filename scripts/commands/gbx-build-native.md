# gbx:build:native

Builds the GeoBrix native stack inside one pinned `manylinux_2_28` container (`scripts/native/build.Dockerfile`), for x86_64 and aarch64. The `toolchain` target is the build container itself: Python 3.12 build tools (cmake, ninja, Cython, numpy 2.1.3, maturin, auditwheel, patchelf), OpenJDK 17, SWIG, Ant, and the pinned Rust toolchain with `cargo-deny`. Later targets build on it: `stage-native` is GDAL, PROJ, GEOS and every library they need, built from source in the same container (below); `bindings` is the Python bindings built against it. Every version comes from `scripts/native/pins.conf`. Source tarballs, Python wheels, `rustup-init` and `cargo-deny` are fetched and checksum-checked on the host first (`scripts/native/prefetch.sh`), so the image build needs no package index. It does still download two things from public hosts while it builds: the JDK (dnf, from the base image's distribution repositories) and the Rust toolchain (`rustup toolchain install` from static.rust-lang.org, checked by rustup against a manifest it fetches itself). Rust crates are the part that is truly offline: they build with `--network none` from sources vendored on the host.

## Usage

```bash
bash scripts/commands/gbx-build-native.sh [--arch x86_64|aarch64|both] [--target toolchain|stage-native|bindings] [--no-cache] [--log <path>]
```

## Options

- `--arch x86_64|aarch64|both` - default: this machine's architecture. `both` builds aarch64 first, then x86_64. The architecture that is not the host's runs under emulation and is much slower.
- `--target toolchain|stage-native|bindings` - default `bindings`. A target whose stage is not in `build.Dockerfile` yet stops with `PRECONDITION MISSING`. `stage-native` and `bindings` build on the toolchain image `geobrix-native-build:toolchain-<hash>-<arch>`: it is built first when missing, and an existing one is used as it is; a change to the toolchain stage or to a file it copies changes `<hash>`, so the next build makes a new toolchain image.
- `--no-cache` - Rebuild every layer. A cold build's seconds are the full cost; a cached one reports only the layers that changed.
- `--log <path>` - Write output to a log file (filename goes to `test-logs/<name>`, relative paths go under `test-logs/`, absolute paths are used as-is)
- `--help` - Display help message

An option that needs a value given without one prints `<option> needs a value` and the usage, and exits 2.

## What `--target toolchain` checks

Per architecture, after the image is built as `geobrix-native-build:toolchain-<hash>-<arch>`, where `<hash>` is the first 12 characters of a sha256 over `pins.conf`, the toolchain stage of `build.Dockerfile` and the files that stage copies (so a toolchain edit, not only a pin change, makes a new tag):

1. The tools match the pins (rustc, maturin, numpy, SWIG, Ant, JDK 17, cargo-deny), and no pip or uv index configuration is in the image.
2. The Rust smoke crate (`scripts/native/rust-smoke`) builds into a `cp312-abi3-manylinux_2_28_<arch>` wheel with `--network none`, from sources vendored on the host (`scripts/native/cargo-vendor.sh`), and leaves no `target/` in the mounted crate; the wheel installs with `--no-index` and `smoke()` returns `ok`.
3. `cargo deny` passes the policy in `deny.toml`: licenses, bans and sources offline, advisories against the public advisory database. **Yanked crates are not checked here, by design:** a vendored source has no crates index, so the command allows the `index-failure` lint and prints a NOTE. The yanked check, and the full `advisories bans licenses sources` run, are the CI job `.github/workflows/rust-deny.yml` (`scripts/native/cargo-deny-ci.sh`): the pinned cargo-deny against every `Cargo.lock` in the repository on public crates.io, weekly and on every push and pull request. That job is the gate for a Rust crate.

## What `--target stage-native` builds and checks

Stages `deps` -> `gdal` -> `stage-native` of `build.Dockerfile`, then `export-stage-native` writes the tree to `build/native/out/<arch>/stage-native/gdal-native/`:

| Path | Holds |
|---|---|
| `lib/` | one file per SONAME (a wheel holds no symlinks), RUNPATH `$ORIGIN`: `libgdal`, `libgdalalljni.so` (the JNI), PROJ, GEOS (`libgeos_c`), netCDF, HDF5 and every library they need, including a private OpenSSL 4 under `libcurl`; `lib/pkgconfig`, `lib/cmake`. Every library except the JNI has a hashed auditwheel-style name, `<stem>-<sha256[:8]>.so<rest>` (`libgdal-71a8c415.so.39`), set as its SONAME and rewritten in every NEEDED, so it shares no SONAME with a system library: the loader reuses an already-loaded library of the same SONAME, which made GDAL run on the host's SQLite and curl, or fail to load, depending on what the process had loaded first. `MANIFEST.json` records the map (`sonames`: original -> hashed); look names up there, never hard-code them |
| `lib-dev/` | symlinks for linking only: for each library its original linker name (`libgdal.so`), its original SONAME and its hashed name, all pointing at `../lib/<hashed>`; a program linked through them records the hashed name. Never packaged |
| `bin/` | `gdalinfo`, `ogrinfo`, `projinfo` (RUNPATH `$ORIGIN/../lib`); `gdal-config`, `geos-config`, `nc-config` (prefix taken from the script's own location) |
| `include/` | the headers of GDAL, PROJ, GEOS, GeoTIFF, TIFF, HDF5, netCDF, curl, OpenSSL and the compression libraries |
| `share/{gdal,proj}` | GDAL and PROJ data. The tools find them relative to the executable, and PROJ relative to `libproj`; osgeo and the JNI do NOT find `share/gdal` on their own (`Cannot find osmconf.ini (GDAL_DATA is not defined)`), and GDAL fixes its data search path at the first lookup: osgeo needs `GDAL_DATA` in the environment before `import osgeo.gdal` (or `gdal.PushFinderLocation(<root>/share/gdal)`; `gdal.SetConfigOption('GDAL_DATA', ...)` after the import has no effect), the JNI needs `gdal.SetConfigOption("GDAL_DATA", ...)` before `gdal.AllRegister()` (and `PROJ_DATA` for PROJ unless it is found relative to `libproj`) |
| `java/gdal-<version>.jar` | the Java classes, from the same build as the JNI library |
| `python/{osgeo,osgeo_utils}` | the GDAL Python bindings (RUNPATH `$ORIGIN/../../lib`) |
| `licenses/<dep>/` | the licence files of every library built from source |
| `MANIFEST.json` | versions, native revision, arch, driver lists, build seconds per dependency, sizes, sha256 of every file |

Every dependency is built from the pinned source with the options of `scripts/native/build-deps.sh`; an option that CMake or `configure` ignores fails the build (`BUILD FAILED: <dep> ignored -D<name>`), so a renamed option cannot be dropped silently. Sources are bind-mounted from the build context and every build directory is a tmpfs (RAM), so the image layers hold only what was installed; each is capped at 6 GB of the Docker VM's RAM.

Per architecture, `scripts/native/verify-native.sh` runs in the toolchain image with the exported tree mounted read-only and `--network none`, and prints `ok: <check>` or `VERIFY FAILED: <what>`:

1. `gdalinfo --version` reports the pinned GDAL and `projinfo EPSG:27700` works, with no `GDAL_DATA`, `PROJ_DATA` or `LD_LIBRARY_PATH` set (the tools only).
2. Licence guard: no library or NEEDED entry matches `poppler|podofo|pdfium|rttopo|spatialite|mysql|mariadb|FileGDBAPI|ecw|ltidsdk|kdu|lura|grok|x265|odbc|clntsh|sfcgal|CGAL`; none of ECW, JP2ECW, MrSID, JP2MrSID, JP2KAK, JP2Lura, JP2Grok, FileGDB (the SDK driver), MySQL, ODBC, MSSQLSpatial, PGeo, OCI is registered; every `licenses/<dep>` is non-empty.
3. The 24 required drivers are registered (GTiff, COG, VRT, MEM, netCDF, HDF5, GRIB, JP2OpenJPEG, Zarr, PNG, JPEG, WEBP, MBTiles, GPKG, OpenFileGDB, ESRI Shapefile, GeoJSON, GeoJSONSeq, FlatGeobuf, MVT, PMTiles, CSV, KML, GML).
4. ELF closure: every ELF file has a RUNPATH `$ORIGIN`-relative to `lib/` and no RPATH, every NEEDED is in `lib/` or in the manylinux policy set, SONAMEs are unique and equal to the file names, every library but the JNI has a hashed SONAME that equals no library name in the image and matches the `sonames` map of `MANIFEST.json`; `auditwheel show` on a throwaway wheel of the tree reports consistency with `manylinux_2_28_<arch>` and no external shared libraries.
   Then `libgdal` is loaded after the host's `libsqlite3.so.0` and `libcurl.so.4` (what a Python process that imported `sqlite3` looks like): it must load, run on its own SQLite and curl, and not warn that GDAL runs against another curl.
5. JNI: `JniSmoke` loads `libgdalalljni.so` by absolute path and checks GDAL, OGR (OpenFileGDB) and OSR (EPSG:27700) from the Java classes.
6. osgeo: the version is the pinned one, `libgdal` is the one in `lib/`, an OpenFileGDB layer is written and read back, a CP1252 Shapefile is written and read back and a CP932 DBF decodes (iconv), the GRIB driver reports `HAVE_AEC=YES` (CCSDS), and `share/gdal` is found with `GDAL_DATA` in the environment or `PushFinderLocation`, and not without. `JniSmoke` also checks that `share/gdal` is found when `GDAL_DATA` is set before `AllRegister()`.
7. HTTPS: a local server with a throwaway CA, which the system trust store also trusts (control: the system curl accepts it). Our `/vsicurl/` read is refused without `GDAL_CURL_CA_BUNDLE` (no CA path is compiled into libcurl) and succeeds with it; the libcurl is built on OpenSSL 4 and no system libssl is loaded.
8. The dev kit: a C program built against `include/` + `lib-dev/` through `pkg-config`, through `gdal-config` and through CMake `find_package(GDAL)`, run with no library path.

To use the tree for a build: `export PKG_CONFIG_PATH=<root>/lib/pkgconfig LIBRARY_PATH=<root>/lib-dev GDAL_CONFIG=<root>/bin/gdal-config CMAKE_PREFIX_PATH=<root>`.

## Rust crates from other tasks

The vendor directory is made on the host; the container builds offline from it. The image sets `CARGO_TARGET_DIR=/tmp/target`, so cargo's `target/` stays inside the container and the mounted crate stays clean; pass `-e CARGO_TARGET_DIR=<dir>` (for example a cache volume) to move it.

```bash
bash scripts/native/cargo-vendor.sh <crate-dir> <out-dir>            # on the host
docker run --rm --network none -v <crate>:/src -v <out-dir>:/vendor:ro -v <out>:/out <toolchain image> \
  bash -lc 'cp /recipes/cargo-offline.toml $CARGO_HOME/config.toml && cd /src && \
            maturin build --release --offline --compatibility manylinux_2_28 -i /opt/python/cp312-cp312/bin/python --out /out'
```

## Environment

- `GBX_CARGO_REGISTRY` (untracked `.gbx-local.env` only) - a sparse crate registry used for `cargo vendor` before the default one. Never printed or committed. The container never uses it. A registry mirror can hide crate versions published in the last 7 days: pick pins at least a week old, or a lock made from the public index fails `cargo vendor --locked` through the mirror (the chain then falls through to the next source, the slowest being cargo's git index). A cargo config of the host that already replaces crates-io makes this source stop with "already defined"; the next source then runs under that config and produces the same vendor tree.
- The host's pip configuration supplies the package index for `pip download` in `prefetch.sh`. The command never reads, passes or prints it.

## Notes

- Needs Docker and nothing else: no dev container, no container lock. It never pushes or saves an image.
- Output goes to `build/native/` (gitignored): `src/`, `bin/<arch>/`, `wheelhouse/<arch>/`, `vendor/`, `ctx/<arch>/` (the build context), `out/<arch>/<target>/` (for `stage-native`: `gdal-native/`, `build.txt`, `verify.txt`).
- Disk: the images are large (3.3 GB toolchain, 3.5 GB stage-native, mostly shared layers) and the Docker VM can be full; the command never prunes images, caches or builders.
- The last line of a good run is `NATIVE_VERIFIED arch=<arch> target=<target> total_seconds=<n> image=<tag>`; the line before the verify is `NATIVE_BUILD ... build_seconds=<n> image_bytes=<n>`.
- Host Rust for vendoring is installed by `bash scripts/native/install-host-rust.sh` (the command runs it when missing) into `~/.cache/geobrix`; no shell profile is touched.
- If the emulated x86_64 build is too slow, the same command runs on GitHub Actions: `.github/workflows/build-gdal-native.yml` (manual trigger, on the org's hardened runner group).
