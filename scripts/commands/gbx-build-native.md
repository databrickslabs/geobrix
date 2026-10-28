# gbx:build:native

Builds the GeoBrix native stack inside one pinned `manylinux_2_28` container (`scripts/native/build.Dockerfile`), for x86_64 and aarch64. The `toolchain` target is the build container itself: Python 3.12 build tools (cmake, ninja, Cython, numpy 2.1.3, maturin, auditwheel, patchelf), OpenJDK 17, SWIG, Ant, and the pinned Rust toolchain with `cargo-deny`. Later targets (`stage-native`, `bindings`) build the GDAL stack and the Python bindings in it. Every version comes from `scripts/native/pins.conf`. Source tarballs, Python wheels, `rustup-init` and `cargo-deny` are fetched and checksum-checked on the host first (`scripts/native/prefetch.sh`), so the image build needs no package index. It does still download two things from public hosts while it builds: the JDK (dnf, from the base image's distribution repositories) and the Rust toolchain (`rustup toolchain install` from static.rust-lang.org, checked by rustup against a manifest it fetches itself). Rust crates are the part that is truly offline: they build with `--network none` from sources vendored on the host.

## Usage

```bash
bash scripts/commands/gbx-build-native.sh [--arch x86_64|aarch64|both] [--target toolchain|stage-native|bindings] [--no-cache] [--log <path>]
```

## Options

- `--arch x86_64|aarch64|both` - default: this machine's architecture. `both` builds aarch64 first, then x86_64. The architecture that is not the host's runs under emulation and is much slower.
- `--target toolchain|stage-native|bindings` - default `bindings`. A target whose stage is not in `build.Dockerfile` yet stops with `PRECONDITION MISSING`.
- `--no-cache` - Rebuild every layer. A cold build's seconds are the full cost; a cached one reports only the layers that changed.
- `--log <path>` - Write output to a log file (filename goes to `test-logs/<name>`, relative paths go under `test-logs/`, absolute paths are used as-is)
- `--help` - Display help message

An option that needs a value given without one prints `<option> needs a value` and the usage, and exits 2.

## What `--target toolchain` checks

Per architecture, after the image is built as `geobrix-native-build:toolchain-<sha256(pins.conf)[:12]>-<arch>`:

1. The tools match the pins (rustc, maturin, numpy, SWIG, Ant, JDK 17, cargo-deny), and no pip or uv index configuration is in the image.
2. The Rust smoke crate (`scripts/native/rust-smoke`) builds into a `cp312-abi3-manylinux_2_28_<arch>` wheel with `--network none`, from sources vendored on the host (`scripts/native/cargo-vendor.sh`), and leaves no `target/` in the mounted crate; the wheel installs with `--no-index` and `smoke()` returns `ok`.
3. `cargo deny` passes the policy in `deny.toml`: licenses, bans and sources offline, advisories against the public advisory database. **Yanked crates are not checked here, by design:** a vendored source has no crates index, so the command allows the `index-failure` lint and prints a NOTE. The yanked check, and the full `advisories bans licenses sources` run, are the CI job `.github/workflows/rust-deny.yml` (`scripts/native/cargo-deny-ci.sh`): the pinned cargo-deny against every `Cargo.lock` in the repository on public crates.io, weekly and on every push and pull request. That job is the gate for a Rust crate.

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
- Output goes to `build/native/` (gitignored): `src/`, `bin/<arch>/`, `wheelhouse/<arch>/`, `vendor/`, `ctx/<arch>/` (the build context), `out/<arch>/<target>/`.
- The last line of a good run is `NATIVE_VERIFIED arch=<arch> target=<target> total_seconds=<n> image=<tag>`; the line before the verify is `NATIVE_BUILD ... build_seconds=<n> image_bytes=<n>`.
- Host Rust for vendoring is installed by `bash scripts/native/install-host-rust.sh` (the command runs it when missing) into `~/.cache/geobrix`; no shell profile is touched.
- If the emulated x86_64 build is too slow, the same command runs on GitHub Actions: `.github/workflows/build-gdal-native.yml` (manual trigger, on the org's hardened runner group).
