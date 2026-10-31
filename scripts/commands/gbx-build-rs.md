# gbx:build:rs

Builds the shipped Rust module `_gbxrs` (`rust/gbxrs`) offline in the native build images, one build per architecture. Crates are vendored on the host first (`scripts/native/cargo-vendor.sh`, re-run when `Cargo.lock` is newer than the vendor tree).

## Usage

```bash
bash scripts/commands/gbx-build-rs.sh [--arch x86_64|aarch64|both] [--features gdal] [--check] [--log <path>]
```

## Options

- `--arch x86_64|aarch64|both` - default: this machine's architecture. The other one runs under emulation and is slow.
- `--features gdal` - default: none. Builds `decode_window` against the payload's libgdal, in the `rust-native` image with the `stage-native` dev kit (`scripts/native/build-rust-native.sh`, `GBX_CARGO_FEATURES=gdal`).
- `--check` - default: off. Builds nothing. Runs `cargo fmt --check`, `clippy -D warnings` and `cargo test` in the toolchain image, then `clippy --features gdal` and `cargo deny --all-features` (licenses, bans, sources, advisories) in the `rust-native` image. Advisories need the network.
- `--log <path>` - write the output to a log file (a filename goes to `test-logs/<name>`).
- `--help` - show the usage.

## Examples

```bash
bash scripts/commands/gbx-build-rs.sh --features gdal --log build-rs.log
bash scripts/commands/gbx-build-rs.sh --check
```

## Output

- No features: `build/rs/out/<arch>/gbxrs-*.whl` and `build/rs/payload/_native/<arch>/rs/_gbxrs.abi3.so` (manylinux_2_28, no GDAL).
- `--features gdal`: `build/rs/out-gdal/<arch>/payload/_native/<arch>/rs/_gbxrs.abi3.so`, with RUNPATH `$ORIGIN/../lib`.
- Pack either into an env wheel with `gbx:build:wheel --extra-payload <payload dir>`. Only one module per architecture may be packed.

## Limits

- A missing image or dev kit prints `PRECONDITION MISSING: ...` and exits 1. Build them with `gbx:build:native`.
- The build leaves no `target/` in the crate; the check is part of the command.
