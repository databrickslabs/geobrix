# gbx:native:verify-arches

The two-architecture native gate. CI builds, caches and tests the native stack on x86_64 only (`.github/workflows/build-gdal-native.yml`); this command checks both architectures' native builds and env wheels on a developer machine, aarch64 natively and x86_64 under emulation on an Apple Silicon Mac, and writes the stamp CI checks, `scripts/native/arch-gate.json`. It verifies what is already built and builds nothing.

## Usage

```bash
bash scripts/commands/gbx-native-verify-arches.sh [--log <path>]
```

## Options

- `--log <path>` - Write output to a log file (filename goes to `test-logs/<name>`, relative paths go under `test-logs/`, absolute paths are used as-is)
- `--help` - Display help message

## What it checks

Preconditions first (`PRECONDITION MISSING: <what>`, exit 1, nothing checked): Docker, both architectures' toolchain images (`geobrix-native-build:toolchain-<hash>-<arch>`), both `stage-native` trees with their debuginfo tarballs, and both env wheels (`dist/env5/`, `dist/env6/`). Then:

1. **Provenance** (`scripts/native/native_inputs.py provenance`): no native input has an uncommitted change (untracked files included: the checks read the checkout, and the stamp names HEAD; else `PRECONDITION MISSING`); each env wheel records a clean `GIT_SHA`, and the native inputs at that commit are HEAD's (a change only to the checks, `verify-native.sh`, `JniSmoke.java`, `check-env-wheel.sh` or `base_held.py`, needs no wheel rebuild: they are read from the checkout when they run); each `build/native/out/<arch>/stage-native/gdal-native/MANIFEST.json` is the one in both wheels. So the trees and wheels checked are one build, made from HEAD's inputs. A mismatch prints `GATE FAILED: <what>` with the rebuild to run, and the command stops. The stamp step refuses too when a native input changed during the run.
2. **`verify-native.sh`** (rows 1-18 of `gbx:build:native --target stage-native`) on each architecture's tree, in its toolchain image with `--network none`, as `gbx:build:native` runs it.
3. **`gbx:build:wheel --env 5 --verify <arch>` and `--env 6 --verify <arch>`** for each architecture (the install check of `check-env-wheel.sh`). Each runs on its own, so one failure does not hide the others; an older report is removed first, so a run that dies is counted as failed.
4. **The stamp**: `native_inputs.py write-stamp` counts each check's `ok:` lines (passed) and failure lines, plus a missing last line (`VERIFY OK arch=<arch>`, `ENV_WHEEL_VERIFIED env=<N> arch=<arch> failures=0`), as failed. With every count at 0 failed it writes `scripts/native/arch-gate.json`; otherwise it writes nothing, and the command ends with `ARCH_GATE_FAILED` and exit 1.

The logs are in `test-logs/arch-gate/` (`verify-native-<arch>.txt`, `env<N>-verify-<arch>.log`; the install reports stay at `build/env-wheel/env<N>/verify-<arch>.txt`). Each step prints `ARCH_GATE_STEP <check> arch=<arch> seconds=<n>`; the last line of a good run is `ARCH_GATE_OK stamp=scripts/native/arch-gate.json seconds=<n>`.

## The stamp, and CI's check

`scripts/native/arch-gate.json` holds the native-input hash of HEAD (`native_inputs.py hash --set gate`), each input file's sha256, the date, the GDAL version and native revision, the commit the wheels were built from, and per architecture the passed / failed counts of `verify_native`, `env5` and `env6`. Commit it.

The native inputs (`native_inputs.py files --set gate`) are the native build's inputs, which are also CI's cache key (`--set build`: the `KEY=VALUE` lines of `scripts/native/pins.conf`, so a comment edit moves nothing; `scripts/native/build.Dockerfile`; every recipe it copies or mounts; `rust-toolchain.toml`), plus exactly what the checks exercise: the env wheel's inputs as its builder lists them (`GIT_INPUTS` of `scripts/native/build_env_wheel.py`, read from the same commit: `python/geobrix/src`, `python/geobrix/envs`, `python/geobrix/pyproject.toml`, `python/geobrix/requirements-build.txt`, `scripts/native`; not `python/geobrix/README.md`, which no check reads), the checks among them (`verify-native.sh` with `JniSmoke.java`, `check-env-wheel.sh` with `base_held.py`), less the stamp itself. So any change to the package source or to `scripts/native` stales the stamp, as it changes the wheel the checks install. Not `GDALManager.scala`: no check here runs it. Only files under git count.

CI's job `arch-gate` (`.github/workflows/build_main.yml`) runs `python3 scripts/native/native_inputs.py check-stamp` on every push and pull request. It builds nothing, and fails when the checkout's hash is not the stamp's (naming the changed files) or the stamp records a failed check. The fix is to run this command and commit the stamp; its provenance check names what to rebuild first, if anything (`gbx:build:native --arch both` for a build input, then `gbx:build:wheel --env 5` and `--env 6`; a change only to the checks needs no rebuild).

## Notes

- Needs Docker and nothing else: no dev container and no container lock. It never pushes or saves an image.
- A whole run took 279 s on an Apple Silicon Mac (2026-10-29: verify-native 29 s aarch64, 77 s x86_64 under emulation; each `--verify` 32-53 s), plus the wheelhouse fetch when `core.txt` or `base.txt` changed.
- The release also runs the release smoke once on a temporary Graviton (aarch64) DBR 19 job cluster: the JVM side on aarch64, which no check here runs.

## Examples

```bash
bash scripts/commands/gbx-native-verify-arches.sh --log arch-gate.log
python3 scripts/native/native_inputs.py check-stamp   # what CI runs
```
