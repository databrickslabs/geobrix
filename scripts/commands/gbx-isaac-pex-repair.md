# gbx:isaac:pex-repair

Repair a broken Isaac Review (`/review`) install when its `dbexec` pex cache resolved a
**wrong-architecture `cryptography` wheel** — e.g. a Linux x86-64 wheel landed on an arm64
Mac, so `isaac` crashes at startup with
`ImportError: dlopen(.../_rust.abi3.so ...): slice is not valid mach-o file`.

The fix is surgical and reversible: `cryptography` ships a stable-ABI (`abi3`) wheel, so the
only platform-specific artifact is `cryptography/hazmat/bindings/_rust.abi3.so`. This command
detects the mis-resolved wheel(s) under `~/.pex/installed_wheels/`, downloads the correct
wheel for this host from the Databricks PyPI proxy, backs up the bad `.so` to `.bad.bak`, and
swaps in the right one. A full pex rebuild can reproduce the mis-resolve, so keep this handy.

## Usage

```bash
bash scripts/commands/gbx-isaac-pex-repair.sh [OPTIONS]
```

## Options

- `--check` — Diagnose only: report any wrong-arch `cryptography` `.so` and exit (no changes).
- `--log <path>` — Tee output to a log (`filename` → `test-logs/<name>`, relative → under
  `test-logs/`, absolute → as-is).
- `-h`, `--help` — Show help.

Exit codes: `0` healthy or repaired, `1` still broken after repair, `2` environment error
(no `dbexec`/`pip`, no pex cache), `3` `--check` found a problem (nothing changed).

## Examples

```bash
# Diagnose without touching anything:
bash scripts/commands/gbx-isaac-pex-repair.sh --check

# Repair, then confirm /review loads:
bash scripts/commands/gbx-isaac-pex-repair.sh --log isaac-pex-repair.log
```
