# gbx:bench:local-timing

Time the forms of a levels set at three sizes on `local[8]` in the dev container. Exit 1 when a form does not scale with n, or (with `--baseline`) costs more per tile than it did.

The pass takes about 3 minutes. Run it before a cluster IO round and after a change to `levels_forms._io_read`, `pyrx/_mvp_forms.py` or `open_tile.py`.

**Usage:** `bash scripts/commands/gbx-bench-local-timing.sh [OPTIONS]`

**Options:**
- `--set <name>`: the levels set to time. Default `IO`. Only `IO` is implemented.
- `--k <number>`: the multiple that flags a form. Default `3`.
- `--baseline [path]`: also flag a form whose cost per tile is over `--baseline-k` times the stored one. The default path is `test-logs/local-timing-baseline-<set>.json` (gitignored, per machine; `gbx:bench:clean` does not touch it).
- `--baseline-write`: store this pass's cost per tile as the baseline (at the `--baseline` path, or the default) instead of comparing. A pass that flags a form is not stored.
- `--baseline-k <number>`: the multiple of the baseline that flags a form. Default `1.5`.
- `--log <path>`: write the output to a log file (a filename goes to `test-logs/<name>`).
- `--no-wait`: exit 75 at once if another test run holds the container lock.
- `--help`: print the help.

**What it does:**
- Checks for a built JAR and the Rust payload (`GBX_NATIVE_ROOT`, default `build/env-wheel/env6/payload`). A missing one prints `PRECONDITION MISSING` and exits 2; so does a corpus with fewer than 96 tiles, a missing baseline, or a baseline of another set.
- Copies 96 tiles from `/Volumes/main/default/bench-corpus/rows` to `/tmp/gbx-local-timing/IO/rows` in the container, once.
- Runs forms `b`, `c`, `udf`, `d-py`, `d-rs` and `floor` at n = 6, 24 and 96. A dropped warmup pass comes first, then one measured run each.
- Prints seconds per n, the ratio to `floor`, the ratio to `udf` and t96/t24.

**Exit codes:** `0` no flags, `1` the pass flagged a form, `2` a precondition is missing (or a usage error), `3` the pass itself failed.

**Flags (exit 1):**
- A form takes more than K times the row UDF at n=96.
- A form does not scale with n: its n=6 time is over 60% of its n=96 time.
- A form does not grow like n: t96/t24 is under 1.1 (flat) or over 6 (worse than linear).
- A form costs more than K times its peers' median per tile.
- A form has no time at some n because it failed, was skipped or timed 0 s.
- With `--baseline`: a form costs more than `--baseline-k` times its stored cost per tile, is missing from the baseline, or is in it but not timed now.

**The baseline.** The ratio rules compare a form with the row UDF and its peers, so they need a 3x change; a form whose own cost per tile doubled (same fixed cost) passes them. The baseline is each form's cost per tile: the slope of its time between n=24 and n=96, less the floor's slope, in ms, stored with the machine (cores, arch) and git SHA. Write it after a change you trust, compare before a cluster round. A baseline from another machine prints a note. One measured run per n is noisy: in two runs on the same machine `d-py`'s cost per tile differed by 1.6x (36 vs 22 ms), the other forms by 1.1x to 1.2x. A baseline taken on an unlucky run makes the next run look fast, and one taken on a lucky run flags a healthy form; re-run before trusting a baseline flag, and write the baseline again if it was the odd run.

**Examples:**
- `bash scripts/commands/gbx-bench-local-timing.sh`
- `bash scripts/commands/gbx-bench-local-timing.sh --k 4 --log local-timing.log`
- `bash scripts/commands/gbx-bench-local-timing.sh --baseline-write`
- `bash scripts/commands/gbx-bench-local-timing.sh --baseline`

**Limits:**
- Only the `IO` set exists. Add another set to `SETS` in `python/geobrix/src/databricks/labs/gbx/bench/local_timing.py`.
- Growth below linear is normal: a fixed scan and worker cost plus 8 cores keep t96/t24 near 1.4 to 3.
- Local ratios understate a cluster's: 8 local cores run the parallel forms about 4 times less parallel.
