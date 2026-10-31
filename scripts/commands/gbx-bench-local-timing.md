# gbx:bench:local-timing

Time the forms of a levels set at three sizes on `local[8]` in the dev container. Exit 1 when a form does not scale with n.

The pass takes about 3 minutes. Run it before a cluster IO round and after a change to `levels_forms._io_read`, `pyrx/_mvp_forms.py` or `open_tile.py`.

**Usage:** `bash scripts/commands/gbx-bench-local-timing.sh [OPTIONS]`

**Options:**
- `--set <name>`: the levels set to time. Default `IO`. Only `IO` is implemented.
- `--k <number>`: the multiple that flags a form. Default `3`.
- `--log <path>`: write the output to a log file (a filename goes to `test-logs/<name>`).
- `--no-wait`: exit 75 at once if another test run holds the container lock.
- `--help`: print the help.

**What it does:**
- Checks for a built JAR and the Rust payload (`GBX_NATIVE_ROOT`, default `build/env-wheel/env6/payload`). A missing one prints `PRECONDITION MISSING` and exits 2.
- Copies 96 tiles from `/Volumes/main/default/bench-corpus/rows` to `/tmp/gbx-local-timing/IO/rows` in the container, once.
- Runs forms `b`, `c`, `udf`, `d-py`, `d-rs` and `floor` at n = 6, 24 and 96. A dropped warmup pass comes first, then one measured run each.
- Prints seconds per n, the ratio to `floor`, the ratio to `udf` and t96/t24.

**Flags (exit 1):**
- A form takes more than K times the row UDF at n=96.
- A form does not scale with n: its n=6 time is over 60% of its n=96 time.
- A form does not grow like n: t96/t24 is under 1.1 (flat) or over 6 (worse than linear).
- A form costs more than K times its peers' median per tile.
- A form has no time at some n because it failed or was skipped.

**Examples:**
- `bash scripts/commands/gbx-bench-local-timing.sh`
- `bash scripts/commands/gbx-bench-local-timing.sh --k 4 --log local-timing.log`

**Limits:**
- Only the `IO` set exists. Add another set to `SETS` in `python/geobrix/src/databricks/labs/gbx/bench/local_timing.py`.
- Growth below linear is normal: a fixed scan and worker cost plus 8 cores keep t96/t24 near 1.4 to 3.
- Local ratios understate a cluster's: 8 local cores run the parallel forms about 4 times less parallel.
