# gbx:bench:local-timing

Time the forms of a levels set at three sizes on `local[8]` in the dev container. Exit 1 when a form does not scale with n, or (with `--baseline`) costs more per tile than it did.

The plain pass takes about 70 seconds on a 12-core arm64 laptop. `--baseline` and `--baseline-write` take about 2.5 minutes (2:24 and 2:38 measured): they time every n three times. Run it before a cluster IO round and after a change to `levels_forms._io_read`, `pyrx/_mvp_forms.py` or `open_tile.py`.

**Usage:** `bash scripts/commands/gbx-bench-local-timing.sh [OPTIONS]`

**Options:**
- `--set <name>`: the levels set to time. Default `IO`. Sets: `IO` (the tile read forms) and `S` (the BNG scalar `gbx_bng_eastnorthasbng` forms `c0`, `c1`, `d`, `floor` over generated points; no corpus copy, n is rows: 1,000,000, 4,000,000, 16,000,000; `--corpus-dir` and `--corpus-src` are refused for it with exit 2).
- `--k <number>`: the multiple that flags a form. Default `3`.
- `--baseline [path]`: also flag a form whose cost per tile is over `--baseline-k` times the stored one. The default path is `test-logs/local-timing-baseline-<set>.json` (gitignored, per machine; `gbx:bench:clean` does not touch it).
- `--baseline-write`: store this pass's cost per tile as the baseline (at the `--baseline` path, or the default) instead of comparing. A pass that flags a form is not stored.
- `--baseline-k <number>`: the multiple of the baseline that flags a form. Default `1.5`.
- `--repeats <n>`: measured runs per n. The table, the flags and the baseline use their median. Default `1` for the plain pass and `3` with `--baseline` or `--baseline-write`. `--baseline-write` refuses fewer than 3 (exit 2).
- `--corpus-dir <path>`: the corpus to time. A folder named `rows` is read in place: nothing is copied, and the pass exits 2 if it holds fewer than 96 tiles. Any other path is the parent of the 96-tile copy. Pass `/Volumes/main/default/bench-corpus/rows` (10,000 tiles) to time the folder shape of a cluster run. The pass then also prints the `readdir_mode` the first tiles get and how long the one cold listing took. The listing cache is cleared first; the operating system's own cache is not. A set whose forms write under the corpus (the `_synth` folder) is refused with exit 2, as the folder is shared.
- `--log <path>`: write the output to a log file (a filename goes to `test-logs/<name>`).
- `--no-wait`: exit 75 at once if another test run holds the container lock.
- `--help`: print the help.

**Run it once with the big folder before a cluster round.** The default pass copies 96 tiles to `/tmp`, a folder of 96 files. A worker's folder listing and the sidecar look-up it chooses cost nothing there. A cost that only shows in a folder of thousands of files, the one the cluster reads, never appears. `--corpus-dir /Volumes/main/default/bench-corpus/rows` reads the 10,000-file folder in place, with the same forms and n. Every form reads only the first n files, through the input builder's `filterRegex` (listed once per n). So the 10,000-file folder costs the pass one listing, not a task per file. The flags mean the same as on the 96-tile copy. A line `readdir_mode = None` in its output is a regression. The line says which: the open keeps the process value `TRUE` and stats every sidecar name, or the per-open value cannot be set in this process. A probe that crashes ends the pass with exit 3 and prints its stderr.

**What it does:**
- Runs with `GBX_NATIVE_PTH=0`. The image's startup `.pth` otherwise puts its own rasterio ahead of the payload's, while pyrx's per-open sidecar mode reaches the payload's libgdal. The two differ, and the mode is silently off: the timed forms never get the `EMPTY_DIR` or `TRUE` a cluster gives them.
- Starts Spark without the `GDAL_DISABLE_READDIR_ON_OPEN=TRUE` that importing pyrx sets in the driver. A JVM started with it hands it to the Python workers. There it reads as a value you set, which turns the per-file sidecar mode off. A value you export yourself is kept.
- Checks for a built JAR and the Rust payload (`GBX_NATIVE_ROOT`, default `build/env-wheel/env6/payload`). A missing one prints `PRECONDITION MISSING` and exits 2; so does a corpus with fewer than 96 tiles, a missing baseline, or a baseline of another set.
- Copies 96 tiles from `/Volumes/main/default/bench-corpus/rows` to `/tmp/gbx-local-timing/IO/rows` in the container, once.
- Runs forms `b`, `c`, `udf`, `d-py`, `d-rs` and `floor` at n = 6, 24 and 96. A dropped warmup pass comes first, then `--repeats` measured runs of each n.
- Prints the median seconds per n, the ratio to `floor`, the ratio to `udf` and t96/t24. With more than one run it also prints each form's cost per tile in every run and the max/min spread.

**Exit codes:** `0` no flags, `1` the pass flagged a form, `2` a precondition is missing (or a usage error), `3` the pass itself failed.

**Flags (exit 1):**
- A form takes more than K times the row UDF at n=96.
- A form does not scale with n: its n=6 time is over 60% of its n=96 time.
- A form does not grow like n: t96/t24 is under 1.1 (flat) or over 6 (worse than linear).
- A form costs more than K times its peers' median per tile.
- A form has no time at some n because it failed, was skipped or timed 0 s.
- With `--baseline`: a form costs more than `--baseline-k` times its stored cost per tile, is missing from the baseline, or is in it but not timed now.

**The baseline.**
- The ratio rules compare a form with the row UDF and its peers. They need a 3x change, so a form whose own cost per tile doubled passes them.
- The baseline stores each form's cost per tile in ms. Each run gives the slope between n=24 and n=96, less the floor's slope. The baseline is the median of those slopes over the runs, and a comparison uses the median of its own runs.
- It is stored with the machine (cores, arch) and the git SHA. A baseline from another machine prints a note.
- Write it after a change you trust. Compare against it before a cluster round.
- One measured run per n is noisy. In two single runs on one machine, `d-py`'s cost per tile differed by 1.6x (36 vs 22 ms). Inside one 3-run pass the spread of a form was 1.2x to 1.9x, so the median of 3 is what is stored and compared.
- A baseline from a slow run hides a regression. A baseline from a fast run flags a healthy form.
- Re-run before you trust a baseline flag. Write the baseline again if it came from the odd run.

**Examples:**
- `bash scripts/commands/gbx-bench-local-timing.sh`
- `bash scripts/commands/gbx-bench-local-timing.sh --k 4 --log local-timing.log`
- `bash scripts/commands/gbx-bench-local-timing.sh --baseline-write`
- `bash scripts/commands/gbx-bench-local-timing.sh --baseline`
- `bash scripts/commands/gbx-bench-local-timing.sh --corpus-dir /Volumes/main/default/bench-corpus/rows` (the big-folder shape; once before a cluster round)

**Limits:**
- Only `IO` and `S` exist. Add another set to `SETS` in `python/geobrix/src/databricks/labs/gbx/bench/local_timing.py`.
- Growth below linear is normal: a fixed scan and worker cost plus 8 cores keep t96/t24 near 1.4 to 3.
- Local ratios understate a cluster's: 8 local cores run the parallel forms about 4 times less parallel.
