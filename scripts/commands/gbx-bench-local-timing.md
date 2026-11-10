# gbx:bench:local-timing

Time the forms of a levels set at three sizes on `local[8]` in the dev container. Exit 1 when a form does not scale with n, or (with `--baseline`) costs more per tile than it did.

The plain pass takes about 70 seconds on a 12-core arm64 laptop. `--baseline` and `--baseline-write` take about 2.5 minutes (2:24 and 2:38 measured): they time every n three times. Run it before a cluster IO round and after a change to `levels_forms._io_read`, `pyrx/tile_batches.py`, `pyrx/_mvp_forms.py`, the `raster_gbx` reader's batch read (`ds/raster.py`) or `open_tile.py`.

**Usage:** `bash scripts/commands/gbx-bench-local-timing.sh [OPTIONS]`

**Options:**
- `--set <name>`: the levels set to time. Default `IO`.
  - `IO`: the tile read forms.
  - `S`: the BNG scalar `gbx_bng_eastnorthasbng` forms `c0`, `c1`, `d` and `floor`, over generated points. There is no corpus copy. n is rows: 1,000,000, 4,000,000 and 16,000,000. `--corpus-dir` and `--corpus-src` are refused for it (exit 2).
  - `<family>`: a function family of the manifest (`raster-accessor`, `raster-stat`, `raster-tile-op`, `raster-file-op`, `raster-vectorize`, `raster-constructor`, `raster-generator`, `raster-array-op`, `raster-aggregate`, `web-tiles`, `cell-aggregate`, `grid-scalar`, `grid-array`, `grid-generator`, `vector-scalar`, `vector-aggregate`, `vector-generator`). One representative function at its JVM level (`l1@<input>`), each light variant and a floor last, over one input every form reads. A tile family reads the same 96-tile copy as `IO` at n = 4, 16 and 64; the others generate their input (n is rows). `--corpus-dir` applies to the tile families only.
- `SE-<function>`: a function of the GDAL spec engine, over replicated in-memory tiles: the row-wise UDF (`engine-c0`), the batch form (`engine-c1`) and a floor. `SE-contour` times `gbx_rst_contour` and `SE-summary` times `gbx_rst_summary`. Their outputs (features, JSON) have no batch form, so `engine-c1` runs the same UDF as `engine-c0`. `SE-fillnodata` times `gbx_rst_fillnodata`. `SE-viewshed` times `gbx_rst_viewshed` from an observer at the centre of each tile.
- `HV-<function>`: the engine forms of a warp, `fillnodata`, `summary` or `proximity`: `transform`, `to_webmercator`, `resample`, `resample_to_size`, `resample_to_res` (over the corpus tiles, n = 4, 8 and 16: 16 MiB tiles), and `fillnodata`, `summary` and `proximity` (over replicated in-memory tiles). None times a JVM companion. The forms are the engine's `engine-c0` and `engine-c1` (`c0` for proximity, `engine-c0` only for summary) and a floor. A corpus input of at most 64 tiles has no path or CRS on its tiles. The fingerprint and the resolution of `resample_to_res` use them. A check of the output needs more than 64 tiles.
- `--set ZL`: the Zarr size ladder, read legs only (see [the ZL set](#the-zl-set)).
- `--k <number>`: the multiple that flags a form. Default `3`.
- `--baseline [path]`: also flag a form whose cost per tile is over `--baseline-k` times the stored one. The default path is `test-logs/local-timing-baseline-<set>.json` (gitignored, per machine; `gbx:bench:clean` does not touch it).
- `--baseline-write`: store this pass's cost per tile as the baseline (at the `--baseline` path, or the default) instead of comparing. A pass that flags a form is not stored.
- `--baseline-k <number>`: the multiple of the baseline that flags a form. Default `1.5`.
- `--repeats <n>`: measured runs per n. The table, the flags and the baseline use their median. Default `1` for the plain pass and `3` with `--baseline` or `--baseline-write`. `--baseline-write` refuses fewer than 3 (exit 2).
- `--corpus-dir <path>`: the corpus to time. A folder named `rows` is read in place: nothing is copied, and the pass exits 2 if it holds fewer than 96 tiles. Any other path is the parent of the 96-tile copy. Pass `/Volumes/main/default/bench-corpus/rows` (10,000 tiles) to time the folder shape of a cluster run. The pass then also prints the `readdir_mode` the first tiles get and how long the one cold listing took. The listing cache is cleared first; the operating system's own cache is not. A set whose forms write under the corpus (the `_synth` folder) is refused with exit 2, as the folder is shared.
- Env `GBX_JAR=<path>`: start Spark with this JAR, not the newest of `python/geobrix/lib` and `target/`. Use it to time a JAR copied out of a tree that other work is rebuilding. The JAR must be the one the payload (`GBX_NATIVE_ROOT`) bundles, or the JVM forms are `na_by_design`.
- `--dry-run`: check the set's preconditions, print what they are and stop. It takes no lock and runs no pass.
- `--log <path>`: write the output to a log file (a filename goes to `test-logs/<name>`).
- `--no-wait`: exit 75 at once if another test run holds the container lock.
- `--help`: print the help.

**Run it once with the big folder before a cluster round.** The default pass copies 96 tiles to `/tmp`, a folder of 96 files. A worker's folder listing and the sidecar look-up it chooses cost nothing there. A cost that only shows in a folder of thousands of files, the one the cluster reads, never appears. `--corpus-dir /Volumes/main/default/bench-corpus/rows` reads the 10,000-file folder in place, with the same forms and n. Every form reads only the first n files, through the input builder's `filterRegex` (listed once per n). So the 10,000-file folder costs the pass one listing, not a task per file. The flags mean the same as on the 96-tile copy. A line `readdir_mode = None` in its output is a regression. The line says which: the open keeps the process value `TRUE` and stats every sidecar name, or the per-open value cannot be set in this process. A probe that crashes ends the pass with exit 3 and prints its stderr.

**What it does:**
- Runs with `GBX_NATIVE_PTH=0`. The image's startup `.pth` otherwise puts its own rasterio ahead of the payload's, while pyrx's per-open sidecar mode reaches the payload's libgdal. The two differ, and the mode is silently off: the timed forms never get the `EMPTY_DIR` or `TRUE` a cluster gives them.
- Starts Spark without the `GDAL_DISABLE_READDIR_ON_OPEN=TRUE` that importing pyrx sets in the driver. A JVM started with it hands it to the Python workers. There it reads as a value you set, which turns the per-file sidecar mode off. A value you export yourself is kept.
- Asks the module which artifacts the set's forms use (`--needs`), and checks only those. The IO set is Python only and needs neither the JAR nor the Rust payload. The S set needs the Rust payload for its `d` form (`GBX_NATIVE_ROOT`, default `build/env-wheel/env6/payload`). A set with an L1, L2 or JVM-DF form needs a built JAR. A missing one prints `PRECONDITION MISSING` and exits 2; so does a corpus with fewer than 96 tiles, a missing baseline, or a baseline of another set.
- Copies 96 tiles from `/Volumes/main/default/bench-corpus/rows` to `/tmp/gbx-local-timing/IO/rows` in the container, once.
- Runs forms `b`, `udf`, `d-py` and `floor` at n = 6, 24 and 96. A dropped warmup pass comes first, then `--repeats` measured runs of each n.
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

## The ZL set {#the-zl-set}

`--set ZL` times the Zarr ladder's read and write legs on `local[8]`. The write legs need the delta-spark jars of the Maven cache (`gbx:test:python` fetches them). It takes no corpus and needs neither the JAR nor the Rust payload.
- `--zarr-rungs <csv>`: rungs to run. Default: `10MB,50MB,100MB`. Do not generate rungs above 100 MB locally.
- `--zarr-families <csv>`: `county_wind`, `global_11km`. Default: both.
- `--zarr-cases <csv>`: `ZL-R1` full table, `ZL-R2` box over all time, `ZL-R3` 100 sites, `ZL-R4` wind speed and area mean, `ZL-R5` one hour as a tile.
  `ZL-W1` writes a table of cells straight to a manifest store (`gbx_zarr_write` with `dim_cols`).
  `ZL-W2` writes to a plain store, and `ZL-W3` is the baseline (tasks write the shards of staged blocks).
  `ZL-W4` and `ZL-W5` append a day of cells to the manifest store and the plain store.
  `ZL-W1s`, `ZL-W2s`, `ZL-W4s` and `ZL-W5s` are the same writes by the path that stages.
  That path packs the cells into blocks, stages them in a Delta table, and writes the blocks.
  `ZL-W6` is the blocks recipe.
  It reads u10 and v10 as blocks and computes the wind speed per block with a pandas UDF.
  It writes all four variables to a plain store (no staging).
  The write table has `path s` (the wall plus the pack and stage time) and `overhead (path)`. Default: all.
- `--zarr-reps <n>`: runs per leg, the first cold. Default: 3.
- `--zarr-cores <n>`: task slots of `local[N]`. Default: 8.
- `--zarr-root <dir>`: where the stores are. Default: `/tmp/gbx-local-timing/ZL` in the container.
- `--zarr-generate`: write the missing stores first (`gbx:bench:gen-data --zarr-ladder` does the same on the host).
- `--zarr-json-out <file>`: keep every leg's metrics.
- `--zarr-budget-mib <n>`: plan reads with this decoded unit budget. The planner packs chunks into tasks up to it: 64 MiB locally and on Serverless, 1536 MiB on a classic cluster.
- `--zarr-plan-only`: plan all eight rungs from stores that hold only their metadata (no data, no reads).
  It also prints the tasks at the 64 and 1536 MiB budgets.
- Output: a table per leg (wall, cold wall, s/GB, tasks, chunks and shards touched, planning, busy share, MB/s, read-back check) and the gates. A read-back check re-computes the values and compares sums.
- Gates (exit 1): time per GB within 1.5x of the 100 MB rung's from that rung up.
  Planning stays under 10% of the wall once it takes 0.25 s.
  Tasks must be at least `min(cores, chunks, MB / 8)`.
  One hour of a tile does not grow with the store, and no check fails. Below the 100 MB rung a fixed cost dominates, so the table gives a fit `wall = fixed + slope x GB` and the slope between rungs.

**Limits:**
- The sets are `IO`, `S` and `SE`. Add another set to `SETS` in `python/geobrix/src/databricks/labs/gbx/bench/local_timing.py`.
- Growth below linear is normal: a fixed scan and worker cost plus 8 cores keep t96/t24 near 1.4 to 3.
- Local ratios understate a cluster's: 8 local cores run the parallel forms about 4 times less parallel.
