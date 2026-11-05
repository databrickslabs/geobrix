# gbx:bench:uc-probes

Write the notebooks and statement files of the batched workspace pass into one folder, check them, and print the command of each run. It needs no login and calls no workspace.

**Usage:** `bash scripts/commands/gbx-bench-uc-probes.sh [OPTIONS]`

**Options:**
- `--catalog C` - catalog of the scratch schemas. Default `geospatial_docs`.
- `--schema S` - base name of the four scratch schemas `S_w`, `S_s6`, `S_s6j`, `S_se`. Default `geobrix_t28`.
- `--volume-dir DIR` - Volume folder that holds the env 6 wheel alone. Default `<volroot>/t28/uc-py`.
- `--jar PATH` - the JAR on a Volume, for the Scala forms. Default `<volroot>/geobrix/0.6.0/jar/geobrix-0.6.0-jar-with-dependencies.jar`.
- `--volroot DIR` - Volume root of the flat env wheels. Default `/Volumes/geospatial_docs/geobrix/sample-data`.
- `--out-dir DIR` - where the files go. Default `.superpowers/streamline/t28`.
- `--profile NAME` - profile in the printed commands. Default `oauth-fe`.
- `--ws-root DIR` - workspace folder in the printed commands. Default `/Users/mjohns@databricks.com/GeoBrix`.
- `--cluster-id ID` - the running JAR-less cluster of probe P1. Without it the two P1 notebooks are not written.
- `--only RUNS` - only these runs, comma-separated, any case. Default all.
- `--dry-run` - build and check everything. Write no file.
- `--parse` - also run Spark's SQL parser over each statement. It starts a local Spark and rejects Databricks-only DDL.
- `--judge FILE` - read a results file that `gbx-uc-sql` wrote and judge it: `w_slice`, `w_tierb`, `w_mech` or `w_engine_m6`.
- `--log <path>` - tee output to `test-logs/<path>`.
- `--help, -h` - show help.

**Examples:**
- `bash scripts/commands/gbx-bench-uc-probes.sh --dry-run`
- `bash scripts/commands/gbx-bench-uc-probes.sh --only S6-C,W-A --out-dir .superpowers/streamline/t28`
- `bash scripts/commands/gbx-bench-uc-probes.sh --judge .superpowers/streamline/t28/w_slice.json`

## What it writes

- Serverless notebooks: `stage_inputs`, `register_s6`, `uc_slice_s6j`, `uc_slice_s6`, `uc_perf`, `uc_proc_warm`, `mech_s6`, `m6_s6`, `register_s5`.
- Classic-cluster notebooks: `register_ded`, `mixed_lang_ded`, `register_std`, `temp_share_std`, `mixed_lang_std`, `p1_install`, `p1_after`.
- Warehouse files: `w_install`, `w_slice`, `w_tierb`, `w_m0` to `w_m13` (no `w_m9`, no `w_m11`), `w_mech`, `w_m9_calls`, `w_m9_idle`, `w_engine_m6`, `w_cleanup`.
- `manifest.json` and `manifest.md`: what each file is for, its runs, their preconditions and commands.

## Checks

- A notebook is JSON, its Python cells compile, and its probe call fits the probe's signature.
- A statement file parses as `gbx-uc-sql` reads it, with unique ids.
- Every `gbx_` routine in it is in the manifest or created earlier in the file, and is qualified to three levels.
- Every Python body in it compiles.

## Limits

- The scratch schemas are created by the first statement of each file. The printed commands pass no `--schema`.
- `w_cleanup.sql` drops the four scratch schemas with CASCADE. Run it last.
- Auth is the controller's job: run `bash ~/.claude/hooks/databricks-auth-status.sh PreDispatch` first.
