#!/usr/bin/env python3
"""
Generate the benchmark row pool (rows/r<N>.tif) on Databricks Serverless, byte-identical to the
local `gbx:bench:gen-data` tiles, and write it to the corpus Volume.

Reached through `gbx:bench:gen-data --on-databricks`. It reuses the bench launcher's plumbing: the env
wheel staged by `gbx:data:stage-wheel --env <N> --versioned`, the Serverless environment spec that carries it
to the workers, the same workspace upload, `submit_once`, and a SMOKE stage before the real run.

  1. Smoke: the workers import geobrix and make the same bytes as this host for one tile per SRID
     (no Volume I/O). A mismatch stops the launch (exit 4); nothing else is submitted.
  2. Run: a DataFrame of tile indices repartitioned by column (the only Serverless parallelism), each task
     writing its tiles through the Volume FUSE mount; tiles already on the Volume at the expected size are
     skipped; nothing is deleted. The notebook prints `[GENCORPUS]` progress lines; this launcher shows the
     latest every poll interval from `jobs export-run` and writes a status file.
  3. Verify: the count of complete tiles and the sha256 of a sample, against the local corpus files.

Options: --profile <name> (required), --rows-from/--rows-to (both included; default 0..9999),
--skip-existing/--no-skip-existing (default skip), --partitions, --chunk, --verify-indices, --out,
--local-corpus, --env-version, --run-id, --no-smoke, --smoke-only, --timeout-min, --stale-min,
--status-file, --seed, --srids, --row-tile-px, --row-bands, --row-dtype, --poll-secs, --no-wait.
Exit codes: 0 verified, 1 verification failed, 2 usage, 3 stalled, 4 smoke failed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

from _cli import exit_if_help  # sibling module (script dir is on sys.path)
from _job_submit import submit_once  # sibling module
import _preflight  # sibling module
from _preflight import serverless_dependencies

import push_and_run_bench_on_cluster as bench  # sibling module: wheel lookup, output save, notebook removal

REPO = Path(__file__).resolve().parents[2]
# The command runs this launcher from a snapshot of the code; the local corpus, the built wheel and the logs
# are the source tree's.
ROOT = Path(os.environ.get("GBX_SOURCE_ROOT") or REPO)
sys.path.insert(0, str(REPO / "python" / "geobrix" / "src"))

EXIT_FAILED, EXIT_USAGE, EXIT_STALLED, EXIT_SMOKE = 1, 2, 3, 4


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="gen-data --on-databricks", add_help=False)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--rows-from", type=int, default=0)
    ap.add_argument("--rows-to", type=int, default=9999)
    ap.add_argument("--skip-existing", dest="skip_existing", action="store_true", default=True)
    ap.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    ap.add_argument("--partitions", type=int, default=64)
    ap.add_argument("--chunk", type=int, default=512)
    ap.add_argument("--verify-indices", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--local-corpus", default=str(ROOT / "sample-data/Volumes/main/default/bench-corpus"))
    ap.add_argument("--env-version", default="6")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--no-smoke", action="store_true")
    ap.add_argument("--smoke-only", action="store_true")
    ap.add_argument("--timeout-min", type=int, default=240)
    ap.add_argument("--smoke-timeout-min", type=int, default=20)
    ap.add_argument("--stale-min", type=float, default=20.0)
    ap.add_argument("--poll-secs", type=float, default=20.0)
    ap.add_argument("--progress-secs", type=float, default=60.0)
    ap.add_argument("--status-file", default="")
    ap.add_argument("--no-wait", action="store_true")
    ap.add_argument("--seed", type=int, default=1234)
    ap.add_argument("--srids", default="4326,3857,32618,27700")
    ap.add_argument("--row-tile-px", type=int, default=1024)
    ap.add_argument("--row-bands", type=int, default=4)
    ap.add_argument("--row-dtype", default="float32")
    return ap


def _local_sha(local_corpus: Path, indices, spec, dr) -> dict:
    """sha256 per index of the LOCAL tile: the file under *local_corpus*/rows, else regenerated here."""
    import hashlib

    out = {}
    for j in indices:
        p = local_corpus / "rows" / f"r{j}.tif"
        data = p.read_bytes() if p.is_file() else dr.row_tile_bytes(j, spec)
        out[str(j)] = hashlib.sha256(data).hexdigest()
    return out


def _task_run_id(run) -> int:
    tasks = getattr(run, "tasks", None) or []
    return tasks[0].run_id if tasks else run.run_id


def _life_result(run):
    st = run.state

    def val(x):
        return x.value if x is not None and hasattr(x, "value") else (str(x) if x is not None else "")

    return (val(st.life_cycle_state) if st else "?"), (val(st.result_state) if st else "")


def _export_text(w, run) -> str:
    from databricks.sdk.service.jobs import ViewsToExport

    views = w.jobs.export_run(run_id=_task_run_id(run), views_to_export=ViewsToExport.CODE).views or []
    return "\n".join(v.content or "" for v in views)  # HTML; dr.exported_text decodes its notebook model


def _write_status(path, run_id, state, elapsed_s, line, extra=""):
    if not path:
        return
    Path(path).write_text(
        "# task-17d status\n\n"
        f"- run id: {run_id}\n- state: {state}\n- elapsed: {elapsed_s / 60:.1f} min\n"
        f"- latest progress: {line or '(none yet)'}\n{extra}"
        f"- updated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
    )


def watch(w, run_id, *, label, poll_s, progress_s, stale_s, status_file, dr, bench_run_id):
    """Poll one run to its end, printing the notebook's latest progress line. Returns (run, stalled)."""
    t0 = time.monotonic()
    last_print = -1e9
    last_line, last_change = None, time.monotonic()
    while True:
        try:
            run = w.jobs.get_run(run_id)
        except Exception as exc:  # noqa: BLE001 - transient: keep polling
            print(f"  (poll error: {exc})", file=sys.stderr)
            time.sleep(poll_s)
            continue
        lc, result = _life_result(run)
        el = time.monotonic() - t0
        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return run, False
        if lc == "RUNNING" and el - last_print >= progress_s:
            last_print = el
            line = None
            try:
                line = dr.last_progress(dr.exported_text(_export_text(w, run)))
            except Exception as exc:  # noqa: BLE001 - export is a convenience
                line = f"(export-run: {type(exc).__name__})"
            if line != last_line:
                last_line, last_change = line, time.monotonic()
            print(f"  [{label} {lc} {el:.0f}s] {line or '(no progress line yet)'}", flush=True)
            _write_status(status_file, bench_run_id, f"{label} {lc}", el, line)
            if line and time.monotonic() - last_change > stale_s:
                print(f"STALLED: no new progress line for {stale_s / 60:.0f} min; cancelling.", file=sys.stderr)
                w.jobs.cancel_run(run_id)
                return run, True
        elif lc != "RUNNING" and el - last_print >= progress_s:
            last_print = el
            print(f"  [{label} {lc} {el:.0f}s]", flush=True)
        time.sleep(poll_s)


def main() -> int:
    exit_if_help(__doc__)
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:  # noqa: BLE001
        pass
    args, unknown = _parser().parse_known_args()
    if unknown:
        print(f"ERROR: unknown option(s) {unknown}", file=sys.stderr)
        return EXIT_USAGE
    if args.smoke_only and args.no_smoke:
        print("ERROR: --smoke-only and --no-smoke exclude each other.", file=sys.stderr)
        return EXIT_USAGE

    from databricks.labs.gbx.bench import datagen_remote as dr

    try:
        idx = dr.index_range(args.rows_from, args.rows_to)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_USAGE
    srids = tuple(int(s) for s in args.srids.split(",") if s.strip())
    spec = dr.RowSpec(args.seed, srids, args.row_tile_px, args.row_bands, args.row_dtype)
    verify = (
        [int(x) for x in args.verify_indices.split(",") if x.strip()]
        if args.verify_indices
        else dr.default_verify_indices(args.rows_from, args.rows_to)
    )
    outside = [j for j in verify if j not in idx]
    if outside:
        print(f"ERROR: --verify-indices {outside} fall outside {args.rows_from}..{args.rows_to}.", file=sys.stderr)
        return EXIT_USAGE

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service import compute, jobs
        from databricks.sdk.service.workspace import ImportFormat
    except ImportError:
        print("Install databricks-sdk (run through gbx:bench:gen-data).", file=sys.stderr)
        return EXIT_USAGE
    import io

    w = WorkspaceClient(profile=args.profile)
    catalog = bench._strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_CATALOG") or "main")
    schema = bench._strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_SCHEMA") or "default")
    volume = bench._strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_NAME") or "geobrix_samples")
    volroot = f"/Volumes/{catalog}/{schema}/{volume}"
    out_dir = args.out or f"{volroot}/bench-corpus"
    run_id = args.run_id or f"gen-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"
    ver = bench._geobrix_version()
    wheel = bench._staged_env_wheel(w, volroot, ver, int(args.env_version))

    problems = _preflight.check_env_spec(serverless_dependencies(wheel), wheel, args.env_version)
    try:
        problems += _preflight.wheel_problems(w, wheel, ROOT)
    except Exception as exc:  # noqa: BLE001
        problems.append(f"cannot read the staged wheel {wheel}: {type(exc).__name__}: {exc}")
    if problems:
        print("PREFLIGHT FAILED:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return EXIT_USAGE

    local_corpus = Path(args.local_corpus)
    smoke_idx = list(range(len(srids)))  # one tile per SRID
    smoke_sha = _local_sha(local_corpus, smoke_idx, spec, dr)
    verify_sha = _local_sha(local_corpus, verify, spec, dr)

    print("=" * 64)
    print("gbx:bench:gen-data --on-databricks pre-flight (SERVERLESS)")
    print(f"  run_id     : {run_id}\n  env_version: {args.env_version}\n  wheel      : {wheel}")
    print(f"  out        : {out_dir}/rows  tiles r{args.rows_from}..r{args.rows_to} ({len(idx)})")
    print(f"  spec       : {spec}")
    print(f"  fan-out    : {args.partitions} partitions per chunk, chunks of {args.chunk}; skip_existing={args.skip_existing}")
    print(f"  verify     : sha256 of r{verify} against {local_corpus}")
    print("  NOTE: this submits a job to Serverless and consumes compute.")
    print("=" * 64)

    me = w.current_user.me()
    base = f"/Users/{me.user_name}/geobrix_gen_corpus_{run_id}"
    nb_path = base + ".ipynb"
    smoke_path = base + "_smoke.ipynb"
    try:
        w.workspace.mkdirs(str(Path(nb_path).parent))
    except Exception:  # noqa: BLE001
        pass

    cfg = dict(
        mode="run", out_dir=out_dir, spec=spec.as_dict(), rows_from=args.rows_from, rows_to=args.rows_to,
        partitions=args.partitions, chunk=args.chunk, skip_existing=args.skip_existing,
        verify_indices=verify, run_id=run_id,
    )  # fmt: skip
    smoke_cfg = dict(cfg, mode="smoke", smoke_indices=smoke_idx, smoke_sha=smoke_sha, run_id=f"{run_id}-smoke")

    def upload(path, nb):
        print(f"Uploading notebook to {path}...")
        w.workspace.upload(
            path, io.BytesIO(json.dumps(nb, indent=1).encode("utf-8")), format=ImportFormat.JUPYTER, overwrite=True
        )

    def submit(path, name, timeout_s):
        print(f"Submitting {name} on Serverless (env v{args.env_version}), time limit {timeout_s // 60} min...")
        waiter = submit_once(
            w,
            run_name=name,
            timeout_seconds=timeout_s,
            environments=[
                jobs.JobEnvironment(
                    environment_key="gen_env",
                    spec=compute.Environment(
                        environment_version=str(args.env_version), dependencies=serverless_dependencies(wheel)
                    ),
                )
            ],
            tasks=[
                jobs.SubmitTask(
                    task_key="gen_corpus",
                    environment_key="gen_env",
                    notebook_task=jobs.NotebookTask(notebook_path=path, source=jobs.Source.WORKSPACE),
                    max_retries=0,
                )
            ],
        )
        rid = getattr(waiter, "run_id", None) or waiter
        print(f"  run id: {rid}")
        return rid

    def payload_of(run):
        try:
            out = w.jobs.get_run_output(_task_run_id(run))
            return json.loads(out.notebook_output.result) if out.notebook_output and out.notebook_output.result else {}
        except Exception as exc:  # noqa: BLE001
            print(f"  (no run output: {type(exc).__name__}: {exc})", file=sys.stderr)
            return {}

    status = args.status_file
    t_all = time.monotonic()

    if not args.no_smoke:
        upload(smoke_path, dr.build_gen_notebook(smoke_cfg))
        print("=" * 64)
        print("SMOKE: the workers import geobrix and make the host's bytes for one tile per SRID (no Volume I/O)")
        sid = submit(smoke_path, f"geobrix-gen-corpus-{run_id}-smoke", args.smoke_timeout_min * 60)
        run, stalled = watch(
            w, sid, label="smoke", poll_s=args.poll_secs, progress_s=args.progress_secs,
            stale_s=args.stale_min * 60, status_file=status, dr=dr, bench_run_id=run_id,
        )  # fmt: skip
        lc, result = _life_result(run)
        res = payload_of(run)
        bad = []
        if stalled or result != "SUCCESS":
            bad.append(f"the smoke run ended {lc}/{result or '?'}: {run.state.state_message if run.state else ''}")
        if not res.get("ok"):
            bad.append(f"smoke payload: {json.dumps(res)[:600]}")
        if bad:
            print("SMOKE FAILED: nothing else is submitted.", file=sys.stderr)
            for b in bad:
                print(f"  - {b}", file=sys.stderr)
            return EXIT_SMOKE
        chk = res["import_check"]
        print(f"SMOKE CLEAN: geobrix {chk.get('version')} on {chk.get('arch')}; worker bytes equal the host's.")
        bench._remove_notebook(w, smoke_path, True)
        if args.smoke_only:
            return 0

    upload(nb_path, dr.build_gen_notebook(cfg))
    rid = submit(nb_path, f"geobrix-gen-corpus-{run_id}", args.timeout_min * 60)
    _write_status(status, run_id, "SUBMITTED", 0, None, f"- job run id: {rid}\n")
    if args.no_wait:
        print(f"--no-wait: run {rid} submitted; notebook kept at {nb_path}.")
        return 0
    run, stalled = watch(
        w, rid, label="gen", poll_s=args.poll_secs, progress_s=args.progress_secs,
        stale_s=args.stale_min * 60, status_file=status, dr=dr, bench_run_id=run_id,
    )  # fmt: skip
    wall = time.monotonic() - t_all
    lc, result = _life_result(run)
    print(f"Run {rid} ended {lc}/{result or '?'} after {wall:.0f} s (wall, launch to end).")
    print(f"Run page: {getattr(run, 'run_page_url', '')}")
    bench._save_run_output(w, run, run_id, ROOT)
    if stalled:
        _write_status(status, run_id, "STALLED", wall, None, f"- job run id: {rid}\n")
        return EXIT_STALLED
    if result != "SUCCESS":
        print(f"FAILED: {run.state.state_message if run.state else ''}", file=sys.stderr)
        _write_status(status, run_id, f"{lc}/{result}", wall, None, f"- job run id: {rid}\n")
        bench._remove_notebook(w, nb_path, False)
        return EXIT_FAILED
    payload = payload_of(run)
    problems = dr.compare_payload(payload, verify_sha, len(idx))
    inv = payload.get("inventory", {})
    print(f"  summary  : {payload.get('summary')}")
    print(f"  inventory: {inv.get('complete')}/{inv.get('expected')} complete, {inv.get('bytes')} bytes, "
          f"{inv.get('tif_files_in_dir')} r*.tif in the directory")  # fmt: skip
    for j in verify:
        got = (payload.get("sha", {}).get(str(j)) or {})
        print(f"  sha r{j}.tif: {got.get('size')} {got.get('sha256')} "
              f"{'== local' if got.get('sha256') == verify_sha[str(j)] else '!= LOCAL ' + verify_sha[str(j)]}")  # fmt: skip
    _write_status(status, run_id, "VERIFIED" if not problems else "VERIFY FAILED", wall, None,
                  f"- job run id: {rid}\n- inventory: {inv.get('complete')}/{inv.get('expected')} complete, {inv.get('bytes')} bytes\n")  # fmt: skip
    bench._remove_notebook(w, nb_path, True)
    if problems:
        print("VERIFICATION FAILED:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return EXIT_FAILED
    print("VERIFIED: count and sha256 sample match the local corpus.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
