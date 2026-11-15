#!/usr/bin/env python3
"""Run the Unity Catalog SQL examples (examples/uc-functions/*.sql) on a SQL warehouse, and upload them.

Every file is a list of `-- cid: <id>` statements. A statement must succeed. A query (SELECT or WITH) must
also return at least one row, and its first row must not be all NULL. The results of each file go to `<out-dir>/<file>.json`.

`--upload` imports the files into the caller's workspace folder `/Workspace/Users/<user>/geobrix/uc-function-examples/`
(the user comes from `current-user me`), as workspace files that open in the SQL editor.

Run it through `gbx:examples:uc-sql` (scripts/commands/gbx-examples-uc-sql.sh), which picks an interpreter with
the Databricks SDK. `--dry-run` parses the files and prints the plan: no workspace call, no auth.
"""

from __future__ import annotations

import argparse
import base64
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # uc_sql.py

import uc_sql  # noqa: E402
from _cluster_config import cluster_config_file  # noqa: E402
from _uc_warehouse import warehouse_problem  # noqa: E402

from databricks.labs.gbx.bench import oracles  # noqa: E402  (uc_sql put the package on the path)

REPO_ROOT = HERE.parents[1]
DEFAULT_DIR = REPO_ROOT / "examples" / "uc-functions"
DEFAULT_OUT_DIR = REPO_ROOT / "test-logs" / "uc-examples"
WS_SUBDIR = "geobrix/uc-function-examples"
QUERY_WORDS = ("SELECT", "WITH")
FILE_CATALOG = "geospatial_docs"  # the names the example files are written with
FILE_SCHEMA = "geobrix_uc"


def retarget(statement: str, catalog: str, schema: str) -> str:
    """The statement with the files' catalog and schema names replaced by `catalog` and `schema`.

    The files name both in `USE`, in qualified table names and in information_schema filters, so a run
    against another install (such as the scratch schema of `gbx:uc:tests`) rewrites the names.
    """
    statement = re.sub(rf"\b{FILE_SCHEMA}\b", schema, statement)
    return re.sub(rf"\b{FILE_CATALOG}\b", catalog, statement)


def is_query(statement: str) -> bool:
    words = statement.lstrip().split(None, 1)
    return bool(words) and words[0].upper() in QUERY_WORDS


def example_files(directory: Path, selection: Optional[str]) -> List[Path]:
    """The .sql files of the folder in name order; `selection` is a comma list of name prefixes (`01,02_grids`)."""
    files = sorted(directory.glob("*.sql"))
    if selection:
        wanted = [s.strip() for s in selection.split(",") if s.strip()]
        files = [f for f in files if any(f.name.startswith(w) for w in wanted)]
        missing = [w for w in wanted if not any(f.name.startswith(w) for f in files)]
        if missing:
            raise SystemExit(f"--files: no example file starts with {', '.join(missing)}")
    return files


def judge(pairs, results) -> List[str]:
    """What is wrong with a file's results: a statement that failed, or a query that returned no row."""
    problems = []
    for cid, statement in pairs:
        entry = results.get(cid)
        if entry is None:
            problems.append(f"{cid}: not run")
        elif "harness_error" in entry:  # the API failed in transit and the retries ran out: not a verdict on the statement
            problems.append(f"{cid}: HARNESS ERROR (transient API) {' '.join(entry['harness_error'].split())[:300]}")
        elif "error" in entry:
            problems.append(f"{cid}: ERROR {' '.join(entry['error'].split())[:300]}")
        elif is_query(statement) and entry.get("rows", 0) < 1:
            problems.append(f"{cid}: the query returned no rows")
        elif is_query(statement) and entry.get("first_row") and all(c is None for c in entry["first_row"]):
            problems.append(f"{cid}: the first row is all NULL (an input missed the data?)")
    return problems


def workspace_dir(w, override: Optional[str]) -> str:
    if override:
        return override.rstrip("/")
    me = w.current_user.me().user_name
    return f"/Workspace/Users/{me}/{WS_SUBDIR}"


def upload(w, files: List[Path], ws_dir: str, catalog: str = FILE_CATALOG, schema: str = FILE_SCHEMA) -> int:
    """Import each file as a workspace file; list the folder afterwards and check every name is there.

    Each file is imported with its names rewritten to `catalog` and `schema`, so it runs as is against that install.
    """
    from databricks.sdk.service.workspace import ImportFormat

    api_dir = ws_dir[len("/Workspace") :] if ws_dir.startswith("/Workspace/") else ws_dir
    w.workspace.mkdirs(api_dir)
    for f in files:
        content = base64.b64encode(retarget(f.read_text(), catalog, schema).encode()).decode()
        w.workspace.import_(path=f"{api_dir}/{f.name}", content=content, format=ImportFormat.AUTO, overwrite=True)
        print(f"  uploaded {f.name}", flush=True)
    listed = {Path(o.path).name: o for o in w.workspace.list(api_dir)}
    missing = [f.name for f in files if f.name not in listed]
    for f in files:
        o = listed.get(f.name)
        if o is not None:
            print(f"  {f.name:42s} {getattr(o.object_type, 'value', o.object_type)}")
    if missing:
        print(f"upload check failed: missing in {ws_dir}: {', '.join(missing)}", file=sys.stderr)
        return 1
    print(f"uploaded {len(files)} file(s) to {ws_dir}")
    return 0


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default=None, help="Databricks config profile (never chosen for you)")
    p.add_argument("--warehouse-id", default=None, help="SQL warehouse id (gbx:examples:uc-sql reads GBX_UC_WAREHOUSE_ID)")
    p.add_argument("--catalog", default=FILE_CATALOG, help="catalog the functions are installed in (the files' names are rewritten)")
    p.add_argument("--schema", default=FILE_SCHEMA, help="schema the functions are installed in (the files' names are rewritten)")
    p.add_argument("--dir", default=str(DEFAULT_DIR), help="folder of example .sql files")
    p.add_argument("--files", default=None, help="comma list of file name prefixes to run, such as 00,05")
    p.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="folder for the per-file result JSON")
    p.add_argument("--timeout-s", type=float, default=uc_sql.DEFAULT_TIMEOUT_S, help="cancel a statement after this long")
    p.add_argument("--parallel", type=int, default=1, help="example files run side by side (default 1); the setup file 00 always runs first, alone")
    p.add_argument("--upload", action="store_true", help="after the run, import the files into your workspace folder")
    p.add_argument("--upload-only", action="store_true", help="import the files and run nothing")
    p.add_argument("--ws-dir", default=None, help=f"workspace folder to import into (default /Workspace/Users/<you>/{WS_SUBDIR})")
    p.add_argument("--dry-run", action="store_true", help="parse the files and print the plan; no workspace call")
    args = p.parse_args(argv)
    if args.timeout_s <= 0:
        p.error("--timeout-s must be positive")
    if args.parallel < 1:
        p.error("--parallel must be at least 1")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    files = example_files(Path(args.dir), args.files)
    if not files:
        print(f"{args.dir}: no .sql files", file=sys.stderr)
        return 2
    parsed = {
        f: [(cid, retarget(s, args.catalog, args.schema)) for cid, s in oracles.parse_uc_sql(f.read_text())]
        for f in files
    }
    empty = [f.name for f, pairs in parsed.items() if not pairs]
    if empty:
        print(f"no '-- cid: <id>' statements in: {', '.join(empty)}", file=sys.stderr)
        return 2
    if args.dry_run:
        total = sum(len(p) for p in parsed.values())
        print(
            f"dry run: {len(files)} file(s), {total} statement(s) on warehouse {args.warehouse_id or '-'} "
            f"(profile {args.profile or '-'}, {args.catalog}.{args.schema}); results -> {args.out_dir}; "
            f"upload {'yes' if args.upload or args.upload_only else 'no'}"
        )
        for f, pairs in parsed.items():
            queries = sum(1 for _, s in pairs if is_query(s))
            print(f"  {f.name:42s} {len(pairs):3d} statements, {queries:3d} queries")
        return 0
    if not args.profile:
        print("--profile required (a profile is never chosen for you: see `databricks auth profiles`)", file=sys.stderr)
        return 2
    if not args.upload_only and not args.warehouse_id:
        print("--warehouse-id required (or GBX_UC_WAREHOUSE_ID in examples/tests/databricks_cluster_config.env)", file=sys.stderr)
        return 2
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=args.profile)
    if not args.upload_only:
        problem = warehouse_problem(w, args.warehouse_id, cluster_config_file(Path(__file__).resolve().parent), args.profile)
        if problem:
            print(f"refused: {problem}", file=sys.stderr)
            return 1
    failed = False
    if not args.upload_only:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        run_args = argparse.Namespace(
            warehouse_id=args.warehouse_id,
            catalog=args.catalog,
            schema=args.schema,
            repeat=1,
            timeout_s=args.timeout_s,
            decode_binary=True,
            record_rows=True,
            out=None,
        )
        def run_file(f, pairs):
            """(table row, problems) of one file; a file's statements run in order (they may build on each other)."""
            out = argparse.Namespace(**vars(run_args))  # one Namespace per file: they run side by side
            out.out = str(out_dir / f"{f.stem}.json")
            print(f"=== {f.name}: {len(pairs)} statement(s)", flush=True)
            results = uc_sql.run_statements(w, pairs, out)
            problems = judge(pairs, results)
            ok = len(pairs) - len({p.split(':')[0] for p in problems})
            slowest = max(
                ((r["wall_s"][0], cid) for cid, r in results.items() if "wall_s" in r), default=(0.0, "-")
            )
            return (f.name, ok, len(pairs), slowest), problems

        # The setup file (name starts 00) creates the tables the others read: it runs first, alone.
        order = list(parsed.items())
        first = [fp for fp in order if fp[0].name.startswith("00")]
        rest = [fp for fp in order if not fp[0].name.startswith("00")]
        done = {}
        try:
            for f, pairs in first:
                done[f] = run_file(f, pairs)
            if args.parallel > 1 and len(rest) > 1:
                with ThreadPoolExecutor(max_workers=min(args.parallel, len(rest))) as pool:
                    futures = {f: pool.submit(run_file, f, pairs) for f, pairs in rest}
                    for f, fut in futures.items():
                        done[f] = fut.result()
            else:
                for f, pairs in rest:
                    done[f] = run_file(f, pairs)
        except Exception as exc:  # noqa: BLE001 - an SDK or network failure: report it and stop
            print(f"stopped: {uc_sql.scrub(f'{type(exc).__name__}: {exc}')}", file=sys.stderr)
            return 1
        table = []
        for f in parsed:
            row, problems = done[f]
            table.append(row)
            for line in problems:
                print(f"  FAIL {line}", file=sys.stderr)
            failed = failed or bool(problems)
        print("\nfile                                        ok/total  slowest statement")
        for name, ok, n, (secs, cid) in table:
            print(f"{name:42s} {ok:3d}/{n:<3d}   {secs:7.1f}s  {cid}")
        print("RESULT: " + ("FAILED" if failed else "all statements succeeded and every query returned rows"))
    if (args.upload or args.upload_only) and not failed:
        ws_dir = workspace_dir(w, args.ws_dir)
        print(f"uploading to {ws_dir}")
        if upload(w, files, ws_dir, args.catalog, args.schema):
            return 1
    elif args.upload and failed:
        print("not uploading: the run failed", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
