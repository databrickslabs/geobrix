#!/usr/bin/env python3
"""Run local .ipynb notebooks on Databricks Serverless via jobs.submit.

Each notebook is imported into the workspace (optionally with %pip/%restart_python
cells stripped — Serverless JOB %pip fails with WSFS), then submitted as a one-time
serverless notebook task.  Dependencies go in the environment spec (environment_version
"5"), not %pip.  Dep list = wheel + union of requested extras' deps (read from
pyproject.toml via tomllib) + any --extra-deps.

Usage examples:
  # Run the four Helios notebooks with defaults (extras=light,stac,vizx,overture, rich):
  python run_notebooks_serverless.py \\
    --dir notebooks/examples/helios \\
    --extra-deps rich

  # Run a single notebook with only the light extra:
  python run_notebooks_serverless.py \\
    --notebook notebooks/examples/helios/01\\ Vector\\ Engine\\(MVT\\).ipynb \\
    --extras light

  # Explicit workspace folder:
  python run_notebooks_serverless.py \\
    --notebook my_nb.ipynb \\
    --ws-dir /Users/me@example.com/GeoBrix/serverless-run
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import pathlib
import sys
import time
import tomllib
from typing import Optional

# ---------------------------------------------------------------------------
# Resolve PROJECT_ROOT (notebooks/tests/ -> notebooks/ -> project root)
# ---------------------------------------------------------------------------
_SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = (_SCRIPT_DIR / ".." / "..").resolve()
_PYPROJECT_PATH = PROJECT_ROOT / "python" / "geobrix" / "pyproject.toml"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ENV_KEY = "ser5"
DEFAULT_WHEEL = (
    "/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.5.0-py3-none-any.whl"
)
DEFAULT_EXTRAS = "light,stac,vizx,overture"
DEFAULT_ENV_VERSION = "5"
DEFAULT_POLL_SECS = 20

# Cells whose source contains a line beginning with these magic prefixes are
# stripped when strip_pip=True.  Serverless JOB kernels cannot %pip install
# (WSFS "Cannot find child https:"); deps must be in the environment spec.
_STRIP_PREFIXES = ("%pip", "%restart_python")


# ---------------------------------------------------------------------------
# pyproject.toml helpers
# ---------------------------------------------------------------------------

def _load_extras_deps(extras: list[str]) -> list[str]:
    """Return the union of pip deps for the requested extras from pyproject.toml."""
    if not _PYPROJECT_PATH.exists():
        print(
            f"WARNING: pyproject.toml not found at {_PYPROJECT_PATH}; "
            "using empty extras deps.",
            flush=True,
        )
        return []

    with open(_PYPROJECT_PATH, "rb") as fh:
        data = tomllib.load(fh)

    opt_deps: dict[str, list[str]] = (
        data.get("project", {}).get("optional-dependencies", {})
    )

    seen: set[str] = set()
    result: list[str] = []
    for extra in extras:
        deps = opt_deps.get(extra, [])
        if not deps:
            print(f"WARNING: extra '{extra}' not found in pyproject.toml", flush=True)
        for dep in deps:
            if dep not in seen:
                seen.add(dep)
                result.append(dep)
    return result


# ---------------------------------------------------------------------------
# Notebook stripping
# ---------------------------------------------------------------------------

def _strip_pip_cells(nb_bytes: bytes) -> bytes:
    """Remove code cells whose source contains a %pip or %restart_python line."""
    nb = json.loads(nb_bytes)
    cells = nb.get("cells", [])
    filtered = []
    removed = 0
    for cell in cells:
        if cell.get("cell_type") != "code":
            filtered.append(cell)
            continue
        source_lines = cell.get("source", [])
        # source can be a list of strings or a single string
        if isinstance(source_lines, str):
            source_lines = source_lines.splitlines(keepends=True)
        stripped_in_cell = any(
            line.lstrip().startswith(_STRIP_PREFIXES) for line in source_lines
        )
        if stripped_in_cell:
            removed += 1
        else:
            filtered.append(cell)
    if removed:
        print(f"    stripped {removed} %pip/%restart_python cell(s)", flush=True)
    nb["cells"] = filtered
    return json.dumps(nb).encode()


# ---------------------------------------------------------------------------
# Polling helpers
# ---------------------------------------------------------------------------

def _terminal_states():
    from databricks.sdk.service import jobs
    return {
        jobs.RunLifeCycleState.TERMINATED,
        jobs.RunLifeCycleState.SKIPPED,
        jobs.RunLifeCycleState.INTERNAL_ERROR,
    }


# ---------------------------------------------------------------------------
# Core: import + submit one notebook
# ---------------------------------------------------------------------------

def _import_notebook(
    w,
    local_path: pathlib.Path,
    ws_dir: str,
    strip_pip: bool,
) -> str:
    """Import a local .ipynb to the workspace; return the workspace path."""
    from databricks.sdk.service.workspace import ImportFormat

    nb_bytes = local_path.read_bytes()
    if strip_pip:
        nb_bytes = _strip_pip_cells(nb_bytes)

    stem = local_path.stem  # filename without .ipynb
    ws_path = f"{ws_dir.rstrip('/')}/{stem}"

    w.workspace.import_(
        path=ws_path,
        format=ImportFormat.JUPYTER,
        content=base64.b64encode(nb_bytes).decode(),
        overwrite=True,
    )
    print(f"    imported → {ws_path}", flush=True)
    return ws_path


def run_one(
    w,
    local_path: pathlib.Path,
    ws_dir: str,
    deps: list[str],
    env_version: str,
    poll_secs: int,
    strip_pip: bool,
) -> bool:
    """Import and run one notebook on Serverless. Returns True on SUCCESS."""
    from databricks.sdk.service import compute, jobs

    print(f"\n=== SUBMIT (serverless): {local_path.name} ===", flush=True)

    ws_path = _import_notebook(w, local_path, ws_dir, strip_pip)

    task_key = "".join(c if c.isalnum() else "_" for c in local_path.stem)[:90]

    waiter = w.jobs.submit(
        run_name=f"gbx-nb:{task_key}",
        environments=[
            jobs.JobEnvironment(
                environment_key=ENV_KEY,
                spec=compute.Environment(
                    environment_version=env_version,
                    dependencies=deps,
                ),
            )
        ],
        tasks=[
            jobs.SubmitTask(
                task_key=task_key,
                environment_key=ENV_KEY,
                notebook_task=jobs.NotebookTask(notebook_path=ws_path),
                # Run exactly once — do not retry a failed validation run (a retry
                # just re-burns compute on the same failure and doubles the child
                # runs to sift through).
                max_retries=0,
                # max_retries alone does NOT stop serverless auto-optimization
                # retries (the "Enable serverless auto-optimization (may include
                # additional retries)" box, on by default). That is what produced
                # a duplicate child run on INTERNAL_ERROR. disable_auto_optimization
                # is the API equivalent of unchecking it → truly at-most-once.
                disable_auto_optimization=True,
            )
        ],
    )

    run_id = waiter.run_id
    info = w.jobs.get_run(run_id=run_id)
    print(f"    run_id={run_id}", flush=True)
    print(f"    run_page_url={info.run_page_url}", flush=True)

    TERMINAL = _terminal_states()
    last_life = None
    started = time.time()

    while True:
        r = w.jobs.get_run(run_id=run_id)
        life = r.state.life_cycle_state if r.state else None
        if life != last_life:
            print(
                f"    [{int(time.time() - started)}s] life_cycle_state={life}",
                flush=True,
            )
            last_life = life
        if life in TERMINAL:
            break
        time.sleep(poll_secs)

    result = r.state.result_state if r.state else None
    msg = (r.state.state_message or "").strip() if r.state else ""
    print(f"=== RESULT: {local_path.name}", flush=True)
    print(f"    result_state={result}  ({msg[:300]})", flush=True)
    for t in r.tasks or []:
        ts = t.state.result_state if t.state else None
        print(f"    task {t.task_key}: {ts}  page={t.run_page_url}", flush=True)
        # The jobs API does NOT expose serverless notebook stdout; a notebook that
        # ends with dbutils.notebook.exit(<json>) surfaces its result here. Print
        # it so experiment summaries are captured client-side (reliable, not stdout).
        try:
            out = w.jobs.get_run_output(run_id=t.run_id)
            nb_out = getattr(out, "notebook_output", None)
            payload = getattr(nb_out, "result", None) if nb_out else None
            if payload:
                print(f"    task {t.task_key} notebook_output: {payload}", flush=True)
        except Exception as exc:  # noqa: BLE001 — output fetch is best-effort
            print(f"    (no notebook_output for {t.task_key}: {exc})", flush=True)

    from databricks.sdk.service.jobs import RunResultState
    ok = (result == RunResultState.SUCCESS)
    if not ok:
        print(f"!!! STOP — {local_path.name} did not SUCCEED ({result}).", flush=True)
    return ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_dep_list(
    wheel: str,
    extras: list[str],
    extra_deps: list[str],
) -> list[str]:
    deps: list[str] = [wheel]
    deps.extend(_load_extras_deps(extras))
    deps.extend(extra_deps)
    return deps


def _collect_notebooks(
    notebooks: Optional[list[str]],
    dirs: Optional[list[str]],
) -> list[pathlib.Path]:
    paths: list[pathlib.Path] = []
    for nb in notebooks or []:
        p = pathlib.Path(nb)
        if not p.exists():
            print(f"ERROR: notebook not found: {p}", file=sys.stderr)
            sys.exit(2)
        paths.append(p.resolve())
    for d in dirs or []:
        dp = pathlib.Path(d)
        if not dp.is_dir():
            print(f"ERROR: directory not found: {dp}", file=sys.stderr)
            sys.exit(2)
        found = sorted(dp.glob("*.ipynb"))
        if not found:
            print(f"WARNING: no .ipynb files in {dp}", flush=True)
        paths.extend(p.resolve() for p in found)
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run local .ipynb notebooks on Databricks Serverless via jobs.submit. "
            "Dependencies are resolved from pyproject.toml extras and injected into "
            "the Serverless environment spec (not %pip, which fails in JOB compute)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--notebook",
        metavar="PATH",
        action="append",
        dest="notebooks",
        help="Local .ipynb file to run (repeatable).",
    )
    parser.add_argument(
        "--dir",
        metavar="DIR",
        action="append",
        dest="dirs",
        help="Directory of .ipynb files to run (all *.ipynb, sorted, repeatable).",
    )
    parser.add_argument(
        "--ws-dir",
        metavar="WSPATH",
        default=None,
        help=(
            "Workspace folder to import notebooks into. "
            "Default: /Users/<current-user>/GeoBrix/serverless-run"
        ),
    )
    parser.add_argument(
        "--extras",
        metavar="CSV",
        default=DEFAULT_EXTRAS,
        help=(
            f"Comma-separated geobrix extras whose deps to include "
            f"(default: {DEFAULT_EXTRAS!r})."
        ),
    )
    parser.add_argument(
        "--extra-deps",
        metavar="CSV",
        default="",
        help="Additional pip requirements (comma-separated, default: empty).",
    )
    parser.add_argument(
        "--wheel",
        metavar="VOLPATH",
        default=DEFAULT_WHEEL,
        help=f"Volume path to the geobrix wheel (default: {DEFAULT_WHEEL!r}).",
    )
    parser.add_argument(
        "--env-version",
        metavar="VER",
        default=DEFAULT_ENV_VERSION,
        help=f"Serverless environment version (default: {DEFAULT_ENV_VERSION!r}).",
    )
    parser.add_argument(
        "--profile",
        metavar="PROFILE",
        default=os.environ.get("DATABRICKS_CONFIG_PROFILE", "oauth-fe"),
        help=(
            "Databricks config profile to use. "
            "Reads DATABRICKS_CONFIG_PROFILE env var; falls back to 'oauth-fe'."
        ),
    )
    parser.add_argument(
        "--poll-secs",
        type=int,
        default=DEFAULT_POLL_SECS,
        help=f"Polling interval in seconds (default: {DEFAULT_POLL_SECS}).",
    )
    parser.add_argument(
        "--no-strip-pip",
        dest="strip_pip",
        action="store_false",
        default=True,
        help=(
            "Do NOT strip %%pip/%%restart_python cells from uploaded notebooks "
            "(default: strip them — they fail in Serverless JOB compute)."
        ),
    )
    parser.add_argument(
        "--dep-notebook",
        metavar="PATH",
        action="append",
        dest="dep_notebooks",
        help=(
            "Local .ipynb to import into ws-dir but NOT submit (e.g. config_nb.ipynb "
            "used via %%run). Repeatable."
        ),
    )

    args = parser.parse_args()

    notebooks = _collect_notebooks(args.notebooks, args.dirs)
    if not notebooks:
        parser.error(
            "Provide at least one notebook via --notebook or --dir."
        )

    extras = [e.strip() for e in args.extras.split(",") if e.strip()]
    extra_deps = [e.strip() for e in args.extra_deps.split(",") if e.strip()]
    deps = _build_dep_list(args.wheel, extras, extra_deps)

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=args.profile)

    # Resolve default ws-dir using the current user
    ws_dir = args.ws_dir
    if not ws_dir:
        me = w.current_user.me().user_name or "unknown"
        ws_dir = f"/Users/{me}/GeoBrix/serverless-run"

    print(f"Profile  : {args.profile}", flush=True)
    print(f"Workspace: {ws_dir}", flush=True)
    print(f"Extras   : {', '.join(extras)}", flush=True)
    print(f"Deps     : {len(deps)} entries (wheel + {len(deps) - 1} packages)", flush=True)
    print(f"Notebooks: {len(notebooks)}", flush=True)
    print(f"Strip %%pip: {args.strip_pip}", flush=True)
    print("", flush=True)

    # Upload dep-notebooks (referenced via %run) before submitting main notebooks.
    for dep_path_str in args.dep_notebooks or []:
        dep_path = pathlib.Path(dep_path_str).resolve()
        if not dep_path.exists():
            print(f"ERROR: dep-notebook not found: {dep_path}", file=sys.stderr)
            return 2
        print(f"=== DEP-IMPORT: {dep_path.name} ===", flush=True)
        _import_notebook(w, dep_path, ws_dir, strip_pip=args.strip_pip)

    for nb_path in notebooks:
        ok = run_one(
            w=w,
            local_path=nb_path,
            ws_dir=ws_dir,
            deps=deps,
            env_version=args.env_version,
            poll_secs=args.poll_secs,
            strip_pip=args.strip_pip,
        )
        if not ok:
            return 1

    print("\n=== ALL NOTEBOOKS SUCCEEDED ===", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
