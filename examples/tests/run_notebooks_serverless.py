#!/usr/bin/env python3
"""Run local .ipynb notebooks on Databricks Serverless via jobs.submit.

Each notebook is imported into the workspace (optionally with %pip/%restart_python
cells stripped — Serverless JOB %pip fails with WSFS), then submitted as a one-time
serverless notebook task.  Dependencies go in the environment spec (environment_version
"6"), not %pip.  Dep list = the wheel + the requested extras' deps + any --extra-deps. For
an env wheel (geobrix-<version>+env5 / +env6, the Databricks install) the extras are that
wheel's (python/geobrix/envs/env<N>/extra-<name>.txt) and its environment version must be
the wheel's; for the plain wheel they are read from pyproject.toml.

Usage examples:
  # Run the four Helios notebooks with defaults (the env6 wheel, extras=stac,vizx):
  python run_notebooks_serverless.py \\
    --dir examples/notebooks/helios \\
    --extra-deps rich

  # Run a single notebook with no extra:
  python run_notebooks_serverless.py \\
    --notebook examples/notebooks/helios/01\\ Vector\\ Engine\\(MVT\\).ipynb \\
    --extras ""

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
import re
import sys
import time
import tomllib
from typing import Optional

from _job_submit import submit_once  # sibling module (script dir is on sys.path)

# ---------------------------------------------------------------------------
# Resolve PROJECT_ROOT (examples/tests/ -> examples/ -> project root)
# ---------------------------------------------------------------------------
_SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = (_SCRIPT_DIR / ".." / "..").resolve()
_PYPROJECT_PATH = PROJECT_ROOT / "python" / "geobrix" / "pyproject.toml"
_ENVS_PATH = PROJECT_ROOT / "python" / "geobrix" / "envs"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ENV_KEY = "ser6"
DEFAULT_WHEEL = (
    "/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.6.0+env6-py3-none-any.whl"
)
DEFAULT_EXTRAS = "stac,vizx"
DEFAULT_ENV_VERSION = "6"
DEFAULT_POLL_SECS = 20

# Cells whose source contains a line beginning with these magic prefixes are
# stripped when strip_pip=True.  Serverless JOB kernels cannot %pip install
# (WSFS "Cannot find child https:"); deps must be in the environment spec.
_STRIP_PREFIXES = ("%pip", "%restart_python")


# ---------------------------------------------------------------------------
# pyproject.toml helpers
# ---------------------------------------------------------------------------

def wheel_env(wheel: str) -> Optional[int]:
    """5 or 6 for an env wheel (``geobrix-<version>+env<N>-py3-none-any.whl``), None for the plain wheel."""
    m = re.search(r"geobrix-[^/]*\+env([56])-py3-none-any\.whl$", wheel)
    return int(m.group(1)) if m else None


# The 0.5.x GPU extras are tripwires in 0.6.0 (GPU = Serverless GPU environment 5, CUDA 12): their packages
# install as a separate step after the env5 wheel, here with --extra-deps.
_GPU_EXTRA_DEPS = {
    "photogrammetry_gpu_env5": "pycolmap-cuda12==4.2.1",
    "models_gpu_env5": "torch==2.9.0,torchvision==0.24.0,segment-geospatial,opencv-python-headless==4.11.0.86",
}
_GPU_EXTRA_DEPS["gpu_env5_all"] = ",".join(_GPU_EXTRA_DEPS.values())


def _refuse_gpu_extra(extra: str) -> None:
    if extra in _GPU_EXTRA_DEPS:
        raise SystemExit(
            f"--extras {extra}: a 0.5.x GPU extra, dropped in 0.6.0. Fix: --wheel <the geobrix-<version>+env5 wheel> "
            f"--env-version 5 --extra-deps '{_GPU_EXTRA_DEPS[extra]}'"
        )


def _env_extras_deps(env: int, extras: list[str]) -> list[str]:
    """The pip deps of an env wheel's extras: envs/env<N>/extra-<name>.txt, one requirement per line.

    The old ``light_*`` names are not extras of an env wheel (the env wheel IS the light install; the
    other family's names are tripwires), and the plain wheel's ``[overture]`` / ``[earthdata]`` are not
    on the env wheels: both are refused with what to pass instead.
    """
    envdir = _ENVS_PATH / f"env{env}"
    have = sorted(p.stem.removeprefix("extra-") for p in envdir.glob("extra-*.txt"))
    out: list[str] = []
    for extra in extras:
        _refuse_gpu_extra(extra)
        if extra.startswith("light"):
            raise SystemExit(
                f"--extras {extra}: the env{env} wheel needs no light extra (it is the light install); "
                "drop it. Fix: pass only feature extras, e.g. --extras stac,vizx"
            )
        if extra not in have:
            raise SystemExit(
                f"--extras {extra}: the env{env} wheel has no [{extra}] (its extras: {', '.join(have)}). "
                "Fix: pass its packages with --extra-deps (e.g. --extra-deps 'overturemaps>=1.0.2,<2')"
            )
        for line in (envdir / f"extra-{extra}.txt").read_text().splitlines():
            req = line.split("#", 1)[0].strip()
            if req and req not in out:
                out.append(req)
    return out


def _split_deps(csv: str) -> list[str]:
    """Split a comma-separated requirement list, keeping a specifier's own commas (``a>=1,<2,b`` -> 2)."""
    return [d.strip() for d in re.split(r",(?=\s*[A-Za-z])", csv) if d.strip()]


def _load_extras_deps(extras: list[str]) -> list[str]:
    """Return the union of pip deps for the requested extras from pyproject.toml (the plain wheel).

    Recursively expands nested ``geobrix[<extra>]`` references into their real package
    requirements. Without this, a nested string like ``geobrix[zarr_dbr19]`` would be
    injected verbatim into the Serverless environment spec, which does NOT honor
    extras: it would resolve from PyPI, and the extra's deps would never install.
    """
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

    seen_deps: set[str] = set()
    seen_extras: set[str] = set()
    result: list[str] = []

    def _visit(extra: str) -> None:
        if extra in seen_extras:
            return
        seen_extras.add(extra)
        _refuse_gpu_extra(extra)
        deps = opt_deps.get(extra, [])
        if deps and all(d.strip().startswith("geobrix==") for d in deps):
            raise SystemExit(  # a tripwire: it names the env wheel to install instead
                f"--extras {extra}: a 0.5.x name; it requires {deps[0]}. Fix: --wheel <that env wheel> with only "
                "its feature extras (e.g. --extras stac,vizx)"
            )
        if not deps:
            print(f"WARNING: extra '{extra}' not found in pyproject.toml", flush=True)
        for dep in deps:
            d = dep.strip()
            if d.startswith("geobrix[") and d.endswith("]"):
                for nested in d[len("geobrix["):-1].split(","):
                    _visit(nested.strip())
            elif dep not in seen_deps:
                seen_deps.add(dep)
                result.append(dep)

    for extra in extras:
        _visit(extra)
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


def _inject_override_cell(nb_bytes: bytes, assignments: list[str]) -> bytes:
    """Inject a code cell of Python assignments so validation runs force compute.

    Example series notebooks skip the expensive compute steps when their output
    tables already exist (guards like ``do_overwrite=FORCE_REBUILD`` with
    ``FORCE_REBUILD=False`` committed as the default). A green run that skips
    every ``rst_*`` / reader step validates nothing. This injects an override
    cell (e.g. ``FORCE_REBUILD=True``) into the UPLOADED copy only — the
    committed notebook keeps its safe defaults.

    Placement: immediately AFTER the later of (a) the last code cell that assigns
    one of the target names at top level (``NAME = ...``) and (b) the last ``%run``
    cell (the shared config include), so neither can re-assign the name after the
    override. With neither present, after the first code cell, else prepended.
    """
    if not assignments:
        return nb_bytes
    nb = json.loads(nb_bytes)
    cells = nb.get("cells", [])

    def _src(cell):
        s = cell.get("source", "")
        return s if isinstance(s, str) else "".join(s)

    names = "|".join(re.escape(a.split("=", 1)[0].strip()) for a in assignments)
    assigns = re.compile(rf"^(?:{names})\s*=(?!=)", re.M)
    code = [i for i, c in enumerate(cells) if c.get("cell_type") == "code"]
    last_assign = max((i for i in code if assigns.search(_src(cells[i]))), default=-1)
    last_run = max((i for i in code if re.search(r"^\s*%run", _src(cells[i]), re.M)), default=-1)
    # After whichever comes last, so neither the notebook's own assignment nor a later
    # %run config include can re-assign the name after the override.
    insert_after = max(last_assign, last_run)
    if insert_after < 0:
        insert_after = code[0] if code else -1

    body = ["# --- validation override (injected by runner; NOT in the committed notebook) ---\n"]
    body += [f"{a}\n" for a in assignments]
    override = {
        "cell_type": "code",
        "execution_count": 0,
        "metadata": {},
        "outputs": [],
        "source": body,
    }
    cells.insert(insert_after + 1, override)
    nb["cells"] = cells
    print(
        f"    injected override cell after cell {insert_after}: {', '.join(assignments)}",
        flush=True,
    )
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
    set_vars: list[str] | None = None,
) -> str:
    """Import a local .ipynb to the workspace; return the workspace path."""
    from databricks.sdk.service.workspace import ImportFormat

    nb_bytes = local_path.read_bytes()
    if strip_pip:
        nb_bytes = _strip_pip_cells(nb_bytes)
    if set_vars:
        nb_bytes = _inject_override_cell(nb_bytes, set_vars)

    stem = local_path.stem  # filename without .ipynb
    ws_path = f"{ws_dir.rstrip('/')}/{stem}"

    # Ensure the target workspace folder exists — import_ raises ResourceDoesNotExist
    # if the parent folder is absent (mkdirs is idempotent, creates parents).
    w.workspace.mkdirs(ws_dir.rstrip("/"))
    w.workspace.import_(
        path=ws_path,
        format=ImportFormat.JUPYTER,
        content=base64.b64encode(nb_bytes).decode(),
        overwrite=True,
    )
    print(f"    imported → {ws_path}", flush=True)
    return ws_path


def _accelerator_type(value: str):
    """Serverless GPU accelerator for ``jobs.Compute.hardware_accelerator``.

    Prefer the SDK enum, but fall back to a ``.value``-bearing shim for platform values
    the installed SDK's ``HardwareAcceleratorType`` enum doesn't list yet — e.g.
    ``GPU_1xH100`` on databricks-sdk 0.115.0, whose enum has only ``GPU_1xA10`` /
    ``GPU_8xH100``. ``as_dict()`` reads ``.value``, so the shim serializes to the exact
    string the Jobs API expects (verified: the API accepts the ``hardware_accelerator``
    string the SDK emits).
    """
    from types import SimpleNamespace

    from databricks.sdk.service import compute

    try:
        return compute.HardwareAcceleratorType(value)
    except ValueError:
        return SimpleNamespace(value=value)


def run_one(
    w,
    local_path: pathlib.Path,
    ws_dir: str,
    deps: list[str],
    env_version: str,
    poll_secs: int,
    strip_pip: bool,
    set_vars: list[str] | None = None,
    hardware_accelerator: str | None = None,
    existing_cluster_id: str | None = None,
    harvest_outputs: bool = False,
    profile: str | None = None,
    new_cluster=None,
) -> bool:
    """Import and run one notebook on Serverless. Returns True on SUCCESS.

    When ``hardware_accelerator`` is set (e.g. ``GPU_1xH100`` / ``GPU_8xH100``), the
    task requests Serverless GPU AI Runtime compute. Deps still come from the
    environment spec (``%pip`` is stripped for jobs, CPU and GPU alike) — pass the
    env5 wheel and the GPU packages via ``--extra-deps`` (e.g. ``pycolmap-cuda12==4.2.1``).

    If ``harvest_outputs`` is True and the run succeeds, exports the run and harvests
    cell outputs back into the local .ipynb file.
    """
    from databricks.sdk.service import compute, jobs

    _mode = (
        f"cluster {existing_cluster_id}"
        if existing_cluster_id
        else f"job cluster {new_cluster.spark_version}" if new_cluster else "serverless"
    )
    tag = f" [{hardware_accelerator}]" if hardware_accelerator else ""
    print(f"\n=== SUBMIT ({_mode}{tag}): {local_path.name} ===", flush=True)

    ws_path = _import_notebook(w, local_path, ws_dir, strip_pip, set_vars=set_vars)

    task_key = "".join(c if c.isalnum() else "_" for c in local_path.stem)[:90]

    if existing_cluster_id or new_cluster:
        # A classic cluster: a warm interactive one (no cold start), or a job cluster created for this run
        # alone and terminated when it ends (--new-cluster). Deps install via the notebook's own %pip (kept, not
        # stripped — classic clusters support %pip), so there is no environment spec or GPU compute request
        # here; --extras/--wheel/--env-version/--hardware-accelerator do not apply in this mode.
        waiter = submit_once(
            w,
            run_name=f"gbx-nb:{task_key}",
            tasks=[
                jobs.SubmitTask(
                    task_key=task_key,
                    existing_cluster_id=existing_cluster_id,
                    new_cluster=new_cluster,
                    notebook_task=jobs.NotebookTask(notebook_path=ws_path),
                    max_retries=0,  # run exactly once — never retry a failed run
                )
            ],
        )
    else:
        gpu_compute = (
            jobs.Compute(hardware_accelerator=_accelerator_type(hardware_accelerator))
            if hardware_accelerator
            else None
        )

        waiter = submit_once(
            w,
            run_name=f"gbx-nb:{task_key}",
            environments=[
                jobs.JobEnvironment(
                    environment_key=ENV_KEY,
                    spec=compute.Environment(
                        environment_version=env_version,
                        # Both CPU and GPU serverless jobs install deps from the environment
                        # spec (%pip is stripped for jobs). For GPU, pass the env5 wheel and the
                        # GPU packages (e.g. pycolmap-cuda12==4.2.1) via --extra-deps.
                        dependencies=deps,
                    ),
                )
            ],
            tasks=[
                jobs.SubmitTask(
                    task_key=task_key,
                    environment_key=ENV_KEY,
                    compute=gpu_compute,
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
    elif harvest_outputs:
        from harvest_notebook_outputs import harvest_run_outputs  # same dir as this script

        try:
            n, unmatched = harvest_run_outputs(run_id, profile or "oauth-fe", local_path)
        except Exception as e:  # a requested harvest that fails must fail the run
            print(f"!!! STOP — output harvest failed ({e.__class__.__name__}: {e})", flush=True)
            return False
        print(f"    harvested outputs into {n} cell(s) of {local_path}"
              + (f"; NOT updated (no match or unsupported result type): code cells {unmatched}"
                 if unmatched else ""), flush=True)
    return ok


def run_jar_task(
    w,
    main_class: str,
    jar: str,
    params: list[str],
    env_version: str,
    poll_secs: int,
) -> bool:
    """Run ONE Serverless JAR task: ``spark_jar_task(main_class, params)`` with the JAR as the environment's
    java dependency (the Serverless JAR task form: environment version 4 or later). Returns True on SUCCESS and
    prints the run's state message and the end of its output."""
    from databricks.sdk.service import compute, jobs

    task_key = "".join(c if c.isalnum() else "_" for c in main_class.rsplit(".", 1)[-1])[:90]
    print(f"\n=== SUBMIT (serverless JAR task): {main_class} {params} ===", flush=True)
    waiter = submit_once(
        w,
        run_name=f"gbx-jar:{task_key}",
        environments=[
            jobs.JobEnvironment(
                environment_key=ENV_KEY,
                spec=compute.Environment(environment_version=env_version, java_dependencies=[jar]),
            )
        ],
        tasks=[
            jobs.SubmitTask(
                task_key=task_key,
                environment_key=ENV_KEY,
                spark_jar_task=jobs.SparkJarTask(main_class_name=main_class, parameters=params),
                max_retries=0,
                disable_auto_optimization=True,  # at most once (see run_one)
            )
        ],
    )
    run_id = waiter.run_id
    print(f"    run_id={run_id}  run_page_url={w.jobs.get_run(run_id=run_id).run_page_url}", flush=True)
    terminal, last, started = _terminal_states(), None, time.time()
    while True:
        r = w.jobs.get_run(run_id=run_id)
        life = r.state.life_cycle_state if r.state else None
        if life != last:
            print(f"    [{int(time.time() - started)}s] life_cycle_state={life}", flush=True)
            last = life
        if life in terminal:
            break
        time.sleep(poll_secs)
    result = r.state.result_state if r.state else None
    print(f"=== RESULT: {main_class}  result_state={result}  ({(r.state.state_message or '')[:500]})", flush=True)
    for t in r.tasks or []:
        try:
            out = w.jobs.get_run_output(run_id=t.run_id)
            for k in ("logs", "error", "error_trace"):
                v = getattr(out, k, None)
                if v:
                    print(f"    task {t.task_key} {k}: {str(v)[-2000:]}", flush=True)
        except Exception as exc:  # noqa: BLE001 - output fetch is best-effort
            print(f"    (no output for {t.task_key}: {exc})", flush=True)
    from databricks.sdk.service.jobs import RunResultState

    return result == RunResultState.SUCCESS


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_dep_list(
    wheel: str,
    extras: list[str],
    extra_deps: list[str],
    env_version: Optional[str] = None,
) -> list[str]:
    env = wheel_env(wheel)
    if env is not None and env_version is not None and str(env_version) != str(env):
        raise SystemExit(
            f"--wheel {wheel} is the env{env} wheel, --env-version is {env_version}: the wheel refuses to import "
            f"on another environment. Fix: pass --env-version {env}, or the +env{env_version} wheel"
        )
    deps: list[str] = [wheel]
    deps.extend(_env_extras_deps(env, extras) if env else _load_extras_deps(extras))
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
            f"Comma-separated geobrix extras whose deps to include (default: {DEFAULT_EXTRAS!r}). "
            "With an env wheel: that wheel's extras (no light_* name)."
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
        help=(
            f"Volume path to the geobrix wheel (default: {DEFAULT_WHEEL!r}). An env wheel "
            "(+env5 / +env6) must match --env-version."
        ),
    )
    parser.add_argument(
        "--env-version",
        metavar="VER",
        default=DEFAULT_ENV_VERSION,
        help=f"Serverless environment version (default: {DEFAULT_ENV_VERSION!r}).",
    )
    parser.add_argument(
        "--hardware-accelerator",
        metavar="ACCEL",
        default=None,
        help=(
            "Request Serverless GPU AI Runtime compute for the task, e.g. "
            "'GPU_1xH100', 'GPU_8xH100', or 'GPU_1xA10'. Omit for CPU serverless. "
            "Deps come from the environment spec as usual (%%pip stripped) — pass the "
            "env5 wheel and the GPU packages via --extra-deps (e.g. pycolmap-cuda12==4.2.1)."
        ),
    )
    parser.add_argument(
        "--existing-cluster-id",
        metavar="CLUSTER_ID",
        default=None,
        help=(
            "Run on an existing (warm) interactive cluster by ID instead of Serverless. "
            "Reuses the running cluster (no cold start); deps install via the notebook's "
            "own %%pip (kept, not stripped). --extras/--wheel/--env-version/"
            "--hardware-accelerator are ignored in this mode."
        ),
    )
    parser.add_argument(
        "--new-cluster",
        metavar="SPARK_VERSION",
        default=None,
        help=(
            "Run each notebook on a job cluster of this Databricks Runtime (e.g. '19.x-scala2.13'), created for "
            "the run and terminated when it ends (nothing left to delete). Dedicated (single user) access mode, "
            "one worker of --node-type. Deps install via the notebook's own %%pip (kept). --extras/--wheel/"
            "--env-version/--hardware-accelerator are ignored in this mode."
        ),
    )
    parser.add_argument(
        "--node-type",
        metavar="NODE_TYPE",
        default="i3.xlarge",
        help="Node type of the --new-cluster job cluster's driver and worker (default: 'i3.xlarge').",
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
    parser.add_argument(
        "--set-var",
        metavar="NAME=VALUE",
        action="append",
        dest="set_vars",
        help=(
            "Inject a Python assignment into the UPLOADED notebook copy (after the "
            "%%run/config cell) so a validation run forces compute the committed "
            "notebook would skip. Example: --set-var FORCE_REBUILD=True forces the "
            "reader/rst_* rebuild steps that are gated on do_overwrite=FORCE_REBUILD, "
            "without changing the committed default. VALUE is inlined as a Python "
            "literal. Repeatable. Applies to every submitted notebook."
        ),
    )
    parser.add_argument(
        "--jar-task",
        metavar="MAIN",
        default=None,
        help=(
            "Run ONE Serverless JAR task with this main class instead of notebooks (needs --jar; --jar-args are "
            "its parameters). The JAR is the environment's java dependency; --env-version must be 4 or later."
        ),
    )
    parser.add_argument("--jar", metavar="VOLPATH", default=None, help="The JAR (a /Volumes path) for --jar-task.")
    parser.add_argument(
        "--jar-args", metavar="CSV", default="", help="Comma-separated parameters of the --jar-task main class."
    )
    parser.add_argument(
        "--harvest-outputs",
        dest="harvest_outputs",
        action="store_true",
        default=False,
        help=(
            "After each successful run, export the run and harvest cell outputs "
            "back into the local .ipynb file (aligning by source code, not cell index). "
            "Strips injected %%pip cells and override cells; preserves local serialization."
        ),
    )

    args = parser.parse_args()

    # Validate --set-var forms early (NAME=VALUE, NAME a valid identifier).
    for sv in args.set_vars or []:
        name, sep, _ = sv.partition("=")
        if not sep or not name.strip().isidentifier():
            parser.error(f"--set-var must be NAME=VALUE with NAME a valid identifier; got {sv!r}")

    if args.jar_task:
        if not args.jar or args.notebooks or args.dirs or args.existing_cluster_id or args.new_cluster:
            parser.error("--jar-task needs --jar and runs alone (no --notebook/--dir/--existing-cluster-id/--new-cluster)")
        if not str(args.env_version).isdigit() or int(args.env_version) < 4:
            parser.error("--jar-task needs --env-version 4 or later (the Serverless JAR task environments)")
        from databricks.sdk import WorkspaceClient

        params = [a.strip() for a in args.jar_args.split(",") if a.strip()]
        w = WorkspaceClient(profile=args.profile)
        ok = run_jar_task(w, args.jar_task, args.jar, params, str(args.env_version), args.poll_secs)
        return 0 if ok else 1

    notebooks = _collect_notebooks(args.notebooks, args.dirs)
    if not notebooks:
        parser.error(
            "Provide at least one notebook via --notebook or --dir."
        )

    extras = [e.strip() for e in args.extras.split(",") if e.strip()]
    extra_deps = _split_deps(args.extra_deps)
    deps = _build_dep_list(args.wheel, extras, extra_deps, args.env_version)

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=args.profile)

    if args.existing_cluster_id and args.new_cluster:
        parser.error("--existing-cluster-id and --new-cluster are exclusive")
    # Classic-cluster modes keep %pip cells (classic clusters install via %pip, unlike
    # Serverless JOB compute where they fail and are stripped).
    if args.existing_cluster_id or args.new_cluster:
        args.strip_pip = False
    new_cluster = None
    if args.new_cluster:
        from databricks.sdk.service import compute

        new_cluster = compute.ClusterSpec(
            spark_version=args.new_cluster,
            node_type_id=args.node_type,
            num_workers=1,
            data_security_mode=compute.DataSecurityMode.SINGLE_USER,
            single_user_name=w.current_user.me().user_name,
            custom_tags={"gbx-run": "notebooks"},
        )

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
    if args.hardware_accelerator:
        print(f"Accelerator: {args.hardware_accelerator}", flush=True)
    if args.existing_cluster_id:
        print(f"Cluster  : {args.existing_cluster_id} (warm; %pip kept, env spec skipped)", flush=True)
    if new_cluster:
        print(f"Cluster  : a job cluster per run, {args.new_cluster} on {args.node_type} (%pip kept)", flush=True)
    if args.set_vars:
        print(f"Override vars: {', '.join(args.set_vars)}", flush=True)
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
            set_vars=args.set_vars,
            hardware_accelerator=args.hardware_accelerator,
            existing_cluster_id=args.existing_cluster_id,
            harvest_outputs=args.harvest_outputs,
            profile=args.profile,
            new_cluster=new_cluster,
        )
        if not ok:
            return 1

    print("\n=== ALL NOTEBOOKS SUCCEEDED ===", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
