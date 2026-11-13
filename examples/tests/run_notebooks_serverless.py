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
from dataclasses import dataclass
from typing import Optional

import _cell_range  # sibling module: run a notebook in pieces (--cells)
import _wheel_guard  # sibling module: force the staged wheel in, then fail the run if another geobrix is imported
import _workspace_sweep  # sibling module: deferred cleanup of the imported notebooks (gbx:workspace:sweep)
from _job_submit import submit_once  # sibling module (script dir is on sys.path)
from _snapshot import snapshot_files  # sibling module (script dir is on sys.path)

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
DEFAULT_EXTRAS = "stac,vizx"
DEFAULT_ENV_VERSION = "6"
# The staged wheels: gbx:data:stage-wheel --env both --versioned puts each env wheel in its own folder of the
# sample-data Volume, <root>/geobrix/<version>/env<N>/ (the root's own wheels are older stagings).
STAGED_WHEEL_ROOT = "/Volumes/geospatial_docs/geobrix/sample-data"
_INIT_PATH = PROJECT_ROOT / "python" / "geobrix" / "src" / "databricks" / "labs" / "gbx" / "__init__.py"


def package_version() -> str:
    """The checkout's geobrix version (``__version__`` of the package)."""
    m = re.search(r'^__version__\s*=\s*"([^"]+)"', _INIT_PATH.read_text(encoding="utf-8"), re.M)
    if not m:
        raise SystemExit(f"no __version__ in {_INIT_PATH}")
    return m.group(1)


def default_wheel(env: int) -> str:
    """The current versioned staged env wheel: ``<root>/geobrix/<version>/env<N>/geobrix-<version>+env<N>-py3-none-any.whl``."""
    v = package_version()
    return f"{STAGED_WHEEL_ROOT}/geobrix/{v}/env{env}/geobrix-{v}+env{env}-py3-none-any.whl"


def default_wheel_env(env_version, new_cluster=None) -> int:
    """The env wheel a run takes when no --wheel is given: a new cluster's runtime decides (DBR 17.3 and 18: env5,
    DBR 19: env6), else the Serverless environment version (5 or 6)."""
    if isinstance(new_cluster, str) and runtime_env(new_cluster) is not None:
        return runtime_env(new_cluster)
    return 5 if str(env_version) == "5" else 6


DEFAULT_WHEEL = default_wheel(int(DEFAULT_ENV_VERSION))
# Each run's time limit (minutes): a notebook that outlives it is ended by the Jobs service, not left to hang.
DEFAULT_TIMEOUT_MIN = 60
DEFAULT_POLL_SECS = 20
# --new-cluster defaults: a DBR 19 job cluster with one worker. m5d.xlarge (x86_64), not the
# Graviton m6gd.xlarge, which has sat for 25 min without capacity on the dev workspace.
DEFAULT_SPARK_VERSION = "19.x-scala2.13"
DEFAULT_NODE_TYPE = "m5d.xlarge"
ACCESS_MODES = ("dedicated", "standard")

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


# The 0.5.x GPU extras are tripwires in 0.6.0 (GPU = CUDA 12): their packages install as a separate step after
# the env wheel, here with --extra-deps. The UDF workers of the Serverless GPU runtime are environment 6.
_GPU_EXTRA_DEPS = {
    "photogrammetry_gpu_env5": "pycolmap-cuda12==4.2.1",
    "models_gpu_env5": "torch==2.9.0,torchvision==0.24.0,segment-geospatial,opencv-python-headless==4.11.0.86",
}
_GPU_EXTRA_DEPS["gpu_env5_all"] = ",".join(_GPU_EXTRA_DEPS.values())


def _canon_extra(name: str) -> str:
    """An extra name as a comparison key: PEP 685 treats case and any run of '-', '_' and '.' as one separator, so
    ``models-gpu-env5`` and ``Models_GPU_Env5`` are the extra ``models_gpu_env5`` (the repo spells its names with '_')."""
    return re.sub(r"[-_.]+", "_", name).lower()


def _refuse_gpu_extra(extra: str) -> None:
    key = _canon_extra(extra)
    if key in _GPU_EXTRA_DEPS:
        raise SystemExit(
            f"--extras {extra}: a 0.5.x GPU extra, dropped in 0.6.0. Fix: --wheel <the geobrix-<version>+env6 wheel> "
            f"--env-version 6 --extra-deps '{_GPU_EXTRA_DEPS[key]}' (the env5 wheel only for a driver-only step)"
        )


def _env_extras_deps(env: int, extras: list[str]) -> list[str]:
    """The pip deps of an env wheel's extras: envs/env<N>/extra-<name>.txt, one requirement per line.

    The old ``light_*`` names are not extras of an env wheel (the env wheel IS the light install; the
    other family's names are tripwires), and the plain wheel's ``[overture]`` / ``[earthdata]`` are not
    on the env wheels: both are refused with what to pass instead.
    """
    envdir = _ENVS_PATH / f"env{env}"
    have = sorted(p.stem.removeprefix("extra-") for p in envdir.glob("extra-*.txt"))
    by_key = {_canon_extra(h): h for h in have}
    out: list[str] = []
    for extra in extras:
        _refuse_gpu_extra(extra)
        if _canon_extra(extra).startswith("light"):
            raise SystemExit(
                f"--extras {extra}: the env{env} wheel needs no light extra (it is the light install); "
                "drop it. Fix: pass only feature extras, e.g. --extras stac,vizx"
            )
        if _canon_extra(extra) not in by_key:
            raise SystemExit(
                f"--extras {extra}: the env{env} wheel has no [{extra}] (its extras: {', '.join(have)}). "
                "Fix: pass its packages with --extra-deps (e.g. --extra-deps 'overturemaps>=1.0.2,<2')"
            )
        for line in (envdir / f"extra-{by_key[_canon_extra(extra)]}.txt").read_text().splitlines():
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

    opt_deps: dict[str, list[str]] = {
        _canon_extra(k): v
        for k, v in data.get("project", {}).get("optional-dependencies", {}).items()
    }

    seen_deps: set[str] = set()
    seen_extras: set[str] = set()
    result: list[str] = []

    def _visit(extra: str) -> None:
        key = _canon_extra(extra)
        if key in seen_extras:
            return
        seen_extras.add(key)
        _refuse_gpu_extra(extra)
        deps = opt_deps.get(key, [])
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


# --- the wheel guard: the run tests the staged wheel, never an older one of the same version -----------------------
# A same-version wheel that is already on a cluster (a cluster library, an earlier install) makes a plain
# `%pip install <staged wheel>` a no-op. On a classic cluster the uploaded copy of every notebook therefore forces
# geobrix in first (`--force-reinstall --no-deps`, the dependencies unchanged), and every mode gets a guard cell after
# the install that fails the run when the driver or a Python worker imports another build (see _wheel_guard.py).
# Only the uploaded copy changes; the committed notebook, and the install cells users copy, stay as they are.
WHEEL_GUARD: dict = {"on": False, "wheel": None, "report_only_without_pip": False}


def _cell_source(cell) -> list:
    src = cell.get("source", [])
    return src.splitlines(keepends=True) if isinstance(src, str) else list(src)


def _force_geobrix_pip(nb_bytes: bytes) -> bytes:
    """Put a forced geobrix install before every `%pip install ... geobrix @ file://...whl` line of the notebook."""
    nb = json.loads(nb_bytes)
    total = 0
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        lines, n = _wheel_guard.force_pip_lines(_cell_source(cell))
        if n:
            cell["source"] = lines
            total += n
    if total:
        print(f"    forced the geobrix install ({total} %pip line(s): --force-reinstall --no-deps first)", flush=True)
    return json.dumps(nb).encode()


def _notebook_wheel(nb_bytes: bytes) -> Optional[str]:
    """The wheel the notebook's own `%pip` lines install, or None."""
    for cell in json.loads(nb_bytes).get("cells", []):
        if cell.get("cell_type") == "code":
            wheel = _wheel_guard.pip_wheel_of(_cell_source(cell))
            if wheel:
                return wheel
    return None


_INSTALL_CELL = re.compile(r"^\s*(%pip|%restart_python|%run)|restartPython\(", re.M)  # %run: a notebook it runs may install geobrix


def _inject_guard_cell(nb_bytes: bytes, wheel: Optional[str]) -> bytes:
    """Add the guard cell after the last install, restart or %run cell of the notebook (else before its first code cell).
    With no `wheel` the cell only reports what the driver and the workers import."""
    nb = json.loads(nb_bytes)
    cells = nb.get("cells", [])
    code = [i for i, c in enumerate(cells) if c.get("cell_type") == "code"]
    installs = [i for i in code if _INSTALL_CELL.search("".join(_cell_source(cells[i])))]
    at = installs[-1] + 1 if installs else (code[0] if code else 0)
    cells.insert(at, {
        "cell_type": "code", "execution_count": 0, "metadata": {}, "outputs": [],
        "source": _wheel_guard.guard_source(wheel=wheel).splitlines(keepends=True),
    })
    nb["cells"] = cells
    print(f"    wheel guard cell at {at}: " + (f"driver and workers must import {wheel}" if wheel else "reports the imported geobrix"), flush=True)
    return json.dumps(nb).encode()


def _warn_cluster_wheel_libraries(w, cluster_id: str, wheel: str) -> None:
    """Say when the cluster carries a geobrix wheel library: it is what a stale run would have imported."""
    try:
        libs = [fs.library.whl for fs in w.libraries.cluster_status(cluster_id)
                if getattr(fs, "library", None) and getattr(fs.library, "whl", None) and "geobrix" in fs.library.whl]
    except Exception as exc:  # noqa: BLE001 - advisory only
        print(f"    (could not list the libraries of {cluster_id}: {type(exc).__name__})", flush=True)
        return
    for lib in libs:
        print(f"    NOTE: cluster {cluster_id} carries the geobrix library {lib}; the notebook forces {wheel} over it "
              "and the wheel guard fails the run if a worker still imports the library", flush=True)


# --pip-wheel: the wheel the `%pip install ... @ file://<...>.whl` lines of an uploaded copy install instead of theirs
PIP_WHEEL: Optional[str] = None
# --keep-notebooks: the imports are not recorded, so gbx:workspace:sweep never removes them.
KEEP_NOTEBOOKS = False


def _record_import(w, ws_path: str) -> None:
    """Record an imported notebook or file for gbx:workspace:sweep (judged by its age until its run ends)."""
    try:
        if KEEP_NOTEBOOKS:  # an earlier run may have recorded the same path
            _workspace_sweep.forget(w, [ws_path])
            return
        _workspace_sweep.record(w, [ws_path], scope="notebooks")
    except Exception as exc:  # noqa: BLE001 - the run does not depend on the record
        print(f"    (could not record {ws_path} for the sweep: {exc})", flush=True)


def _release_import(w, ws_path: str, ok: bool) -> None:
    """At the end of a run: keep the notebook for the window (60 min, 24 h after a failure); the sweep removes it after."""
    if not KEEP_NOTEBOOKS:
        _workspace_sweep.release(w, [ws_path], scope="notebooks", succeeded=ok, out=lambda m: print(f"    {m}", flush=True))
_PIP_FILE_WHEEL = re.compile(r"@\s*file://(/Volumes/[^\"'\s]+\.whl)")


def _repoint_pip_wheel(nb_bytes: bytes, wheel: str) -> bytes:
    """Point every `%pip install ... @ file:///Volumes/....whl` line of the notebook at `wheel` (a `/Volumes` path), so
    a probe installs the wheel of an isolated staging folder. Only `%pip` lines change; the committed notebook is not
    touched."""
    nb = json.loads(nb_bytes)
    changed = 0
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source", [])
        lines = src.splitlines(keepends=True) if isinstance(src, str) else list(src)
        out = []
        for line in lines:
            if line.lstrip().startswith("%pip") and _PIP_FILE_WHEEL.search(line):
                line = _PIP_FILE_WHEEL.sub(f"@ file://{wheel}", line)
                changed += 1
            out.append(line)
        cell["source"] = out
    if changed:
        print(f"    %pip wheel -> {wheel} ({changed} line(s))", flush=True)
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
# Job cluster (--new-cluster)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class JobCluster:
    """A cluster made for one run: Databricks terminates it when the run ends.

    ``access_mode`` is ``dedicated`` (single user: ``user_name``) or ``standard``
    (``USER_ISOLATION``, where user code runs isolated from other users).
    """

    spark_version: str = DEFAULT_SPARK_VERSION
    node_type: str = DEFAULT_NODE_TYPE
    num_workers: int = 1
    access_mode: str = "dedicated"
    user_name: Optional[str] = None
    jar: Optional[str] = None  # a /Volumes path: installed as a library of the cluster (the heavy tier)
    wheel: Optional[str] = None  # a /Volumes path: installed as a library of the cluster (--python-task, notebooks)
    extra_jars: tuple = ()  # more /Volumes JARs installed as libraries (--python-task, --extra-jar)
    # a Databricks Container Service image URL (--docker-image): the cluster starts from it, with UC Volumes on
    docker_image: Optional[str] = None


def _cluster_spec(cluster: JobCluster):
    """The ``new_cluster`` of the run's task."""
    from databricks.sdk.service import compute

    dedicated = cluster.access_mode == "dedicated"
    return compute.ClusterSpec(
        spark_version=cluster.spark_version,
        node_type_id=cluster.node_type,
        num_workers=cluster.num_workers,
        data_security_mode=(
            compute.DataSecurityMode.SINGLE_USER
            if dedicated
            else compute.DataSecurityMode.USER_ISOLATION
        ),
        single_user_name=cluster.user_name if dedicated else None,
        # No worker means a single node cluster: without this profile the cluster has no executor at all and
        # the first Spark job waits for one forever.
        spark_conf=_spark_conf(cluster),
        custom_tags={"ResourceClass": "SingleNode"} if cluster.num_workers == 0 else None,
        docker_image=compute.DockerImage(url=cluster.docker_image) if cluster.docker_image else None,
        aws_attributes=_aws_attributes(cluster),
    )


def _aws_attributes(cluster: JobCluster):
    """On-demand nodes, the driver and every worker. A spot request with fallback loses minutes to capacity retries
    before a start (a DCS image start measured 2.5 min of them), and a probe's timing must not include that.
    ``None`` for an Azure node type (``Standard_*``), where ``aws_attributes`` does not apply."""
    if cluster.node_type.startswith("Standard_"):
        return None
    from databricks.sdk.service import compute

    return compute.AwsAttributes(
        availability=compute.AwsAvailability.ON_DEMAND,
        first_on_demand=cluster.num_workers + 1,
        zone_id="auto",  # the workspace's default zone can be throttled for hours: let Databricks pick one
    )


def _spark_conf(cluster: JobCluster):
    """The cluster's Spark config: the single node profile for no worker, and UC Volumes for a DCS image (an image
    cluster has no `/Volumes` without it, `installation/dcs.mdx`)."""
    conf = {}
    if cluster.num_workers == 0:
        # No worker means a single node cluster: without this profile the cluster has no executor at all and
        # the first Spark job waits for one forever.
        conf.update({"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*, 4]"})
    if cluster.docker_image:
        conf["spark.databricks.unityCatalog.volumes.enabled"] = "true"
    return conf or None


def _cluster_libraries(cluster: JobCluster) -> list:
    """The libraries of the run's task: the wheel, the JAR and any extra JARs that were given, else none."""
    from databricks.sdk.service import compute

    libs = [compute.Library(whl=cluster.wheel)] if cluster.wheel else []
    libs += [compute.Library(jar=j) for j in ([cluster.jar] if cluster.jar else []) + list(cluster.extra_jars)]
    return libs


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

def _select_piece(nb_bytes: bytes, cells: str, prelude: Optional[str],
                  base_dir: Optional[pathlib.Path] = None) -> bytes:
    """The notebook with only the piece ``cells`` (``A:B``) and its prelude (see _cell_range.py)."""
    nb = json.loads(nb_bytes)
    out, rng, pre = _cell_range.select_cells(nb, cells, prelude, base_dir)
    print(f"    piece: cells {_cell_range.range_label(rng)}; prelude cells "
          f"{', '.join(str(i + 1) for i in pre) or 'none'}", flush=True)
    return json.dumps(out).encode()


def _import_notebook(
    w,
    local_path: pathlib.Path,
    ws_dir: str,
    strip_pip: bool,
    set_vars: list[str] | None = None,
    guard: bool = True,
    cells: Optional[str] = None,
    prelude: Optional[str] = None,
) -> str:
    """Import a local .ipynb to the workspace; return the workspace path (`guard=False` for a %run dependency).
    `cells` uploads only that piece (``A:B``) and its prelude."""
    from databricks.sdk.service.workspace import ImportFormat

    nb_bytes = local_path.read_bytes()
    if cells:
        nb_bytes = _select_piece(nb_bytes, cells, prelude, local_path.parent)
    if strip_pip:
        nb_bytes = _strip_pip_cells(nb_bytes)
    elif PIP_WHEEL:
        nb_bytes = _repoint_pip_wheel(nb_bytes, PIP_WHEEL)
    guard_wheel = None
    if guard and WHEEL_GUARD["on"]:
        if not strip_pip:  # a classic cluster keeps the %pip cells: put the staged wheel in over any same-version geobrix
            nb_bytes = _force_geobrix_pip(nb_bytes)
        guard_wheel = (None if strip_pip else _notebook_wheel(nb_bytes)) or WHEEL_GUARD["wheel"]
        # an existing cluster, a notebook with no %pip of geobrix and no --wheel given: the cluster's own geobrix library is
        # what the run uses on purpose (the DEFAULT wheel is not a promise), so the guard only reports what is imported
        report_only = WHEEL_GUARD["report_only_without_pip"] and not strip_pip and not _notebook_wheel(nb_bytes)
        nb_bytes = _inject_guard_cell(nb_bytes, None if report_only else guard_wheel)
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
    _record_import(w, ws_path)
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


# Notebooks whose run succeeded but whose output harvest failed (reported at the end of the launch).
HARVEST_FAILURES: list[str] = []


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
    job_cluster: JobCluster | None = None,
    timeout_min: int = DEFAULT_TIMEOUT_MIN,
    harvest_path: pathlib.Path | None = None,
    on_submit=None,
    cells: Optional[str] = None,
    prelude: Optional[str] = None,
    run_context: Optional[dict] = None,
) -> bool:
    """Import and run one notebook on Serverless. Returns True on SUCCESS.

    ``on_submit(run_id)`` is called once the run exists (a leased pool cluster writes it on its lease).

    ``timeout_min`` is the run's time limit. ``harvest_path`` is where harvested outputs go when it is not
    ``local_path`` (the launcher runs a snapshot copy of each notebook, so a later edit cannot change a live run).

    When ``hardware_accelerator`` is set (e.g. ``GPU_1xH100`` / ``GPU_8xH100``), the
    task requests Serverless GPU AI Runtime compute. Deps still come from the
    environment spec (``%pip`` is stripped for jobs, CPU and GPU alike) — pass the
    env6 wheel (the GPU runtime's UDF workers are environment 6) and the GPU packages via
    ``--extra-deps`` (e.g. ``pycolmap-cuda12==4.2.1``).

    With ``existing_cluster_id`` the run attaches to that cluster; with ``job_cluster``
    it runs on a cluster made for it (terminated when the run ends). Either way the
    notebook installs its own deps with ``%pip``.

    If ``harvest_outputs`` is True and the run succeeds, exports the run and harvests
    cell outputs back into the local .ipynb file (a ``%run`` cell gets what its notebook printed), with the
    provenance (``run_context``: where it ran) in the metadata. ``cells`` runs and harvests one piece (``A:B``).
    """
    from databricks.sdk.service import compute, jobs

    if existing_cluster_id:
        _mode = f"cluster {existing_cluster_id}"
    elif job_cluster:
        _mode = f"new {job_cluster.access_mode} cluster {job_cluster.spark_version}"
    else:
        _mode = "serverless"
    tag = f" [{hardware_accelerator}]" if hardware_accelerator else ""
    piece = f" cells {cells}" if cells else ""
    print(f"\n=== SUBMIT ({_mode}{tag}): {local_path.name}{piece} ===", flush=True)

    ws_path = _import_notebook(w, local_path, ws_dir, strip_pip, set_vars=set_vars, cells=cells, prelude=prelude)

    task_key = "".join(c if c.isalnum() else "_" for c in local_path.stem)[:90]

    if existing_cluster_id or job_cluster:
        # A classic cluster: the warm interactive one (no cold start), or a job cluster
        # made for this run. Deps install via the notebook's own %pip (kept, not
        # stripped — classic clusters support %pip), so there is no environment spec or
        # GPU compute request here; --extras/--env-version/--hardware-accelerator do not
        # apply in this mode. --wheel is installed as a library of a new cluster, so a notebook
        # without a %pip cell still imports databricks.labs.gbx.
        where = (
            {"existing_cluster_id": existing_cluster_id}
            if existing_cluster_id
            else {"new_cluster": _cluster_spec(job_cluster)}
        )
        if job_cluster and _cluster_libraries(job_cluster):
            where["libraries"] = _cluster_libraries(job_cluster)
        waiter = submit_once(
            w,
            run_name=f"gbx-nb:{task_key}",
            timeout_seconds=timeout_min * 60,
            tasks=[
                jobs.SubmitTask(
                    task_key=task_key,
                    notebook_task=jobs.NotebookTask(notebook_path=ws_path),
                    max_retries=0,  # run exactly once — never retry a failed run
                    **where,
                )
            ],
        )
        if on_submit is not None:
            on_submit(waiter.run_id)
    else:
        gpu_compute = (
            jobs.Compute(hardware_accelerator=_accelerator_type(hardware_accelerator))
            if hardware_accelerator
            else None
        )

        waiter = submit_once(
            w,
            run_name=f"gbx-nb:{task_key}",
            timeout_seconds=timeout_min * 60,
            environments=[
                jobs.JobEnvironment(
                    environment_key=ENV_KEY,
                    spec=compute.Environment(
                        environment_version=env_version,
                        # Both CPU and GPU serverless jobs install deps from the environment
                        # spec (%pip is stripped for jobs). For GPU, pass the env6 wheel (the UDF
                        # workers are environment 6) and the GPU packages via --extra-deps.
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
        cluster_id = getattr(getattr(t, "cluster_instance", None), "cluster_id", None)
        if job_cluster and cluster_id:
            print(f"    task {t.task_key} job cluster: {cluster_id}", flush=True)
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
    _release_import(w, ws_path, ok)
    if not ok:
        print(f"!!! STOP — {local_path.name} did not SUCCEED ({result}).", flush=True)
    elif harvest_outputs:
        harvest(run_id, profile, local_path, harvest_path or local_path, cells=cells, run_context=run_context)
    return ok


def harvest(run_id, profile, snapshot_path: pathlib.Path, target: pathlib.Path, cells: Optional[str] = None,
            run_context: Optional[dict] = None, model: Optional[dict] = None) -> bool:
    """Write the run's outputs into ``target`` (see harvest_notebook_outputs.py). A failure is recorded in
    HARVEST_FAILURES (the launch then exits 1) and leaves every file as it was."""
    import harvest_notebook_outputs as hno  # same dir as this script

    try:
        rng = None
        if cells:
            n_snap = len(json.loads(snapshot_path.read_bytes()).get("cells", []))
            n_now = len(json.loads(target.read_bytes()).get("cells", []))
            if n_snap != n_now:
                raise hno.HarvestError(
                    f"{target.name} has {n_now} cells now and had {n_snap} at launch: the piece's cell numbers no "
                    "longer name the same cells. Fix: rerun the piece on the current notebook")
            rng = _cell_range.parse_range(cells, n_now)
        rep = hno.harvest_run_outputs(run_id, profile, target, cells=rng, run_context=run_context, model=model)
    except Exception as e:  # noqa: BLE001 — the run itself succeeded: say so loudly, and fail the launch at its end
        HARVEST_FAILURES.append(target.name)
        print(
            f"!!! HARVEST FAILED for {target.name} ({e.__class__.__name__}: {e}); the run SUCCEEDED, the local "
            "notebook is unchanged",
            flush=True,
        )
        return False
    prov = rep.provenance or {}
    print(f"    harvested run {run_id} into {rep.updated} cell(s) of {target}"
          + (f" (cells {cells})" if cells else "")
          + (f"; cleared, not in this run: cells {[i + 1 for i in rep.not_run]}" if rep.not_run else ""), flush=True)
    print(f"    provenance: geobrix {prov.get('version')} git_sha={(prov.get('git_sha') or 'none')[:12]} "
          f"wheel_sha256={(prov.get('wheel_sha256') or 'none')[:12]} runtime={(run_context or {}).get('label')}"
          + ("" if rep.install_cell is None else f"; install cell {rep.install_cell + 1} shows it")
          if prov else "    provenance: none (no wheel guard ran): only the launcher's view is recorded", flush=True)
    return True


def run_jar_task(
    w,
    main_class: str,
    jar: str,
    params: list[str],
    env_version: str,
    poll_secs: int,
    timeout_min: int = DEFAULT_TIMEOUT_MIN,
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
        timeout_seconds=timeout_min * 60,
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
    message = (r.state.state_message or "") if r.state else ""
    print(f"=== RESULT: {main_class}  result_state={result}  ({message[:500]})", flush=True)
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


def _poll_run(w, run_id: int, poll_secs: int) -> object:
    """Poll a run to a terminal state, printing each change of life cycle state; returns the final run."""
    terminal, last, started = _terminal_states(), None, time.time()
    while True:
        r = w.jobs.get_run(run_id=run_id)
        life = r.state.life_cycle_state if r.state else None
        if life != last:
            print(f"    [{int(time.time() - started)}s] life_cycle_state={life}", flush=True)
            last = life
        if life in terminal:
            return r
        time.sleep(poll_secs)


def run_python_task(
    w,
    local_path: pathlib.Path,
    ws_dir: str,
    params: list[str],
    job_cluster: JobCluster,
    poll_secs: int,
    timeout_min: int = DEFAULT_TIMEOUT_MIN,
) -> bool:
    """Run ONE ``spark_python_task`` on a job cluster made for the run (terminated when the run ends).

    The local ``.py`` file is imported into the workspace as a file, and the cluster installs the wheel and the
    JARs of ``job_cluster`` as libraries before the file runs. Returns True on SUCCESS; prints the run's state
    message and the task's stdout (``logs``).
    """
    from databricks.sdk.service import jobs, workspace

    ws_path = f"{ws_dir.rstrip('/')}/{local_path.name}"
    w.workspace.mkdirs(ws_dir)
    w.workspace.import_(
        ws_path,
        format=workspace.ImportFormat.AUTO,
        content=base64.b64encode(local_path.read_bytes()).decode(),
        overwrite=True,
    )
    _record_import(w, ws_path)
    kind = w.workspace.get_status(ws_path).object_type
    if kind != workspace.ObjectType.FILE:  # a notebook would run as a notebook, not as the Python file
        print(f"!!! STOP - {ws_path} was imported as {kind}, not as a workspace file.", flush=True)
        return False
    task_key = "".join(c if c.isalnum() else "_" for c in local_path.stem)[:90]
    print(
        f"\n=== SUBMIT (new {job_cluster.access_mode} cluster {job_cluster.spark_version}, python task): "
        f"{ws_path} {params} ===",
        flush=True,
    )
    waiter = submit_once(
        w,
        run_name=f"gbx-py:{task_key}",
        timeout_seconds=timeout_min * 60,
        tasks=[
            jobs.SubmitTask(
                task_key=task_key,
                spark_python_task=jobs.SparkPythonTask(python_file=ws_path, parameters=params),
                new_cluster=_cluster_spec(job_cluster),
                libraries=_cluster_libraries(job_cluster),
                max_retries=0,  # run exactly once
            )
        ],
    )
    run_id = waiter.run_id
    print(f"    run_id={run_id}  run_page_url={w.jobs.get_run(run_id=run_id).run_page_url}", flush=True)
    r = _poll_run(w, run_id, poll_secs)
    result = r.state.result_state if r.state else None
    message = (r.state.state_message or "") if r.state else ""
    print(f"=== RESULT: {local_path.name}  result_state={result}  ({message[:500]})", flush=True)
    for t in r.tasks or []:
        cluster_id = getattr(getattr(t, "cluster_instance", None), "cluster_id", None)
        if cluster_id:
            print(f"    task {t.task_key} job cluster: {cluster_id}", flush=True)
        try:
            out = w.jobs.get_run_output(run_id=t.run_id)
            for k in ("logs", "error", "error_trace"):
                v = getattr(out, k, None)
                if v:
                    print(f"    task {t.task_key} {k}:\n{v}", flush=True)
        except Exception as exc:  # noqa: BLE001 - output fetch is best-effort
            print(f"    (no output for {t.task_key}: {exc})", flush=True)
    from databricks.sdk.service.jobs import RunResultState

    ok = result == RunResultState.SUCCESS
    _release_import(w, ws_path, ok)
    return ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def runtime_env(spark_version: str) -> Optional[int]:
    """The wheel environment a classic runtime takes: 5 for DBR 17.3 and 18, 6 for DBR 19 and later, else None."""
    m = re.match(r"(\d+)\.", spark_version or "")
    if not m:
        return None
    major = int(m.group(1))
    return 5 if major in (17, 18) else 6 if major >= 19 else None


def gpu_wheel_warning(wheel: Optional[str], hardware_accelerator: Optional[str]) -> Optional[str]:
    """A note when an env5 wheel goes to Serverless GPU: the UDF workers there run environment 6 whatever the
    driver's environment, so the wheel's import guard refuses in any UDF. (A driver-only GPU step is fine.)"""
    if wheel and hardware_accelerator and wheel_env(wheel) == 5:
        return (
            f"--wheel {wheel} is the env5 wheel and the run asks for Serverless GPU ({hardware_accelerator}). The UDF "
            "workers of Serverless GPU run environment 6 even when the driver runs environment 5, so a notebook that "
            "runs UDFs fails with 'the runtime is env6'. Fix: pass the +env6 wheel and --env-version 6. "
            "Ignore this for a driver-only GPU step."
        )
    return None


def _check_wheel_env(wheel: Optional[str], env_version: Optional[str], runtime: Optional[str] = None) -> None:
    """Refuse an env wheel built for another environment than the run's: it refuses to import there.

    ``runtime`` names the classic cluster runtime that fixed ``env_version`` (--python-task); without it the
    environment is the --env-version of a Serverless run."""
    env = wheel_env(wheel) if wheel else None
    if env is None or env_version is None or str(env_version) == str(env):
        return
    if runtime:
        raise SystemExit(
            f"--wheel {wheel} is the env{env} wheel, runtime {runtime} takes the env{env_version} wheel: the wheel "
            f"refuses to import on another environment. Fix: pass the +env{env_version} wheel, or a runtime that "
            f"takes env{env} (DBR 17.3 and 18: env5, DBR 19: env6)"
        )
    raise SystemExit(
        f"--wheel {wheel} is the env{env} wheel, --env-version is {env_version}: the wheel refuses to import "
        f"on another environment. Fix: pass --env-version {env}, or the +env{env_version} wheel"
    )


def _build_dep_list(
    wheel: Optional[str],
    extras: list[str],
    extra_deps: list[str],
    env_version: Optional[str] = None,
) -> list[str]:
    if wheel is None:  # --no-wheel: a plain-pyspark probe installs only what --extra-deps names
        return list(extra_deps)
    env = wheel_env(wheel)
    _check_wheel_env(wheel, env_version)
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
        "--keep-notebooks",
        action="store_true",
        help=(
            "Do not record the imported notebooks, so gbx:workspace:sweep never removes them. By default each "
            "stays 60 min after its run ends (24 h after a failure), then the sweep removes it."
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
        default=None,
        help=(
            f"Volume path to the geobrix wheel (default: the versioned staged env wheel for the run's environment, "
            f"{default_wheel(6)!r} for environment 6 and DBR 19, the env5 one for environment 5 and DBR 17.3/18). "
            "An env wheel (+env5 / +env6) must match --env-version."
        ),
    )
    parser.add_argument(
        "--no-wheel-guard",
        action="store_true",
        help=(
            "Do not add the wheel guard cell (and, on a classic cluster, do not force the geobrix install first). "
            "Default: every notebook gets both, so a same-version geobrix already on the compute cannot stand in for the "
            "staged wheel; the guard fails the run when the driver or a Python worker imports another build."
        ),
    )
    parser.add_argument(
        "--no-wheel",
        action="store_true",
        help=(
            "Install no geobrix wheel (and no --extras): for a probe notebook that uses plain pyspark. "
            "Only --extra-deps are installed. Exclusive with --wheel."
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
            "env6 wheel (the UDF workers of the GPU runtime are environment 6) and the GPU packages via "
            "--extra-deps (e.g. pycolmap-cuda12==4.2.1)."
        ),
    )
    parser.add_argument(
        "--pool",
        metavar="NAME",
        default=None,
        help=(
            "Lease an idle cluster of the pool NAME (gbx:bench:pool up --name NAME) instead of Serverless: no cluster "
            "start per run, one run per cluster at a time, an ended cluster is started, and --jar (or none) is made "
            "the cluster's jar. Exclusive with --new-cluster and --existing-cluster-id."
        ),
    )
    parser.add_argument(
        "--pool-volume",
        metavar="VOLROOT",
        default=None,
        help="--pool: the Volume root that holds the lease files (default: GBX_BUNDLE_VOLUME_* or the sample-data Volume).",
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
        nargs="?",
        const=True,
        default=None,
        metavar="SPARK_VERSION",
        help=(
            "Run on a classic job cluster made for each run, which Databricks terminates "
            "when the run ends, instead of Serverless. An optional runtime (e.g. "
            "'19.x-scala2.13') sets the cluster's Databricks Runtime, as --spark-version does. "
            "Deps install via the notebook's own %%pip (kept, not stripped); --wheel and --jar are also "
            "installed as libraries of the cluster. --extras/--env-version/--hardware-accelerator "
            "are ignored in this mode."
        ),
    )
    parser.add_argument(
        "--access-mode",
        choices=ACCESS_MODES,
        default="dedicated",
        help=(
            "--new-cluster access mode: dedicated (single user: you) or standard "
            "(USER_ISOLATION). Default: dedicated."
        ),
    )
    parser.add_argument(
        "--spark-version",
        default=DEFAULT_SPARK_VERSION,
        help=f"--new-cluster runtime (default: {DEFAULT_SPARK_VERSION!r}).",
    )
    parser.add_argument(
        "--node-type",
        default=DEFAULT_NODE_TYPE,
        help=f"--new-cluster driver and worker type (default: {DEFAULT_NODE_TYPE!r}).",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=1,
        help="--new-cluster worker count (default: 1).",
    )
    parser.add_argument(
        "--docker-image",
        default=None,
        help=(
            "--new-cluster: start the cluster from this Databricks Container Service image (for example "
            "mjohns797/geobrix:0.6.0-env6-x86; match the node type's architecture). UC Volumes are enabled on it. "
            "The image carries the wheel and the JAR: give no --jar and no %%pip of the wheel."
        ),
    )
    parser.add_argument(
        "--profile",
        metavar="PROFILE",
        default=os.environ.get("DATABRICKS_CONFIG_PROFILE") or None,
        help=(
            "Databricks config profile to use (required; DATABRICKS_CONFIG_PROFILE counts as given). No profile is "
            "picked for you."
        ),
    )
    parser.add_argument(
        "--poll-secs",
        type=int,
        default=DEFAULT_POLL_SECS,
        help=f"Polling interval in seconds (default: {DEFAULT_POLL_SECS}).",
    )
    parser.add_argument(
        "--timeout-min",
        type=int,
        default=DEFAULT_TIMEOUT_MIN,
        help=(
            f"Time limit of each run in minutes (default: {DEFAULT_TIMEOUT_MIN}). The Jobs service ends a run "
            "that outlives it; size it to the notebook."
        ),
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
        "--pip-wheel",
        metavar="VOLPATH",
        default=None,
        help=(
            "Classic modes (%%pip kept): point every `%%pip install ... @ file:///Volumes/....whl` line of the "
            "uploaded copy at this /Volumes wheel, for example the env6 wheel of an isolated staging folder. "
            "Without --wheel it is also the wheel of the environment and the cluster library."
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
    parser.add_argument(
        "--jar",
        metavar="VOLPATH",
        default=None,
        help=(
            "The JAR (a /Volumes path): for --jar-task, the environment's java dependency; with --new-cluster, "
            "a library of the cluster (the heavy tier; also for --python-task)."
        ),
    )
    parser.add_argument(
        "--jar-args", metavar="CSV", default="", help="Comma-separated parameters of the --jar-task main class."
    )
    parser.add_argument(
        "--python-task",
        metavar="PY",
        default=None,
        help=(
            "Run ONE spark_python_task (a local .py file, imported into --ws-dir) on a new job cluster instead of "
            "notebooks. Needs --new-cluster. The cluster installs --wheel, --jar and each --extra-jar as libraries; "
            "--python-args are the file's parameters."
        ),
    )
    parser.add_argument(
        "--python-args", metavar="CSV", default="", help="Comma-separated parameters of the --python-task file."
    )
    parser.add_argument(
        "--extra-jar",
        metavar="VOLPATH",
        action="append",
        dest="extra_jars",
        default=[],
        help="With --python-task: one more JAR (a /Volumes path) to install as a library of the cluster. Repeatable.",
    )
    parser.add_argument(
        "--cells",
        metavar="A:B",
        action="append",
        default=None,
        help=(
            "Run the notebook in pieces: cells A to B (numbered from 1 over all cells, both included; A: and :B are "
            "open). Repeat for more pieces: each piece is its own run, in order, and stops the launch on a failure. "
            "The uploaded piece keeps its cells plus the setup cells before it (installs, %%run, imports, "
            "definitions, literal assignments); the state between pieces is what the notebook persists (tables, "
            "files) and its FORCE_* flags. A piece that needs a skipped cell's name is refused before submitting. "
            "With --harvest-outputs each piece writes only its own cells."
        ),
    )
    parser.add_argument(
        "--prelude",
        metavar="LIST",
        default=None,
        help="With --cells: more cells to run before each piece (e.g. '5' or '5,8:9'), besides the automatic setup cells.",
    )
    parser.add_argument(
        "--harvest-run-id",
        metavar="RUN_ID",
        type=int,
        default=None,
        help=(
            "Harvest an earlier run of the ONE --notebook (with --cells for a piece) instead of running it: the "
            "run's outputs and provenance are written as --harvest-outputs would. Give the --env-version / "
            "--new-cluster the run had, for the runtime label."
        ),
    )
    parser.add_argument(
        "--harvest-outputs",
        dest="harvest_outputs",
        action="store_true",
        default=False,
        help=(
            "After each successful run, export the run and write ITS outputs into the local .ipynb: every covered "
            "code cell gets exactly this run's outputs or none, the install cell one line naming the wheel and "
            "runtime, metadata.geobrix_run the provenance, and a %%run cell what its notebook printed. An output that cannot be converted fails the harvest and writes nothing; a failed harvest "
            "exits 1."
        ),
    )

    args = parser.parse_args()

    if not args.profile:
        parser.error("--profile is required (or DATABRICKS_CONFIG_PROFILE): no profile is picked for you")
    if args.no_wheel and args.wheel:
        parser.error("--no-wheel and --wheel are exclusive: --no-wheel installs no geobrix wheel")
    explicit_wheel = args.wheel  # None when the default is used below
    if not args.no_wheel and args.wheel is None:
        # the %pip wheel is also the cluster library: two different wheels give the workers another geobrix
        new_rt = args.new_cluster if isinstance(args.new_cluster, str) else (args.spark_version if args.new_cluster else None)
        args.wheel = args.pip_wheel or default_wheel(default_wheel_env(args.env_version, new_rt))
    if args.prelude and not args.cells:
        parser.error("--prelude goes with --cells")
    for spec in args.cells or []:
        try:
            _cell_range.parse_range(spec, 10**9)  # the form only; the bounds are checked against each notebook
        except ValueError as exc:
            parser.error(str(exc).split(":")[0] + ": expected A:B (cell numbers from 1, both included)")

    # Validate --set-var forms early (NAME=VALUE, NAME a valid identifier).
    for sv in args.set_vars or []:
        name, sep, _ = sv.partition("=")
        if not sep or not name.strip().isidentifier():
            parser.error(f"--set-var must be NAME=VALUE with NAME a valid identifier; got {sv!r}")

    if args.jar_task and args.extra_jars:
        parser.error("--extra-jar goes with --python-task (--jar-task runs the one --jar)")
    if args.pip_wheel and not args.pip_wheel.startswith("/Volumes/"):
        parser.error("--pip-wheel takes a /Volumes path to a .whl")
    global PIP_WHEEL, KEEP_NOTEBOOKS
    PIP_WHEEL = args.pip_wheel
    KEEP_NOTEBOOKS = bool(args.keep_notebooks)
    # the guard compares with the wheel the run installs; a DCS image or --no-wheel names none, so there is nothing to compare
    WHEEL_GUARD.update(
        on=bool(args.wheel) and not args.no_wheel_guard and not args.docker_image,
        wheel=args.wheel,
        report_only_without_pip=bool(args.existing_cluster_id or args.pool) and not (explicit_wheel or args.pip_wheel),
    )
    if args.docker_image and (not args.new_cluster or args.jar):
        parser.error(
            "--docker-image needs --new-cluster and takes no --jar: the image carries the wheel and the one geobrix JAR"
        )

    if args.jar_task:
        if not args.jar or args.notebooks or args.dirs or args.existing_cluster_id or args.new_cluster:
            parser.error("--jar-task needs --jar and runs alone (no --notebook/--dir/--existing-cluster-id/--new-cluster)")
        if not str(args.env_version).isdigit() or int(args.env_version) < 4:
            parser.error("--jar-task needs --env-version 4 or later (the Serverless JAR task environments)")
        from databricks.sdk import WorkspaceClient

        params = [a.strip() for a in args.jar_args.split(",") if a.strip()]
        w = WorkspaceClient(profile=args.profile)
        ok = run_jar_task(
            w, args.jar_task, args.jar, params, str(args.env_version), args.poll_secs, args.timeout_min
        )
        return 0 if ok else 1

    if args.python_task:
        if not args.new_cluster or args.notebooks or args.dirs or args.existing_cluster_id or args.hardware_accelerator:
            parser.error(
                "--python-task needs --new-cluster and runs alone (no --notebook/--dir/--existing-cluster-id/"
                "--hardware-accelerator)"
            )
        local = pathlib.Path(args.python_task).resolve()
        if not local.is_file() or local.suffix != ".py":
            parser.error(f"--python-task {args.python_task}: not a .py file")
        runtime = args.new_cluster if isinstance(args.new_cluster, str) else args.spark_version
        try:
            _check_wheel_env(args.wheel, runtime_env(runtime), runtime)
        except SystemExit as exc:
            parser.error(str(exc.code))
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(profile=args.profile)
        me = w.current_user.me().user_name
        _workspace_sweep.sweep_at_launch(w, ("notebooks",), me=me)
        snap = snapshot_files([local], prefix="gbx-py-")
        cluster = JobCluster(
            spark_version=args.new_cluster if isinstance(args.new_cluster, str) else args.spark_version,
            node_type=args.node_type,
            num_workers=args.num_workers,
            access_mode=args.access_mode,
            user_name=me,
            jar=args.jar,
            wheel=None if args.docker_image else args.wheel,  # the image carries the wheel, as on the notebook path
            extra_jars=tuple(args.extra_jars),
            docker_image=args.docker_image,
        )
        params = [a.strip() for a in args.python_args.split(",") if a.strip()]
        ok = run_python_task(
            w,
            snap.get(local, local),
            args.ws_dir or f"/Users/{me}/GeoBrix/python-task",
            params,
            cluster,
            args.poll_secs,
            args.timeout_min,
        )
        return 0 if ok else 1

    if args.extra_jars:
        parser.error("--extra-jar goes with --python-task")

    if args.pool and (args.new_cluster or args.existing_cluster_id or args.hardware_accelerator):
        parser.error("--pool is exclusive with --new-cluster, --existing-cluster-id and --hardware-accelerator")

    if args.jar and not args.new_cluster and not args.pool:
        parser.error("--jar goes with --jar-task, --new-cluster or --pool (it is not used on Serverless or a warm cluster)")

    if args.new_cluster and (args.existing_cluster_id or args.hardware_accelerator):
        parser.error(
            "--new-cluster is exclusive with --existing-cluster-id and --hardware-accelerator"
        )

    gpu_note = gpu_wheel_warning(args.wheel, args.hardware_accelerator)
    if gpu_note:
        print(f"WARNING: {gpu_note}", file=sys.stderr, flush=True)

    notebooks = _collect_notebooks(args.notebooks, args.dirs)
    if not notebooks:
        parser.error(
            "Provide at least one notebook via --notebook or --dir."
        )
    if args.harvest_run_id is not None and len(notebooks) != 1:
        parser.error("--harvest-run-id harvests ONE --notebook")
    for nb_path in notebooks if args.cells else []:  # every piece must fit, and need no skipped cell, before any run
        for spec in args.cells:
            try:
                _select_piece(nb_path.read_bytes(), spec, args.prelude, nb_path.parent)
            except ValueError as exc:
                parser.error(f"{nb_path.name}: {exc}")
            except json.JSONDecodeError as exc:
                parser.error(f"{nb_path.name}: not a notebook ({exc})")

    if args.timeout_min < 1:
        parser.error("--timeout-min must be at least 1")

    # Run from a snapshot: copy every notebook this launch touches into a temp dir now, so an edit while the
    # run is live cannot change what a later notebook of the same launch uploads. Harvested outputs still go
    # back to the original notebook.
    dep_nbs = [pathlib.Path(d).resolve() for d in args.dep_notebooks or []]
    snap = snapshot_files([*notebooks, *dep_nbs], prefix="gbx-nb-")
    originals = {snap.get(nb, nb): nb for nb in notebooks}
    notebooks = [snap.get(nb, nb) for nb in notebooks]
    args.dep_notebooks = [str(snap.get(d, d)) for d in dep_nbs]

    extras = [e.strip() for e in args.extras.split(",") if e.strip()] if args.wheel else []
    extra_deps = _split_deps(args.extra_deps)
    # A new cluster's runtime fixes the wheel environment (--env-version is the Serverless one).
    new_runtime = args.new_cluster if isinstance(args.new_cluster, str) else args.spark_version
    dep_env = args.env_version
    if args.new_cluster:
        try:
            _check_wheel_env(args.wheel, runtime_env(new_runtime), new_runtime)
        except SystemExit as exc:
            parser.error(str(exc.code))
        if runtime_env(new_runtime) is not None:
            dep_env = str(runtime_env(new_runtime))
    deps = _build_dep_list(args.wheel, extras, extra_deps, dep_env)

    if args.harvest_run_id is not None:  # harvest an earlier run; no workspace import, no run
        ctx = _run_context(args, None, new_runtime if args.new_cluster else None)
        for spec in args.cells or [None]:
            harvest(args.harvest_run_id, args.profile, notebooks[0], originals[notebooks[0]], cells=spec,
                    run_context=ctx)
        return 1 if HARVEST_FAILURES else 0

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=args.profile)

    import atexit
    import contextlib

    lease = None
    stack = contextlib.ExitStack()
    atexit.register(stack.close)  # a leased pool cluster is given back when the process ends, whatever the way out
    if args.pool:
        import _cluster_pool

        volroot = args.pool_volume or (
            "/Volumes/{}/{}/{}".format(
                os.environ.get("GBX_BUNDLE_VOLUME_CATALOG", "geospatial_docs"),
                os.environ.get("GBX_BUNDLE_VOLUME_SCHEMA", "geobrix"),
                os.environ.get("GBX_BUNDLE_VOLUME_NAME", "sample-data"),
            )
        )
        try:
            lease = stack.enter_context(
                _cluster_pool.leased_cluster(w, args.pool, volroot, {"run_id": "notebooks", "owner": w.current_user.me().user_name})
            )
            lease.sync_jars([args.jar] if args.jar else [])
        except _cluster_pool.PoolError as exc:
            stack.close()
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        args.existing_cluster_id = lease.cluster_id

    # Classic-cluster modes keep %pip cells (classic clusters install via %pip, unlike
    # Serverless JOB compute where they fail and are stripped).
    if args.existing_cluster_id or args.new_cluster:
        args.strip_pip = False
    job_cluster = None
    if args.new_cluster:
        job_cluster = JobCluster(
            spark_version=new_runtime,
            node_type=args.node_type,
            num_workers=args.num_workers,
            access_mode=args.access_mode,
            user_name=w.current_user.me().user_name,
            jar=args.jar,
            # a notebook with no %pip cell has no other way to import databricks.labs.gbx on a job cluster; a DCS
            # image carries the wheel and the JAR, and a second copy would be a second geobrix on the cluster
            wheel=None if args.docker_image else args.wheel,
            docker_image=args.docker_image,
        )

    # Resolve default ws-dir using the current user
    ws_dir = args.ws_dir
    if not ws_dir:
        me = w.current_user.me().user_name or "unknown"
        ws_dir = f"/Users/{me}/GeoBrix/serverless-run"
    # Earlier runs' imports stay for their window; remove those past it (bounded, never fails the launch).
    _workspace_sweep.sweep_at_launch(w, ("notebooks",))

    print(f"Profile  : {args.profile}", flush=True)
    print(f"Workspace: {ws_dir}", flush=True)
    print(f"Extras   : {', '.join(extras)}", flush=True)
    if args.wheel:
        print(f"Deps     : {len(deps)} entries (wheel + {len(deps) - 1} packages)", flush=True)
    else:
        print(f"Deps     : {len(deps)} entries (no geobrix wheel: --no-wheel)", flush=True)
    print(f"Notebooks: {len(notebooks)}", flush=True)
    print(f"Strip %%pip: {args.strip_pip}", flush=True)
    print(f"Wheel guard: {'on, ' + WHEEL_GUARD['wheel'] if WHEEL_GUARD['on'] else 'off'}", flush=True)
    if WHEEL_GUARD["on"] and args.existing_cluster_id:
        _warn_cluster_wheel_libraries(w, args.existing_cluster_id, WHEEL_GUARD["wheel"])
    if args.hardware_accelerator:
        print(f"Accelerator: {args.hardware_accelerator}", flush=True)
    if args.existing_cluster_id:
        print(f"Cluster  : {args.existing_cluster_id} (warm; %pip kept, env spec skipped)", flush=True)
    if job_cluster:
        print(
            f"Cluster  : new job cluster per run, {job_cluster.access_mode} access, "
            f"{job_cluster.spark_version}, {job_cluster.num_workers} x {job_cluster.node_type} "
            "(%pip kept, env spec skipped; terminated when the run ends)"
            + (f"; JAR library {job_cluster.jar}" if job_cluster.jar else ""),
            flush=True,
        )
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
        _import_notebook(w, dep_path, ws_dir, strip_pip=args.strip_pip, guard=False)

    ctx = _run_context(args, w, new_runtime if job_cluster else None)
    for nb_path in notebooks:
        for spec in args.cells or [None]:
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
                job_cluster=job_cluster,
                timeout_min=args.timeout_min,
                harvest_path=originals[nb_path],
                on_submit=lease.job_run if lease is not None else None,  # the lease follows the notebook's job run
                cells=spec,
                prelude=args.prelude,
                run_context=ctx,
            )
            if not ok:
                return 1

    print("\n=== ALL NOTEBOOKS SUCCEEDED ===", flush=True)
    if HARVEST_FAILURES:
        print(f"=== OUTPUT HARVEST FAILED for: {', '.join(HARVEST_FAILURES)} (exit 1) ===", flush=True)
        return 1
    return 0


def _run_context(args, w, new_runtime: Optional[str]) -> dict:
    """Where the runs execute, for the harvested notebook's record: the Serverless environment or the runtime."""
    ctx: dict = {"wheel": args.wheel}
    if args.existing_cluster_id:
        ctx["compute"] = "cluster"
        try:
            ctx["spark_version"] = w.clusters.get(args.existing_cluster_id).spark_version if w is not None else None
        except Exception:  # noqa: BLE001 - the label only
            ctx["spark_version"] = None
    elif args.new_cluster or new_runtime:
        ctx["compute"] = "job cluster"
        ctx["spark_version"] = new_runtime or args.spark_version
    else:
        ctx["compute"] = "serverless"
        ctx["environment_version"] = str(args.env_version)
        ctx["label"] = f"Serverless environment {args.env_version}" + (
            f" ({args.hardware_accelerator})" if args.hardware_accelerator else "")
    if ctx.get("spark_version"):
        ctx["label"] = f"Databricks Runtime {ctx['spark_version']}"
    return {k: v for k, v in ctx.items() if v is not None}


if __name__ == "__main__":
    sys.exit(main())
