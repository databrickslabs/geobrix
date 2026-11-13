#!/usr/bin/env python3
"""Run the GeoBrix Lakeflow connector on Databricks end to end and write its evidence file (gbx:test:lakeflow-connector).

Run it through scripts/commands/gbx-test-lakeflow-connector.sh, which picks an interpreter that has the
Databricks SDK. Every workspace object is a scratch object named after the commit (``<sha12>``):

  1. refuse when a tracked file under the connector sources (LFC_PATHS) differs from HEAD;
  2. run the connector unit tests (gbx:test:connectors), which also makes the connector venv;
  3. the three wheels: the GeoBrix env 6 wheel (dist/env6 when it fits HEAD, else built), and the framework and
     connector wheels built as the docs' BUILD_WHEELS says (the connector from `git archive HEAD`);
  4. what each table should hold: the connector's own code reads a local copy of the landing folder
     (_lfc_expect.py, in the connector venv): each table's columns and each file's rows;
  5. the scratch schema ``<catalog>.geobrix_lfc_it_<sha12>`` with a managed Volume ``files``: the landing
     folder (``files/landing``) and the three wheels (``files/wheels``), sizes verified by listing;
  6. the Unity Catalog connection ``geobrix_lfc_it_<sha12>`` (type COMMUNITY, sourceName geobrix, the
     allowlist of connector_spec.yaml, root_path = the landing folder);
  7. a Serverless pipeline (PREVIEW channel, the documented dependency lines) whose source is the connector's
     pipeline/ingest.py with its pipeline_spec filled in, in /Users/<me>/jobs/geobrix/lfc-it-<sha12>/;
  8. update 1: every table exists, has the expected columns, and holds the expected rows of each file;
  9. update 2, after one new file lands: only that file's rows are appended;
 10. update 3, after a truncated GeoJSON file lands: the update fails and names the file, as the docs say,
     and no row is appended;
 11. drop the pipeline, the connection and the schema (with its Volume) unless --keep;
 12. write test-logs/lakeflow-connector/evidence.json.

scripts/ci/check_lakeflow_connector_evidence.py reads the evidence file.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "ci"))
sys.path.insert(0, str(HERE))

import check_lakeflow_connector_evidence as lfc_check  # noqa: E402
import run_sdp_bundle  # noqa: E402  (the pipeline update poll, shared with gbx:test:sdp)
import uc_integration as uc  # noqa: E402  (wheel reuse and build helpers, shared with gbx:uc:tests)
import uc_sources_hash  # noqa: E402
import _uc_warehouse as gate  # noqa: E402  (the per-profile gate target: catalog and warehouse)
from _job_submit import jobs_root  # noqa: E402

CMD = REPO_ROOT / "scripts" / "commands"


def main_root(root: Path = REPO_ROOT) -> Path:
    """The main checkout: ``root`` itself, or the checkout a linked worktree (a clean build tree) belongs to."""
    res = subprocess.run(["git", "-C", str(root), "rev-parse", "--path-format=absolute", "--git-common-dir"],
                         capture_output=True, text=True)
    common = Path(res.stdout.strip()) if res.returncode == 0 and res.stdout.strip() else None
    return common.parent if common and common.name == ".git" else root


def untracked(rel: str, root: Path = REPO_ROOT) -> Path:
    """An untracked artifact (dist/env6, sample-data): this checkout's, else the main checkout's."""
    own = root / rel
    return own if own.exists() else main_root(root) / rel


# The evidence goes to the main checkout's test-logs, where the QC check reads it, also from a worktree run.
OUT_DIR = main_root() / "test-logs" / "lakeflow-connector"
EVIDENCE = OUT_DIR / "evidence.json"
CONNECTOR_DIR = "connectors/lakeflow"
SPEC_YAML = REPO_ROOT / CONNECTOR_DIR / "sources" / "geobrix" / "connector_spec.yaml"
INGEST_TEMPLATE = REPO_ROOT / CONNECTOR_DIR / "sources" / "geobrix" / "pipeline" / "ingest.py"
DOC_EXAMPLES = REPO_ROOT / "docs" / "tests" / "python" / "ingestion" / "lakeflow_connector_examples.py"
UNIT_TESTS_CMD = CMD / "gbx-test-connectors.sh"
CONNECTOR_VENV_PY = REPO_ROOT / ".venv-connectors" / "bin" / "python"
DIST_ENV6 = REPO_ROOT / "dist" / "env6"  # where gbx:build:wheel --env 6 leaves the wheel
FRAMEWORK_CACHE = REPO_ROOT / ".cache" / "lfcc-wheels"
DEFAULT_CATALOG = "geospatial_docs"
DEFAULT_DATA_DIR = untracked("sample-data/Volumes/main/default/geobrix_samples/geobrix-examples")
DEFAULT_TIMEOUT_MIN = 30
DEFAULT_POLL_SECS = 20
CHANNEL = "PREVIEW"  # DBR 19: the env 6 wheel (docs: Environment)
NAME_PREFIX = "geobrix_lfc_it_"
VOLUME = "files"


@dataclass(frozen=True)
class Dataset:
    """One connector table: the landing files (landing path -> sample-data path) and its table options."""

    table: str
    files: Tuple[Tuple[str, str], ...]
    config: Tuple[Tuple[str, str], ...] = ()

    @property
    def cfg(self) -> Dict[str, str]:
        return dict(self.config)


# Small real samples, one per reader kind: a GeoJSON folder (discovered by name), one layer of a GeoPackage
# (an explicit path and layer), a zipped shapefile folder, GeoTIFFs inferred from the extension, and a .tif
# read with the COG reader (format: cog).
DATASETS = (
    Dataset("boroughs", (("boroughs/nyc_boroughs.geojson", "nyc/boroughs/nyc_boroughs.geojson"),), (("format", "geojson"),)),
    Dataset("taxi_zones", (("nyc_complete.gpkg", "nyc/geopackage/nyc_complete.gpkg"),),
            (("format", "gpkg"), ("path", "nyc_complete.gpkg"), ("layer", "taxi_zones"))),
    Dataset("subway", (("subway/nyc_subway.shp.zip", "nyc/subway/nyc_subway.shp.zip"),), (("format", "shapefile"),)),
    Dataset("elevation", (("elevation/srtm_n40w074.tif", "nyc/elevation/srtm_n40w074.tif"),)),
    Dataset("elevation_cog", (("elevation_cog/srtm_n51w001.tif", "london/elevation/srtm_n51w001.tif"),), (("format", "cog"),)),
)
# Update 2: one new file in one dataset (same schema as the dataset's first file).
INCREMENT = ("elevation", "elevation/srtm_n40w073.tif", "nyc/elevation/srtm_n40w073.tif")
# Update 3: a truncated GeoJSON file (the first bytes of a real one). Its name sorts after the dataset's first
# file, so the table schema still comes from the readable file and the read of this one fails.
UNREADABLE = ("boroughs", "boroughs/zz_truncated.geojson", "nyc/boroughs/nyc_boroughs.geojson", 4096)


class StepFailed(Exception):
    """A step failed; the message goes into the evidence."""


class Precondition(StepFailed):
    """The workspace refuses a step for a reason outside the code (a privilege, a preview not enabled)."""


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def say(msg: str) -> None:
    print(f"[LFC IT {datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ---- pure helpers (host-tested) ------------------------------------------------------------------------------------


def names(sha12: str, catalog: str) -> dict:
    """Every scratch name of a run at commit ``sha12``."""
    if not re.fullmatch(r"[0-9a-f]{12}", sha12 or ""):
        raise ValueError(f"not a 12-character commit prefix: {sha12!r}")
    base = f"{NAME_PREFIX}{sha12}"
    volume = f"/Volumes/{catalog}/{base}/{VOLUME}"
    return {
        "schema": base,
        "connection": base,
        "pipeline": base,
        "volume": volume,
        "landing": f"{volume}/landing",
        "wheels": f"{volume}/wheels",
        "ws_folder": f"lfc-it-{sha12}",
    }


_SCRATCH = re.compile(rf"{NAME_PREFIX}[0-9a-f]{{12}}")


def scratch_name_ok(name: str, sha12: str) -> bool:
    """True only for the scratch name of commit ``sha12`` (``geobrix_lfc_it_<sha12>``): the one thing a drop may touch."""
    return bool(_SCRATCH.fullmatch(name or "")) and name == f"{NAME_PREFIX}{sha12}"


def gone(exc: BaseException) -> bool:
    """The object is already gone: a delete that finds nothing is not a failure."""
    text = f"{type(exc).__name__} {exc}"
    return type(exc).__name__ in ("NotFound", "ResourceDoesNotExist") or bool(
        re.search(r"does not exist|not found|RESOURCE_DOES_NOT_EXIST", text, re.I))


def framework_commit(root: Path = REPO_ROOT) -> str:
    """The framework commit of the docs' BUILD_WHEELS; it must equal the one the unit tests run against."""
    doc = (root / "docs/tests/python/ingestion/lakeflow_connector_examples.py").read_text()
    sh = (root / "scripts/commands/gbx-test-connectors.sh").read_text()
    m_doc = re.search(r'^FRAMEWORK_COMMIT = "([0-9a-f]{40})"', doc, re.M)
    m_sh = re.search(r'^LFCC_COMMIT="([0-9a-f]{40})"', sh, re.M)
    if not m_doc or not m_sh:
        raise StepFailed("cannot read the framework commit from the docs examples or gbx-test-connectors.sh")
    if m_doc.group(1) != m_sh.group(1):
        raise StepFailed(f"the docs build the framework at {m_doc.group(1)[:12]}, the unit tests at {m_sh.group(1)[:12]}")
    if m_doc.group(1) not in re.search(r'BUILD_WHEELS = r"""(.*?)"""', doc, re.S).group(1):
        raise StepFailed("BUILD_WHEELS does not build the framework at FRAMEWORK_COMMIT")
    return m_doc.group(1)


def doc_constant(name: str, root: Path = REPO_ROOT) -> str:
    """A string constant of the docs examples file (BUILD_WHEELS, PIPELINE_DEPENDENCIES)."""
    text = (root / "docs/tests/python/ingestion/lakeflow_connector_examples.py").read_text()
    m = re.search(rf'^{name} = r?"""(.*?)"""', text, re.S | re.M)
    if not m:
        raise StepFailed(f"no {name} in the docs examples")
    return m.group(1)


def allowlist(spec_yaml: Path = SPEC_YAML) -> str:
    """The external options allowlist of connector_spec.yaml."""
    m = re.search(r'^external_options_allowlist:\s*"([^"]+)"', spec_yaml.read_text(), re.M)
    if not m:
        raise StepFailed(f"no external_options_allowlist in {spec_yaml}")
    return m.group(1)


def connection_body(name: str, landing: str, allow: str, comment: str) -> dict:
    """The Unity Catalog connection of the docs (Set up, step 2)."""
    return {
        "name": name,
        "connection_type": "COMMUNITY",
        "comment": comment,
        "options": {"sourceName": "geobrix", "externalOptionsAllowList": allow, "root_path": landing},
    }


def pipeline_spec(connection: str, datasets: Sequence[Dataset] = DATASETS) -> dict:
    return {
        "connection_name": connection,
        "objects": [
            {"table": {"source_table": d.table, "destination_table": d.table,
                       "table_configuration": {"scd_type": "APPEND_ONLY", **d.cfg}}}
            for d in datasets
        ],
    }


_SPEC_BLOCK = re.compile(r"^pipeline_spec = \{\n.*?^\}\n", re.S | re.M)


def pipeline_source(template: str, spec: dict, header: Sequence[str] = ()) -> str:
    """The connector's pipeline source file with its pipeline_spec replaced: everything else stays as shipped."""
    if len(_SPEC_BLOCK.findall(template)) != 1 or "ingest(spark, pipeline_spec)" not in template:
        raise StepFailed("pipeline/ingest.py has no single `pipeline_spec = {...}` block followed by ingest(spark, pipeline_spec)")
    body = "pipeline_spec = " + json.dumps(spec, indent=4) + "\n"
    out = _SPEC_BLOCK.sub(lambda _m: body, template, count=1)
    head = "".join(f"# {line}\n" if line else "#\n" for line in header)
    return head + ("#\n" if head else "") + out


_WHEEL_KINDS = (
    ("geobrix_lakeflow_connectors-", "connector"),
    ("lakeflow_community_connectors-", "framework"),
    ("geobrix-", "geobrix"),
)


def wheel_kind(basename: str) -> Optional[str]:
    for prefix, kind in _WHEEL_KINDS:
        if basename.startswith(prefix):
            return kind
    return None


def dependency_lines(template: str, folder: str, wheels: Dict[str, str]) -> List[str]:
    """The docs' PIPELINE_DEPENDENCIES lines for the wheels in ``folder`` (``wheels``: kind -> file name).

    The GeoBrix line keeps its documented form (``geobrix@file://...``, no spaces around ``@``); the
    placeholders become the folder and the file names."""
    out = []
    for line in [x.strip() for x in template.strip().splitlines() if x.strip()]:
        base = line.rsplit("/", 1)[-1]
        kind = wheel_kind(base)
        if kind is None or kind not in wheels:
            raise StepFailed(f"PIPELINE_DEPENDENCIES line {line!r} names no wheel this run builds")
        if kind == "geobrix":
            if not line.startswith("geobrix@file:///Volumes/"):
                raise StepFailed(f"the GeoBrix dependency line must read geobrix@file:///Volumes/..., without spaces: {line!r}")
            out.append(f"geobrix@file://{folder.rstrip('/')}/{wheels[kind]}")
        else:
            if not line.startswith("/Volumes/"):
                raise StepFailed(f"PIPELINE_DEPENDENCIES line {line!r} is not a Volume path")
            out.append(f"{folder.rstrip('/')}/{wheels[kind]}")
    kinds = [wheel_kind(x.rsplit("/", 1)[-1]) for x in out]
    if sorted(kinds) != sorted(k for _, k in _WHEEL_KINDS):
        raise StepFailed(f"PIPELINE_DEPENDENCIES lists {kinds}, want one line per wheel")
    return out


def pipeline_body(name: str, catalog: str, schema: str, source_file: str, deps: Sequence[str]) -> dict:
    """The Serverless pipeline of the docs (Set up, step 3): the source file, the wheels, the PREVIEW channel."""
    return {
        "name": name, "catalog": catalog, "schema": schema, "serverless": True, "channel": CHANNEL,
        "development": True, "continuous": False,
        "libraries": [{"file": {"path": source_file}}],
        "environment": {"dependencies": list(deps)},
    }


def rel_source(path: str, landing: str) -> Optional[str]:
    """A ``_source_path`` value relative to the landing folder, or None when it is outside it."""
    p = re.sub(r"^(?:dbfs:|file:)", "", path or "")
    root = landing.rstrip("/") + "/"
    return p[len(root):] if p.startswith(root) else None


def need(rc: int, what: str, out: str, fix: str) -> None:
    """Stop the run when a step's command failed: the last lines of its output, then one Fix line."""
    if rc != 0:
        tail = " | ".join(out.strip().splitlines()[-3:]) if out else ""
        raise StepFailed(f"{what} failed (exit {rc}){': ' + tail[:400] if tail else ''}. Fix: {fix}")


def check(name: str, ok: bool, detail: str) -> dict:
    return {"name": name, "ok": bool(ok), "detail": detail}


def judge_tables(stage: str, expect: dict, observed: dict, landed: Dict[str, List[str]]) -> List[dict]:
    """One check per table: it exists, has the connector's columns, and holds each landed file's rows.

    ``expect``: _lfc_expect output; ``observed``: {table: {"columns": [...], "files": {rel: rows}}} (a missing
    table is absent); ``landed``: {table: [landing paths present]}."""
    out = []
    for table in sorted(landed):
        obs = observed.get(table)
        if obs is None:
            out.append(check(f"{stage}:{table}", False, "the table does not exist"))
            continue
        want_cols = [c[0] for c in expect[table]["columns"]]
        problems = []
        if obs["columns"] != want_cols:
            problems.append(f"columns {obs['columns']} != the connector's {want_cols}")
        for lineage in ("_source_path", "_source_mtime"):
            if lineage not in obs["columns"]:
                problems.append(f"no {lineage} column")
        want = {f: expect[table]["files"][f] for f in landed[table]}
        if obs["files"] != want:
            problems.append(f"rows per file {obs['files']} != expected {want}")
        rows = sum(obs["files"].values())
        out.append(check(f"{stage}:{table}", not problems,
                         "; ".join(problems) or f"{rows} row(s) from {len(want)} file(s), {len(want_cols)} columns"))
    return out


def judge_increment(table: str, new_file: str, expect: dict, before: dict, after: dict) -> dict:
    """Update 2 appended exactly the new file's rows to ``table`` and nothing to any other table."""
    problems = []
    for t in sorted(set(before) | set(after)):
        b, a = (before.get(t) or {}).get("files", {}), (after.get(t) or {}).get("files", {})
        if t != table:
            if a != b:
                problems.append(f"{t} changed: {b} -> {a}")
            continue
        want = dict(b, **{new_file: expect[table]["files"][new_file]})
        if a != want:
            problems.append(f"{t}: rows per file {a}, want {want} (the old files unchanged plus {new_file})")
    added = sum((after.get(table) or {}).get("files", {}).values()) - sum((before.get(table) or {}).get("files", {}).values())
    return check("update2:increment", not problems,
                 "; ".join(problems) or f"{table} +{added} row(s), all from {new_file}; other tables unchanged")


def judge_unreadable(file_name: str, update_ok: bool, problems: Sequence[str], before: dict, after: dict) -> dict:
    """Update 3 failed, its error names the unreadable file, and no table gained a row."""
    issues = []
    if update_ok:
        issues.append("the update succeeded; the docs say an unreadable file fails it")
    elif not any(file_name in p for p in problems):
        issues.append(f"the update failed but no error names {file_name}: {list(problems)[:3]}")
    changed = [t for t in sorted(set(before) | set(after))
               if (before.get(t) or {}).get("files") != (after.get(t) or {}).get("files")]
    if changed:
        issues.append(f"rows were appended to {changed}")
    return check("update3:unreadable", not issues,
                 "; ".join(issues) or f"the update failed and named {file_name}; no row appended")


def decide(checks: Sequence[dict]) -> str:
    return "pass" if checks and all(c["ok"] for c in checks) else "fail"


def precondition_line(exc: BaseException, what: str, workspace: str) -> Optional[str]:
    """The PRECONDITION MISSING line for a refusal that no code change fixes, else None."""
    text = f"{type(exc).__name__}: {exc}"
    if re.search(r"PermissionDenied|PERMISSION_DENIED|not enabled|does not have .*privilege|User does not have", text, re.I):
        return f"PRECONDITION MISSING: {what} on {workspace} ({text.strip()[:300]}); the run stops here"
    return None


def header_lines(sha12: str, n: dict, expect: dict) -> List[str]:
    rows = ", ".join(f"{t} {sum(v['files'].values())}" for t, v in sorted(expect.items()))
    return [
        f"GeoBrix Lakeflow connector integration test (gbx:test:lakeflow-connector) at {sha12}.",
        f"Tests: the connector reads {n['landing']} through connection {n['connection']} into "
        f"{len(expect)} append-only tables.",
        f"Expected: rows per table after update 1 (with files): {rows}. Update 2 appends only {INCREMENT[1]}. "
        f"Update 3 fails on {UNREADABLE[1]}.",
        "Time: about 3 to 8 min per update on Serverless; the driver stops an update after --timeout-min.",
        "Monitoring: the driver polls each update and stops the pipeline on an ERROR event. Cancel: stop the pipeline.",
        "Written by examples/tests/lakeflow_connector_it.py; the shipped file is connectors/lakeflow/sources/geobrix/pipeline/ingest.py.",
    ]


# ---- the run -------------------------------------------------------------------------------------------------------


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _query(w, warehouse_id: str, sql: str, wait_s: int = 300) -> list:
    resp = w.statement_execution.execute_statement(statement=sql, warehouse_id=warehouse_id, wait_timeout="50s")
    waited = 0
    while run_sdp_bundle._state(resp, "status", "state") in ("PENDING", "RUNNING") and waited < wait_s:
        time.sleep(5)
        waited += 5
        resp = w.statement_execution.get_statement(resp.statement_id)
    state = run_sdp_bundle._state(resp, "status", "state")
    if state != "SUCCEEDED":
        raise StepFailed(f"statement {state}: {(run_sdp_bundle._state(resp, 'status', 'error', 'message') or '')[:300]}")
    return list(getattr(resp.result, "data_array", None) or [])


class Run:
    def __init__(self, a):
        self.a = a
        self.head = uc_sources_hash.head_sha(REPO_ROOT)
        self.sha12 = self.head[:12]
        self.n = names(self.sha12, a.catalog)
        self.run_dir = OUT_DIR / self.sha12
        self.warehouse = a.warehouse_id or uc.warehouse_from_config(profile=a.profile or "")
        self.w = None
        self.checks: List[dict] = []
        self.ev: dict = {}
        self.created = {"schema": False, "connection": False, "pipeline": None}
        self.ws_dir: Optional[str] = None
        self.update_ids: List[str] = []
        self.tables: Dict[str, dict] = {}

    def client(self):
        if self.w is None:
            from databricks.sdk import WorkspaceClient

            self.w = WorkspaceClient(profile=self.a.profile)
        return self.w

    @property
    def fqn(self) -> str:
        return f"{self.a.catalog}.{self.n['schema']}"

    # -- local steps --
    def step_unit_tests(self) -> None:
        say("step 2/12: the connector unit tests (gbx:test:connectors; makes the connector venv)")
        rc, out = uc.run_cmd(["bash", str(UNIT_TESTS_CMD), "--log", str(self.run_dir / "unit-tests.log")],
                             self.run_dir / "unit-tests.out", "connector unit tests", echo=False)
        m = re.findall(r"=+ (.*?) in [\d.]+s =+", out)
        summary = m[-1] if m else "(no pytest summary)"
        self.checks.append(check("unit-tests", rc == 0, summary))
        say(f"connector unit tests: {summary}")
        need(rc, "the connector unit tests", out,
             f"run bash scripts/commands/gbx-test-connectors.sh and fix the failing tests (log {self.run_dir / 'unit-tests.log'})")
        if not CONNECTOR_VENV_PY.exists():
            raise StepFailed(f"the unit tests left no connector venv at {CONNECTOR_VENV_PY}. Fix: run "
                             "bash scripts/commands/gbx-test-connectors.sh --resync")

    def step_wheels(self, commit: str) -> Dict[str, Path]:
        say("step 3/12: the three wheels")
        out_dir = self.run_dir / "wheels"
        shutil.rmtree(out_dir, ignore_errors=True)
        out_dir.mkdir(parents=True)
        wheels = {"geobrix": self.geobrix_wheel()}
        self.record_geobrix_wheel(wheels["geobrix"])
        # The framework wheel is built once per commit (it is pinned) and cached.
        fw_dir = FRAMEWORK_CACHE / commit
        fw = sorted(fw_dir.glob("lakeflow_community_connectors-*.whl"))
        if not fw:
            fw_dir.mkdir(parents=True, exist_ok=True)
            rc, out = uc.run_cmd([str(CONNECTOR_VENV_PY), "-m", "pip", "wheel", "--no-deps", "-w", str(fw_dir),
                                  f"lakeflow-community-connectors @ git+https://github.com/databrickslabs/lakeflow-community-connectors@{commit}"],
                                 self.run_dir / "build-framework.log", "framework wheel", echo=False)
            need(rc, "the framework wheel build", out, "check access to github.com and the package index "
                 f"(log {self.run_dir / 'build-framework.log'}), then run again")
            fw = sorted(fw_dir.glob("lakeflow_community_connectors-*.whl"))
        wheels["framework"] = fw[-1]
        # The connector wheel from the committed tree at the run's commit (git archive), never the working tree.
        src = self.run_dir / "src"
        shutil.rmtree(src, ignore_errors=True)
        src.mkdir(parents=True)
        arch = subprocess.run(["git", "-C", str(REPO_ROOT), "archive", "--format=tar", self.head, CONNECTOR_DIR],
                              capture_output=True, check=True).stdout
        subprocess.run(["tar", "-x", "-C", str(src)], input=arch, check=True)
        rc, out = uc.run_cmd([str(CONNECTOR_VENV_PY), "-m", "pip", "wheel", "--no-deps", "-w", str(out_dir), str(src / CONNECTOR_DIR)],
                             self.run_dir / "build-connector.log", "connector wheel", echo=False)
        need(rc, "the connector wheel build", out, f"read {self.run_dir / 'build-connector.log'}; the build is "
             f"pip wheel --no-deps {CONNECTOR_DIR} at commit {self.sha12}")
        conn = sorted(out_dir.glob("geobrix_lakeflow_connectors-*.whl"))
        if not conn:
            raise StepFailed(f"the connector wheel build left no wheel in {out_dir}. Fix: read {self.run_dir / 'build-connector.log'}")
        wheels["connector"] = conn[-1]
        for kind, p in wheels.items():
            say(f"  {kind}: {p.name} ({p.stat().st_size} bytes, sha256 {_sha256(p)[:16]})")
        return wheels

    def record_geobrix_wheel(self, wheel: Path) -> dict:
        """The evidence of the GeoBrix wheel: its sha256, the commit it was built at, and whether it fits HEAD."""
        info = uc.wheel_info(wheel)
        fits, why = uc.wheel_reusable(info)
        self.ev["geobrix_wheel"] = {"name": info["name"], "sha256": info["sha256"], "git_sha": info["git_sha"],
                                    "dirty": info["dirty"], "fits_head": fits, "why": why}
        say(f"  GeoBrix wheel built at {(info['git_sha'] or '?')[:12]}: {'fits HEAD' if fits else 'does not fit HEAD'} ({why})")
        return self.ev["geobrix_wheel"]

    def geobrix_wheel(self) -> Path:
        """--geobrix-wheel when given; else dist/env6 when it fits HEAD (as gbx:uc:tests reuses it), else a clean
        build at HEAD. The evidence records the wheel's commit and whether it fits HEAD."""
        if self.a.geobrix_wheel:
            given = Path(self.a.geobrix_wheel)
            if not re.fullmatch(r"geobrix-[^/]+\+env6-py3-none-any\.whl", given.name):
                raise StepFailed(f"--geobrix-wheel {given}: not an env 6 wheel (geobrix-<version>+env6-py3-none-any.whl); the "
                                 "pipeline runs on the PREVIEW channel (DBR 19). Fix: pass the +env6 wheel")
            if not given.is_file():
                raise StepFailed(f"--geobrix-wheel {given}: no such file. Fix: pass a local path to the env 6 wheel")
            return given
        dist = sorted(untracked("dist/env6").glob("geobrix-*+env6-py3-none-any.whl"))
        if dist and not self.a.rebuild:
            ok, why = uc.wheel_reusable(uc.wheel_info(dist[-1]))
            say(f"  reuse {dist[-1].name}: {'yes' if ok else 'no'}, {why}")
            if ok:
                return dist[-1]
        jar = REPO_ROOT / "target" / "geobrix-0.6.0-jar-with-dependencies.jar"
        if uc.jar_stale(jar):
            say("  building the JAR (target/ is missing or older than the Scala sources)")
            rc, out = uc.run_cmd(["bash", str(CMD / "gbx-data-stage-jar.sh")], self.run_dir / "build-jar.log", "JAR build",
                                 env={"GBX_BUNDLE_SKIP_JAR_UPLOAD": "1"})
            need(rc, "the JAR build", out, "run bash scripts/commands/gbx-data-stage-jar.sh (needs the dev container), "
                 "or pass --geobrix-wheel")
        say("  building the env 6 wheel")
        rc, out = uc.run_cmd(["bash", str(CMD / "gbx-build-wheel.sh"), "--env", "6"], self.run_dir / "build-wheel.log", "wheel build")
        need(rc, "the wheel build", out, "run bash scripts/commands/gbx-build-wheel.sh --env 6 (needs the dev container), "
             "or pass --geobrix-wheel")
        dist = sorted(DIST_ENV6.glob("geobrix-*+env6-py3-none-any.whl"))
        info = uc.wheel_info(dist[-1]) if dist else None
        if not info or info["dirty"] or info["git_sha"] != self.head:
            raise StepFailed(f"the wheel build left no clean env 6 wheel at {self.sha12}")
        return dist[-1]

    def step_expect(self) -> Tuple[Path, dict]:
        say("step 4/12: what each table should hold (the connector's code on a local copy of the landing folder)")
        data = Path(self.a.data_dir)
        landing = self.run_dir / "landing"
        shutil.rmtree(landing, ignore_errors=True)
        sources = [(rel, src) for d in DATASETS for rel, src in d.files] + [(INCREMENT[1], INCREMENT[2])]
        missing = [str(data / src) for _, src in sources if not (data / src).is_file()]
        if missing:
            raise StepFailed(f"sample data missing: {missing[:3]}. Fix: bash scripts/commands/gbx-data-download.sh --bundle essential")
        for rel, src in sources:
            (landing / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(data / src, landing / rel)
        spec = json.dumps([{"table": d.table, "config": d.cfg} for d in DATASETS])
        res = subprocess.run([str(CONNECTOR_VENV_PY), str(HERE / "_lfc_expect.py"), "--root", str(landing), "--tables", spec],
                             capture_output=True, text=True, env=uc.child_env())
        if res.returncode != 0:
            raise StepFailed(f"the local connector read failed: {res.stderr.strip()[-600:]}")
        expect = json.loads(res.stdout)
        for t, v in sorted(expect.items()):
            say(f"  {t} ({v['format']}): {v['files']}")
        (self.run_dir / "expect.json").write_text(json.dumps(expect, indent=2, sort_keys=True))
        return landing, expect

    # -- workspace steps --
    def leftovers(self) -> List[str]:
        w, out = self.client(), []
        try:
            w.schemas.get(self.fqn)
            out.append(f"schema {self.fqn}")
        except Exception as exc:  # noqa: BLE001
            if type(exc).__name__ not in ("NotFound", "ResourceDoesNotExist"):
                raise
        try:
            w.connections.get(self.n["connection"])
            out.append(f"connection {self.n['connection']}")
        except Exception as exc:  # noqa: BLE001
            if type(exc).__name__ not in ("NotFound", "ResourceDoesNotExist"):
                raise
        out += [f"pipeline {p.pipeline_id}" for p in self.pipelines_named()]
        return out

    def pipelines_named(self) -> list:
        return [p for p in self.client().pipelines.list_pipelines(filter=f"name LIKE '{self.n['pipeline']}'")
                if getattr(p, "name", None) == self.n["pipeline"]]

    def step_stage(self, landing: Path, wheels: Dict[str, Path]) -> None:
        say(f"step 5/12: schema {self.fqn}, Volume {VOLUME}, landing folder and wheels")
        w = self.client()
        try:
            w.schemas.create(name=self.n["schema"], catalog_name=self.a.catalog,
                             comment=f"gbx:test:lakeflow-connector scratch at {self.sha12}; dropped at the end of the run")
        except Exception as exc:  # noqa: BLE001
            line = precondition_line(exc, f"CREATE SCHEMA in catalog {self.a.catalog}", self.a.profile)
            raise (Precondition(line) if line else exc)
        self.created["schema"] = True
        w.api_client.do("POST", "/api/2.1/unity-catalog/volumes",
                        body={"catalog_name": self.a.catalog, "schema_name": self.n["schema"], "name": VOLUME, "volume_type": "MANAGED"})
        uploads = [(landing / rel, f"{self.n['landing']}/{rel}") for d in DATASETS for rel, _ in d.files]
        uploads += [(p, f"{self.n['wheels']}/{p.name}") for p in wheels.values()]
        for local, remote in uploads:
            self.upload(local, remote)
        say(f"  {len(uploads)} files uploaded; sizes verified by listing")

    def upload(self, local: Path, remote: str, data: Optional[bytes] = None) -> None:
        """Upload one file and verify its size by listing (an upload can return without its bytes landing)."""
        w = self.client()
        folder = remote.rsplit("/", 1)[0]
        w.files.create_directory(folder)
        payload = data if data is not None else local.read_bytes()
        w.files.upload(remote, io.BytesIO(payload), overwrite=True)
        listed = {e.path.rstrip("/"): e.file_size for e in w.files.list_directory_contents(folder) if not e.is_directory}
        if listed.get(remote) != len(payload):
            raise StepFailed(f"{remote}: listed size {listed.get(remote)}, uploaded {len(payload)} bytes")

    def step_connection(self) -> None:
        say(f"step 6/12: connection {self.n['connection']} (COMMUNITY, root_path {self.n['landing']})")
        body = connection_body(self.n["connection"], self.n["landing"], allowlist(),
                               f"gbx:test:lakeflow-connector scratch at {self.sha12}; dropped at the end of the run")
        try:
            self.client().api_client.do("POST", "/api/2.1/unity-catalog/connections", body=body)
        except Exception as exc:  # noqa: BLE001
            line = precondition_line(exc, "CREATE CONNECTION (type COMMUNITY)", self.a.profile)
            raise (Precondition(line) if line else exc)
        self.created["connection"] = True

    def step_pipeline(self, expect: dict, wheels: Dict[str, Path]) -> str:
        w = self.client()
        me = w.current_user.me().user_name
        self.ws_dir = f"{jobs_root(me)}/{self.n['ws_folder']}"
        src_path = f"{self.ws_dir}/ingest.py"
        say(f"step 7/12: pipeline {self.n['pipeline']} ({CHANNEL}, Serverless) from {src_path}")
        text = pipeline_source(INGEST_TEMPLATE.read_text(), pipeline_spec(self.n["connection"]),
                               header_lines(self.sha12, self.n, {t: {"files": {f: r for f, r in v["files"].items() if f != INCREMENT[1]}}
                                                                  for t, v in expect.items()}))
        (self.run_dir / "ingest.py").write_text(text)
        w.workspace.mkdirs(self.ws_dir)
        # A workspace FILE (format RAW), not a notebook: the pipeline library is `file`.
        w.api_client.do("POST", "/api/2.0/workspace/import",
                        body={"path": src_path, "format": "RAW", "content": base64.b64encode(text.encode()).decode(), "overwrite": True})
        deps = dependency_lines(doc_constant("PIPELINE_DEPENDENCIES"), self.n["wheels"],
                                {k: p.name for k, p in wheels.items()})
        for d in deps:
            say(f"  dependency: {d}")
        # Development mode: a failed update is not retried by Lakeflow (this run never retries an update).
        body = pipeline_body(self.n["pipeline"], self.a.catalog, self.n["schema"], src_path, deps)
        pid = w.api_client.do("POST", "/api/2.0/pipelines", body=body)["pipeline_id"]
        self.created["pipeline"] = pid
        self.ev["pipeline_url"] = f"{w.config.host.rstrip('/')}/pipelines/{pid}"
        self.ev["dependencies"] = deps
        say(f"  pipeline {pid}: {self.ev['pipeline_url']}")
        return pid

    def update(self, pid: str, label: str) -> Tuple[bool, List[str]]:
        w = self.client()
        since = int(time.time() * 1000) - 5_000
        up = w.pipelines.start_update(pipeline_id=pid)
        self.update_ids.append(up.update_id)
        say(f"{label}: update {up.update_id} started (limit {self.a.timeout_min} min)")
        t0 = time.time()
        ok, problems = run_sdp_bundle.poll(w, pipeline_ids={"lfc": pid}, update=(pid, up.update_id), since_ms=since,
                                           timeout_s=self.a.timeout_min * 60, poll_s=self.a.poll_secs,
                                           say=lambda m: say(m.strip()))
        say(f"{label}: {'COMPLETED' if ok else 'FAILED'} in {int(time.time() - t0)} s")
        if not ok:
            # The poll keeps 400 characters per error; the reader's message (it names the file) sits deeper in the
            # Python worker's traceback. Read the update's ERROR events whole, and keep them with the run's logs.
            full = [f"{where}: {msg}" for where, msg in run_sdp_bundle.pipeline_errors(w, pid, up.update_id)]
            (self.run_dir / f"{label.replace(' ', '-')}-errors.txt").write_text("\n\n".join(full or problems) + "\n")
            problems = list(problems) + full
        for p in problems[:5]:
            say(f"  - {p[:400]}")
        return ok, problems

    def observe(self) -> dict:
        """{table: {"columns": [...], "files": {rel: rows}}} for each table that exists."""
        w, out = self.client(), {}
        for d in DATASETS:
            try:
                info = w.tables.get(f"{self.fqn}.{d.table}")
            except Exception as exc:  # noqa: BLE001
                if type(exc).__name__ in ("NotFound", "ResourceDoesNotExist"):
                    continue
                raise
            out[d.table] = {"columns": [c.name for c in (info.columns or [])], "files": {}}
        if out:
            sql = " UNION ALL ".join(
                f"SELECT '{t}' AS t, _source_path AS p, count(*) AS n FROM `{self.a.catalog}`.`{self.n['schema']}`.`{t}` GROUP BY _source_path"
                for t in out)
            for t, p, n in _query(w, self.warehouse, sql):
                rel = rel_source(p, self.n["landing"]) or f"<outside the landing folder: {p}>"
                out[t]["files"][rel] = int(n)
        return out

    def landed(self, extra: Sequence[str] = ()) -> Dict[str, List[str]]:
        got = {d.table: [rel for rel, _ in d.files] for d in DATASETS}
        for rel in extra:
            got[rel.split("/", 1)[0]].append(rel)
        return got

    def cleanup(self) -> str:
        if self.a.keep:
            say(f"kept (--keep): schema {self.fqn}, connection {self.n['connection']}, pipeline {self.created['pipeline']}")
            return "kept"
        say("step 11/12: drop the pipeline, the connection and the schema (with its Volume)")
        bad = [f"{k} {self.n[k]!r}" for k in ("schema", "connection") if not scratch_name_ok(self.n[k], self.sha12)]
        if bad:  # never drop a name this run does not own
            return f"refused: {', '.join(bad)} is not the scratch name {NAME_PREFIX}{self.sha12}"
        w, notes = self.client(), []
        if self.created["pipeline"]:
            try:
                w.pipelines.delete(pipeline_id=self.created["pipeline"])
            except Exception as exc:  # noqa: BLE001
                if not gone(exc):
                    notes.append(f"pipeline delete failed: {str(exc)[:200]}")
        if self.created["connection"]:
            try:
                w.api_client.do("DELETE", f"/api/2.1/unity-catalog/connections/{self.n['connection']}")
            except Exception as exc:  # noqa: BLE001
                if not gone(exc):
                    notes.append(f"connection delete failed: {str(exc)[:200]}")
        if self.created["schema"]:
            try:
                w.schemas.delete(self.fqn, force=True)
            except Exception as exc:  # noqa: BLE001
                if not gone(exc):
                    notes.append(f"schema drop failed: {str(exc)[:200]}")
        if self.ws_dir:
            import _workspace_sweep

            _workspace_sweep.release(w, [self.ws_dir], scope="lfc", succeeded=decide(self.checks) == "pass",
                                     out=lambda m: say(m))
        return "dropped" if not notes else "; ".join(notes)

    def drop(self, dry_run: bool = False) -> int:
        """--drop: remove what a kept or broken run of this commit left. It touches only the scratch names of
        ``self.sha12`` (``geobrix_lfc_it_<sha12>``), refuses any other name, and takes "already gone" as done."""
        bad = [f"{k} {self.n[k]!r}" for k in ("schema", "connection", "pipeline") if not scratch_name_ok(self.n[k], self.sha12)]
        if bad:
            say(f"--drop refused: {', '.join(bad)} is not the scratch name {NAME_PREFIX}{self.sha12}")
            return 2
        found = self.leftovers()
        say(f"leftovers of {self.sha12}: {found or 'none'}")
        if dry_run:
            for f in found:
                say(f"  would drop {f}")
            return 0
        notes = []
        for p in self.pipelines_named():
            try:
                self.client().pipelines.delete(pipeline_id=p.pipeline_id)
            except Exception as exc:  # noqa: BLE001
                if not gone(exc):
                    notes.append(f"pipeline {p.pipeline_id} delete failed: {str(exc)[:200]}")
        self.created = {"pipeline": None, "connection": any(f.startswith("connection ") for f in found),
                        "schema": any(f.startswith("schema ") for f in found)}
        self.ws_dir = None
        self.a.keep = False
        result = self.cleanup()
        if result != "dropped":
            notes.append(result)
        say(f"--drop: {'; '.join(notes) or 'dropped'}")
        return 0 if not notes else 1

    def run(self) -> int:
        started, t0 = now(), time.time()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        landing, expect = None, {}
        self.ev = {"head_sha": self.head, "sources_hash": lfc_check.sources_hash(REPO_ROOT, self.head), "started": started,
                   "profile": self.a.profile, "catalog": self.a.catalog, "schema": self.n["schema"],
                   "connection": self.n["connection"], "warehouse_id": self.warehouse, "channel": CHANNEL}
        error = None
        try:
            commit = framework_commit()
            self.step_unit_tests()
            wheels = self.step_wheels(commit)
            self.ev["wheels"] = {p.name: _sha256(p) for p in wheels.values()}
            self.ev["framework_commit"] = commit
            landing, expect = self.step_expect()
            self.client()
            problem = (uc.warehouse_problem(self.w, self.warehouse, uc.CONFIG, self.a.profile)
                       or uc.catalog_problem(self.w, self.a.catalog, uc.CONFIG, self.a.profile))
            if problem:
                raise StepFailed(problem)
            left = self.leftovers()
            if left:
                raise StepFailed(f"a run of {self.sha12} left {left}. Fix: bash scripts/commands/gbx-test-lakeflow-connector.sh "
                                 f"--profile {self.a.profile} --drop {self.sha12}")
            self.step_stage(landing, wheels)
            self.step_connection()
            pid = self.step_pipeline(expect, wheels)
            self.ev["pipeline_id"] = pid

            say("step 8/12: update 1 (every table, every landed file)")
            ok, problems = self.update(pid, "update 1")
            self.checks.append(check("update1:completed", ok, "; ".join(problems)[:600] or "COMPLETED"))
            obs1 = self.observe()
            self.checks += judge_tables("update1", expect, obs1, self.landed())
            if not ok:
                raise StepFailed("update 1 failed: the later updates are not run (fail fast)")

            say(f"step 9/12: update 2 after {INCREMENT[1]} lands")
            self.upload(landing / INCREMENT[1], f"{self.n['landing']}/{INCREMENT[1]}")
            ok, problems = self.update(pid, "update 2")
            self.checks.append(check("update2:completed", ok, "; ".join(problems)[:600] or "COMPLETED"))
            obs2 = self.observe()
            self.checks.append(judge_increment(INCREMENT[0], INCREMENT[1], expect, obs1, obs2))
            self.checks += judge_tables("update2", expect, obs2, self.landed([INCREMENT[1]]))
            if not ok:
                raise StepFailed("update 2 failed: update 3 is not run (fail fast)")
            obs3 = obs2
            if not self.a.skip_unreadable:
                say(f"step 10/12: update 3 after the truncated {UNREADABLE[1]} lands (it must fail)")
                data = (Path(self.a.data_dir) / UNREADABLE[2]).read_bytes()[: UNREADABLE[3]]
                self.upload(Path(UNREADABLE[1]), f"{self.n['landing']}/{UNREADABLE[1]}", data=data)
                ok, problems = self.update(pid, "update 3")
                obs3 = self.observe()
                self.checks.append(judge_unreadable(UNREADABLE[1].rsplit("/", 1)[-1], ok, problems, obs2, obs3))
            self.tables = {t: {"columns": (obs3.get(t) or obs2.get(t) or {}).get("columns"),
                               "update1": obs1.get(t, {}).get("files"), "update2": obs2.get(t, {}).get("files")}
                           for t in sorted(expect)}
        except Precondition as exc:
            error = str(exc)
            print(error, flush=True)
        except Exception as exc:  # noqa: BLE001 - every failure goes into the evidence
            error = f"{type(exc).__name__}: {str(exc)[:800]}"
            say(f"!!! {error}")
        if error:
            self.checks.append(check("run", False, error))
        cleanup = self.cleanup() if (self.w is not None) else "nothing created"
        self.ev.update({
            "finished": now(), "wall_s": int(time.time() - t0), "checks": self.checks, "tables": self.tables,
            "update_ids": self.update_ids, "cleanup": cleanup, "result": decide(self.checks),
            "ws_dir": self.ws_dir,
        })
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        EVIDENCE.write_text(json.dumps(self.ev, indent=2, sort_keys=True) + "\n")
        say("checks:")
        for c in self.checks:
            say(f"  {'PASS' if c['ok'] else 'FAIL'} {c['name']}: {c['detail'][:300]}")
        say(f"evidence: {EVIDENCE}")
        print(lfc_check.summary_line(self.ev), flush=True)
        return 0 if self.ev["result"] == "pass" else 1


def plan(a, sha12: str) -> None:
    n = names(sha12, a.catalog)
    print(f"=== gbx:test:lakeflow-connector at {sha12} (dry run: no build, no workspace call) ===")
    print(f"    profile    : {a.profile}   catalog: {a.catalog}   warehouse: {a.warehouse_id or uc.warehouse_from_config(profile=a.profile or '')}")
    for k in ("schema", "connection", "pipeline", "landing", "wheels"):
        print(f"    {k:<11}: {n[k]}")
    print(f"    source file: /Users/<you>/jobs/geobrix/{n['ws_folder']}/ingest.py ({CHANNEL}, Serverless)")
    print(f"    allowlist  : {allowlist()}")
    print(f"    framework  : {framework_commit()[:12]}")
    print("    tables     :")
    for d in DATASETS:
        print(f"      {d.table:<14} {', '.join(r for r, _ in d.files)}  {d.cfg or '(format from the extension)'}")
    print(f"    update 2   : + {INCREMENT[1]} (only its rows appended)")
    print("    update 3   : " + ("skipped (--skip-unreadable)" if a.skip_unreadable else f"+ {UNREADABLE[1]} (truncated; the update must fail)"))
    print(f"    cleanup    : {'kept (--keep)' if a.keep else 'drop the pipeline, the connection and the schema'}")
    deps = dependency_lines(doc_constant("PIPELINE_DEPENDENCIES"), n["wheels"],
                            {"geobrix": "geobrix-<version>+env6-py3-none-any.whl",
                             "framework": "lakeflow_community_connectors-<version>-py3-none-any.whl",
                             "connector": "geobrix_lakeflow_connectors-<version>-py3-none-any.whl"})
    for d in deps:
        print(f"    dependency : {d}")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--profile", default=None, help="Databricks CLI profile (required)")
    p.add_argument("--catalog", default=None, help=f"catalog of the scratch schema (default GBX_GATE_CATALOG__<profile> in the cluster config, else {DEFAULT_CATALOG})")
    p.add_argument("--warehouse-id", default=None, help="SQL warehouse for the row counts (default GBX_UC_GATE_WAREHOUSE_ID__<profile> in the cluster config, then GBX_UC_GATE_WAREHOUSE_ID, then GBX_UC_WAREHOUSE_ID)")
    p.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="the local sample data (geobrix-examples)")
    p.add_argument("--timeout-min", type=int, default=DEFAULT_TIMEOUT_MIN, help="limit per pipeline update")
    p.add_argument("--poll-secs", type=int, default=DEFAULT_POLL_SECS)
    p.add_argument("--skip-unreadable", action="store_true", help="skip update 3 (the unreadable file)")
    p.add_argument("--rebuild", action="store_true", help="build the env 6 wheel even when dist/env6 fits HEAD")
    p.add_argument("--geobrix-wheel", default=None, metavar="PATH", help="a local env 6 wheel to use (no build)")
    p.add_argument("--keep", action="store_true", help="keep the pipeline, the connection and the schema")
    p.add_argument("--drop", default=None, metavar="SHA12", help="drop what a run of commit SHA12 left, then stop")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    gate.apply_gate_target(a, uc.CONFIG, DEFAULT_CATALOG)
    return a


def main(argv=None) -> int:
    try:
        a = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0) and 2
    if not a.profile:
        print("--profile required: no profile is picked for you (see `databricks auth profiles`)", file=sys.stderr)
        return 2
    if a.timeout_min < 1:
        print("--timeout-min must be at least 1", file=sys.stderr)
        return 2
    if a.drop:
        if not re.fullmatch(r"[0-9a-f]{12}", a.drop):
            print(f"--drop {a.drop!r}: want the 12-character commit prefix of the run", file=sys.stderr)
            return 2
        r = Run(a)
        r.sha12, r.n = a.drop, names(a.drop, a.catalog)
        return r.drop(dry_run=a.dry_run)
    dirty = uc_sources_hash.dirty_files(REPO_ROOT, paths=lfc_check.LFC_PATHS)
    if dirty:
        print("uncommitted changes under the connector sources (the evidence must describe committed code):", file=sys.stderr)
        for f in dirty:
            print(f"  {f}", file=sys.stderr)
        return 1
    sha12 = uc_sources_hash.head_sha(REPO_ROOT)[:12]
    if a.dry_run:
        plan(a, sha12)
        return 0
    if not a.warehouse_id:
        print("no SQL warehouse: pass --warehouse-id or set "
              f"{gate.profile_key(gate.GATE_WAREHOUSE_KEY, a.profile)} in examples/tests/databricks_cluster_config.env", file=sys.stderr)
        return 2
    say(gate.target_line(a, a.profile))
    say(f"step 1/12: connector sources clean at {sha12}")
    return Run(a).run()


if __name__ == "__main__":
    sys.exit(main())
