"""Host tests for gbx:test:lakeflow-connector: the driver (examples/tests/lakeflow_connector_it.py) and the
QC check check_lakeflow_connector_evidence.py.

The driver runs against a fake WorkspaceClient (schemas, volumes, files, connections, workspace, pipelines,
tables, statements): nothing reaches a workspace. The QC check runs in a scratch git repository with copies
of its scripts, so nothing touches the real checkout.
"""

import ast
import base64
import json
import os
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

from conftest import REPO_ROOT

CI = REPO_ROOT / "scripts" / "ci"
SH = REPO_ROOT / "scripts" / "commands" / "gbx-test-lakeflow-connector.sh"
SHA12 = "0123456789ab"


@pytest.fixture
def it(monkeypatch):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    monkeypatch.syspath_prepend(str(CI))
    import importlib

    return importlib.import_module("lakeflow_connector_it")


# ---- names, spec, connection, source file ---------------------------------------------------------------------------


def test_every_scratch_name_carries_the_commit(it):
    n = it.names(SHA12, "cat")
    assert n["schema"] == n["connection"] == n["pipeline"] == f"geobrix_lfc_it_{SHA12}"
    assert n["landing"] == f"/Volumes/cat/geobrix_lfc_it_{SHA12}/files/landing"
    assert n["wheels"] == f"/Volumes/cat/geobrix_lfc_it_{SHA12}/files/wheels"
    assert n["ws_folder"] == f"lfc-it-{SHA12}"
    with pytest.raises(ValueError):
        it.names("main", "cat")  # never a name another run could own


def test_the_framework_commit_of_the_docs_is_the_one_the_unit_tests_use(it, tmp_path):
    commit = it.framework_commit()
    assert len(commit) == 40 and commit in it.doc_constant("BUILD_WHEELS")
    # Fail on revert: a docs bump that the unit tests do not follow stops the run.
    root = tmp_path / "r"
    for rel in ("docs/tests/python/ingestion/lakeflow_connector_examples.py", "scripts/commands/gbx-test-connectors.sh"):
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO_ROOT / rel, root / rel)
    sh = root / "scripts/commands/gbx-test-connectors.sh"
    sh.write_text(sh.read_text().replace(commit, "f" * 40))
    with pytest.raises(it.StepFailed, match="unit tests"):
        it.framework_commit(root)


def test_the_connection_carries_the_spec_allowlist_the_docs_name(it):
    allow = it.allowlist()
    doc = (REPO_ROOT / "docs/docs/ingestion/lakeflow-connector.mdx").read_text()
    assert f"`{allow}`" in doc  # the docs' connection table names the same allowlist
    body = it.connection_body("c1", "/Volumes/a/b/c/landing", allow, "x")
    assert body["connection_type"] == "COMMUNITY" and body["name"] == "c1"
    assert body["options"] == {"sourceName": "geobrix", "externalOptionsAllowList": allow, "root_path": "/Volumes/a/b/c/landing"}


def test_the_pipeline_source_is_the_shipped_file_with_its_spec_filled_in(it):
    template = it.INGEST_TEMPLATE.read_text()
    spec = it.pipeline_spec("conn1")
    text = it.pipeline_source(template, spec, ["what it is", "", "how long"])
    compile(text, "ingest.py", "exec")
    assert text.startswith("# what it is\n#\n# how long\n")
    for line in template.splitlines():  # every line outside the spec block stays as shipped
        if line.startswith(("from ", "spark.", "ingest(")) or "connectionDfOptionInjection" in line:
            assert line in text
    tree = ast.parse(text)
    assigned = [n.value for n in tree.body if isinstance(n, ast.Assign) and n.targets[0].id == "pipeline_spec"]
    assert len(assigned) == 1 and ast.literal_eval(assigned[0]) == spec
    tables = [o["table"] for o in spec["objects"]]
    assert {t["source_table"] for t in tables} == {d.table for d in it.DATASETS}
    assert all(t["table_configuration"]["scd_type"] == "APPEND_ONLY" for t in tables)
    with pytest.raises(it.StepFailed):
        it.pipeline_source("ingest(spark, pipeline_spec)\n", spec)


def test_the_dependency_lines_are_the_documented_ones_for_the_staged_wheels(it):
    wheels = {"geobrix": "geobrix-0.6.0+env6-py3-none-any.whl", "framework": "lakeflow_community_connectors-0.1.0-py3-none-any.whl",
              "connector": "geobrix_lakeflow_connectors-0.1.0-py3-none-any.whl"}
    lines = it.dependency_lines(it.doc_constant("PIPELINE_DEPENDENCIES"), "/Volumes/c/s/files/wheels/", wheels)
    assert lines == [
        "geobrix@file:///Volumes/c/s/files/wheels/geobrix-0.6.0+env6-py3-none-any.whl",
        "/Volumes/c/s/files/wheels/lakeflow_community_connectors-0.1.0-py3-none-any.whl",
        "/Volumes/c/s/files/wheels/geobrix_lakeflow_connectors-0.1.0-py3-none-any.whl",
    ]
    assert not any(" " in x for x in lines)
    # Fail on revert: a docs line with spaces around @ splits under the pipeline's unquoted %pip install.
    with pytest.raises(it.StepFailed, match="without spaces"):
        it.dependency_lines("geobrix @ file:///Volumes/a/b/c/d/geobrix-1+env5-py3-none-any.whl\n"
                            "/Volumes/a/b/c/d/lakeflow_community_connectors-1-py3-none-any.whl\n"
                            "/Volumes/a/b/c/d/geobrix_lakeflow_connectors-1-py3-none-any.whl", "/V", wheels)
    with pytest.raises(it.StepFailed, match="one line per wheel"):
        it.dependency_lines("/Volumes/a/b/c/d/lakeflow_community_connectors-1-py3-none-any.whl", "/V", wheels)


def test_source_paths_are_judged_relative_to_the_landing_folder(it):
    assert it.rel_source("/Volumes/c/s/v/landing/a/b.tif", "/Volumes/c/s/v/landing") == "a/b.tif"
    assert it.rel_source("dbfs:/Volumes/c/s/v/landing/x.gpkg", "/Volumes/c/s/v/landing/") == "x.gpkg"
    assert it.rel_source("/Volumes/c/s/v/landing2/x.gpkg", "/Volumes/c/s/v/landing") is None


# ---- the judges -----------------------------------------------------------------------------------------------------

EXPECT = {
    "a": {"columns": [["x", "int"], ["_source_path", "string"], ["_source_mtime", "timestamp"]], "files": {"a/1.geojson": 5}},
    "e": {"columns": [["tile", "binary"], ["_source_path", "string"], ["_source_mtime", "timestamp"]],
          "files": {"e/1.tif": 1, "e/2.tif": 2}},
}
COLS = {t: [c[0] for c in v["columns"]] for t, v in EXPECT.items()}


def obs(a=None, e=None):
    out = {}
    if a is not None:
        out["a"] = {"columns": COLS["a"], "files": a}
    if e is not None:
        out["e"] = {"columns": COLS["e"], "files": e}
    return out


def test_update_one_passes_when_each_table_holds_each_files_rows(it):
    got = it.judge_tables("update1", EXPECT, obs({"a/1.geojson": 5}, {"e/1.tif": 1}), {"a": ["a/1.geojson"], "e": ["e/1.tif"]})
    assert [c["ok"] for c in got] == [True, True] and "5 row(s) from 1 file(s)" in got[0]["detail"]


def test_update_one_fails_on_a_missing_table_wrong_columns_or_rows(it):
    landed = {"a": ["a/1.geojson"], "e": ["e/1.tif"]}
    got = it.judge_tables("update1", EXPECT, obs({"a/1.geojson": 5}), landed)
    assert not got[1]["ok"] and "does not exist" in got[1]["detail"]
    bad = obs({"a/1.geojson": 4}, {"e/1.tif": 1})
    bad["e"]["columns"] = ["tile", "_source_path"]
    got = it.judge_tables("update1", EXPECT, bad, landed)
    assert not got[0]["ok"] and "rows per file" in got[0]["detail"]
    assert not got[1]["ok"] and "no _source_mtime column" in got[1]["detail"]


def test_update_two_must_append_only_the_new_files_rows(it):
    before = obs({"a/1.geojson": 5}, {"e/1.tif": 1})
    good = it.judge_increment("e", "e/2.tif", EXPECT, before, obs({"a/1.geojson": 5}, {"e/1.tif": 1, "e/2.tif": 2}))
    assert good["ok"] and "+2 row(s), all from e/2.tif" in good["detail"]
    # Fail on revert: an update that reads the old file again appends its rows twice.
    twice = it.judge_increment("e", "e/2.tif", EXPECT, before, obs({"a/1.geojson": 5}, {"e/1.tif": 2, "e/2.tif": 2}))
    assert not twice["ok"]
    other = it.judge_increment("e", "e/2.tif", EXPECT, before, obs({"a/1.geojson": 10}, {"e/1.tif": 1, "e/2.tif": 2}))
    assert not other["ok"] and "a changed" in other["detail"]


def test_update_three_must_fail_name_the_file_and_append_nothing(it):
    o = obs({"a/1.geojson": 5}, {"e/1.tif": 1})
    assert it.judge_unreadable("zz.geojson", False, ["flow a: cannot read /Volumes/x/zz.geojson"], o, o)["ok"]
    assert not it.judge_unreadable("zz.geojson", True, [], o, o)["ok"]
    assert not it.judge_unreadable("zz.geojson", False, ["some other error"], o, o)["ok"]
    assert not it.judge_unreadable("zz.geojson", False, ["zz.geojson"], o, obs({"a/1.geojson": 6}, {"e/1.tif": 1}))["ok"]


def test_a_refusal_of_the_workspace_is_a_precondition_line(it):
    exc = RuntimeError("Securable kind 'CONNECTION_COMMUNITY' is not enabled. If this is a securable kind associated "
                       "with a preview feature, please enable it in workspace settings.")
    line = it.precondition_line(exc, "CREATE CONNECTION (type COMMUNITY)", "fevm-geospatial")
    assert line.startswith("PRECONDITION MISSING: CREATE CONNECTION (type COMMUNITY) on fevm-geospatial")
    assert it.precondition_line(RuntimeError("PERMISSION_DENIED: User does not have CREATE CONNECTION"), "x", "p")
    assert it.precondition_line(ValueError("bad value"), "x", "p") is None


def test_the_result_is_pass_only_when_every_check_passed(it):
    assert it.decide([it.check("a", True, ""), it.check("b", True, "")]) == "pass"
    assert it.decide([it.check("a", True, ""), it.check("b", False, "")]) == "fail"
    assert it.decide([]) == "fail"


# ---- the run, against a fake workspace ------------------------------------------------------------------------------


class NotFound(Exception):
    pass


class FakeW:
    """The calls the driver makes. Tables appear after update 1; ``rows`` per update decide what they hold."""

    def __init__(self, it, connection_error=None, update_states=("COMPLETED", "COMPLETED", "FAILED")):
        self.it = it
        self.calls = []
        self.schemas_ = set()
        self.files_ = {}
        self.connection_error = connection_error
        self.update_states = list(update_states)
        self.updates = 0
        self.config = SimpleNamespace(host="https://ws.example")
        w = self

        def rec(_what, **kw):
            w.calls.append((_what, kw))

        class Schemas:
            def get(self, full):
                if full not in w.schemas_:
                    raise NotFound(full)

            def create(self, name, catalog_name, comment=None):
                rec("schemas.create", name=name)
                w.schemas_.add(f"{catalog_name}.{name}")

            def delete(self, full, force=False):
                rec("schemas.delete", full=full, force=force)
                w.schemas_.discard(full)

        class Volumes:
            def create(self, **kw):
                rec("volumes.create", **{k: v for k, v in kw.items() if k != "volume_type"})

        class Files:
            def create_directory(self, path):
                pass

            def upload(self, path, contents, overwrite=False):
                w.files_[path] = len(contents.read())

            def list_directory_contents(self, folder):
                return [SimpleNamespace(path=p, file_size=n, is_directory=False) for p, n in w.files_.items()
                        if p.rsplit("/", 1)[0] == folder]

        class Api:
            def do(self, method, path, body=None):
                rec(f"api.{method} {path.split('/')[-1] if method == 'POST' else 'connection'}", **(body or {}))
                if path.endswith("/connections") and w.connection_error:
                    raise w.connection_error
                if path == "/api/2.0/pipelines":
                    return {"pipeline_id": "pid-1"}
                return {}

        class Connections:
            def get(self, name):
                raise NotFound(name)

        class Workspace:
            def mkdirs(self, path):
                pass

        class Pipelines:
            def list_pipelines(self, filter=None):
                return []

            def start_update(self, pipeline_id):
                w.updates += 1
                return SimpleNamespace(update_id=f"u{w.updates}")

            def delete(self, pipeline_id):
                rec("pipelines.delete", pipeline_id=pipeline_id)

        class Tables:
            def get(self, full):
                if w.updates == 0:
                    raise NotFound(full)
                t = full.rsplit(".", 1)[-1]
                return SimpleNamespace(columns=[SimpleNamespace(name=c[0]) for c in EXPECT_RUN[t]["columns"]])

        class Statements:
            def execute_statement(self, statement, warehouse_id, wait_timeout=None):
                rec("sql", warehouse_id=warehouse_id)
                land = w.it.names(SHA12, "cat")["landing"]
                rows = []
                done = min(w.updates, 2)  # update 3 fails: it appends nothing
                for t, v in EXPECT_RUN.items():
                    for f, n in v["files"].items():
                        if f == w.it.INCREMENT[1] and done < 2:
                            continue
                        rows.append([t, f"{land}/{f}", str(n)])
                return SimpleNamespace(status=SimpleNamespace(state="SUCCEEDED"), result=SimpleNamespace(data_array=rows))

        self.schemas, self.volumes, self.files = Schemas(), Volumes(), Files()
        self.api_client, self.connections, self.workspace = Api(), Connections(), Workspace()
        self.pipelines, self.tables, self.statement_execution = Pipelines(), Tables(), Statements()
        self.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name="me@example.com"))

    def names_called(self):
        return [c[0] for c in self.calls]


EXPECT_RUN = {}


@pytest.fixture
def fake_run(it, monkeypatch, tmp_path):
    """A Run whose local steps are stubbed and whose workspace is a FakeW; returns (make, evidence path)."""
    EXPECT_RUN.clear()
    for d in it.DATASETS:
        files = {rel: 3 for rel, _ in d.files}
        if d.table == it.INCREMENT[0]:
            files[it.INCREMENT[1]] = 2
        EXPECT_RUN[d.table] = {"columns": [["v", "int"], ["_source_path", "string"], ["_source_mtime", "timestamp"]],
                               "files": files, "format": "x"}
    data = tmp_path / "data"
    (data / it.UNREADABLE[2]).parent.mkdir(parents=True)
    (data / it.UNREADABLE[2]).write_bytes(b"x" * 9000)
    landing = tmp_path / "landing"
    for rel in [r for d in it.DATASETS for r, _ in d.files] + [it.INCREMENT[1]]:
        (landing / rel).parent.mkdir(parents=True, exist_ok=True)
        (landing / rel).write_bytes(b"data")
    wheel_dir = tmp_path / "wheels"
    wheel_dir.mkdir()
    wheels = {}
    for kind, name in (("geobrix", "geobrix-0.6.0+env6-py3-none-any.whl"),
                       ("framework", "lakeflow_community_connectors-0.1.0-py3-none-any.whl"),
                       ("connector", "geobrix_lakeflow_connectors-0.1.0-py3-none-any.whl")):
        (wheel_dir / name).write_bytes(kind.encode())
        wheels[kind] = wheel_dir / name
    out = tmp_path / "out"
    monkeypatch.setattr(it, "OUT_DIR", out)
    monkeypatch.setattr(it, "EVIDENCE", out / "evidence.json")
    monkeypatch.setattr(it.uc, "warehouse_problem", lambda *a, **k: None)
    monkeypatch.setattr(it.uc_sources_hash, "head_sha", lambda root: SHA12 + "c" * 28)
    monkeypatch.setattr(it.lfc_check, "sources_hash", lambda root=None, rev="HEAD": "h" * 64)
    monkeypatch.setattr(it.Run, "step_unit_tests", lambda self: self.checks.append(it.check("unit-tests", True, "60 passed")))
    monkeypatch.setattr(it.Run, "step_wheels", lambda self, commit: wheels)
    monkeypatch.setattr(it.Run, "step_expect", lambda self: (landing, EXPECT_RUN))

    def fake_poll(w, *, pipeline_ids, update, **kw):
        state = w.update_states[w.updates - 1]
        if state == "COMPLETED":
            return True, []
        # As the poll does: 400 characters per error, so the file name in the worker's traceback is cut off.
        return False, ["pipeline lfc: flow boroughs: [STREAM_FAILED] ... Traceback (most recent call last): ..."]

    def fake_errors(w, pipeline_id, update_id, limit=200):
        if w.update_states[w.updates - 1] == "COMPLETED":
            return []
        return [("source_boroughs_append_flow", "[STREAM_FAILED] ... Traceback ... DataSourceError: "
                 f"/Volumes/x/{it.UNREADABLE[1]}: Failed to read GeoJSON data")]

    monkeypatch.setattr(it.run_sdp_bundle, "poll", fake_poll)
    monkeypatch.setattr(it.run_sdp_bundle, "pipeline_errors", fake_errors)

    def make(**kw):
        args = it.parse_args(["--profile", "p", "--catalog", "cat", "--warehouse-id", "wh", "--data-dir", str(data)]
                             + kw.pop("argv", []))
        r = it.Run(args)
        r.w = FakeW(it, **kw)
        return r

    return make, out / "evidence.json"


def test_a_full_run_passes_writes_evidence_and_drops_everything_it_made(fake_run, it):
    make, ev_path = fake_run
    r = make()
    assert r.run() == 0
    ev = json.loads(ev_path.read_text())
    assert ev["result"] == "pass", [c for c in ev["checks"] if not c["ok"]]
    names = {c["name"] for c in ev["checks"]}
    assert {"unit-tests", "update1:completed", "update2:completed", "update2:increment", "update3:unreadable"} <= names
    assert {f"update1:{d.table}" for d in it.DATASETS} <= names
    assert ev["update_ids"] == ["u1", "u2", "u3"] and ev["pipeline_id"] == "pid-1" and ev["cleanup"] == "dropped"
    assert ev["head_sha"].startswith(SHA12) and ev["sources_hash"] == "h" * 64 and len(ev["wheels"]) == 3
    assert ev["tables"][it.INCREMENT[0]]["update2"][it.INCREMENT[1]] == 2
    calls = r.w.names_called()
    # Cleanup order: the pipeline (its tables), then the connection, then the schema with its Volume.
    tail = [c for c in calls if c in ("pipelines.delete", "api.DELETE connection", "schemas.delete")]
    assert tail == ["pipelines.delete", "api.DELETE connection", "schemas.delete"]
    assert dict(r.w.calls)["schemas.delete"]["force"] is True
    created = dict(r.w.calls)["api.POST pipelines"]
    assert created["serverless"] is True and created["channel"] == "PREVIEW" and created["schema"] == f"geobrix_lfc_it_{SHA12}"
    assert created["development"] is True  # Lakeflow does not retry a failed update
    assert created["environment"]["dependencies"][0].startswith("geobrix@file:///Volumes/cat/")
    src = f"/Users/me@example.com/jobs/geobrix/lfc-it-{SHA12}/ingest.py"
    assert created["libraries"] == [{"file": {"path": src}}]
    imported = dict(r.w.calls)["api.POST import"]
    assert imported["path"] == src and imported["format"] == "RAW"
    assert "ingest(spark, pipeline_spec)" in base64.b64decode(imported["content"]).decode()
    conn = dict(r.w.calls)["api.POST connections"]
    assert conn["connection_type"] == "COMMUNITY" and conn["options"]["root_path"] == it.names(SHA12, "cat")["landing"]
    # The truncated file was uploaded as a few KiB of the real one.
    trunc = f"{it.names(SHA12, 'cat')['landing']}/{it.UNREADABLE[1]}"
    assert r.w.files_[trunc] == it.UNREADABLE[3]


def test_keep_leaves_the_pipeline_connection_and_schema(fake_run):
    make, ev_path = fake_run
    r = make(argv=["--keep"])
    assert r.run() == 0
    assert not {"pipelines.delete", "api.DELETE connection", "schemas.delete"} & set(r.w.names_called())
    assert json.loads(ev_path.read_text())["cleanup"] == "kept"


def test_an_update_that_does_not_fail_on_the_unreadable_file_fails_the_run(fake_run):
    make, ev_path = fake_run
    r = make(update_states=("COMPLETED", "COMPLETED", "COMPLETED"))
    assert r.run() == 1
    ev = json.loads(ev_path.read_text())
    bad = [c for c in ev["checks"] if not c["ok"]]
    assert [c["name"] for c in bad] == ["update3:unreadable"] and ev["cleanup"] == "dropped"


def test_a_failed_first_update_stops_the_run_and_still_cleans_up(fake_run):
    make, ev_path = fake_run
    r = make(update_states=("FAILED",))
    assert r.run() == 1
    ev = json.loads(ev_path.read_text())
    assert ev["update_ids"] == ["u1"] and ev["result"] == "fail" and ev["cleanup"] == "dropped"
    assert any(c["name"] == "run" and "fail fast" in c["detail"] for c in ev["checks"])


def test_a_refused_connection_is_a_precondition_and_drops_the_schema(fake_run, capsys):
    make, ev_path = fake_run
    r = make(connection_error=RuntimeError("Securable kind 'CONNECTION_COMMUNITY' is not enabled."))
    assert r.run() == 1
    out = capsys.readouterr().out
    assert "PRECONDITION MISSING: CREATE CONNECTION (type COMMUNITY) on p" in out
    calls = r.w.names_called()
    assert "api.POST pipelines" not in calls and "api.DELETE connection" not in calls and "schemas.delete" in calls
    assert json.loads(ev_path.read_text())["result"] == "fail"


def test_leftovers_of_the_same_commit_stop_the_run_before_anything_is_made(fake_run, it):
    make, ev_path = fake_run
    r = make()
    r.w.schemas_.add(f"cat.geobrix_lfc_it_{SHA12}")
    assert r.run() == 1
    assert "schemas.create" not in r.w.names_called() and "schemas.delete" not in r.w.names_called()
    detail = [c["detail"] for c in json.loads(ev_path.read_text())["checks"] if c["name"] == "run"][0]
    assert f"--drop {SHA12}" in detail


def test_the_driver_needs_a_profile_and_a_dry_run_makes_no_client(it, monkeypatch, capsys):
    assert it.main([]) == 2
    assert "--profile required" in capsys.readouterr().err
    monkeypatch.setattr(it.uc_sources_hash, "dirty_files", lambda root, paths=None: [])
    monkeypatch.setattr(it.Run, "client", lambda self: (_ for _ in ()).throw(AssertionError("no workspace call")))
    assert it.main(["--profile", "p", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "dry run" in out and "geobrix@file:///Volumes/geospatial_docs/geobrix_lfc_it_" in out


def test_the_driver_refuses_dirty_connector_sources(it, monkeypatch, capsys):
    monkeypatch.setattr(it.uc_sources_hash, "dirty_files", lambda root, paths=None: ["connectors/lakeflow/pyproject.toml"])
    assert it.main(["--profile", "p", "--dry-run"]) == 1
    assert "connectors/lakeflow/pyproject.toml" in capsys.readouterr().err


def test_the_command_help_names_the_options_and_bad_options_exit_2():
    res = subprocess.run(["bash", str(SH), "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    for opt in ("--profile", "--catalog", "--warehouse-id", "--keep", "--drop", "--dry-run", "--skip-unreadable", "--timeout-min", "--log"):
        assert opt in res.stdout
    assert subprocess.run(["bash", str(SH), "--nope"], capture_output=True, text=True).returncode == 2
    assert subprocess.run(["bash", str(SH), "--profile"], capture_output=True, text=True).returncode == 2


# ---- check_lakeflow_connector_evidence ------------------------------------------------------------------------------


def git(root, *args):
    subprocess.run(["git", "-C", str(root), "-c", "user.email=t@t", "-c", "user.name=t", *args], check=True,
                   capture_output=True, text=True)


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "repo"
    (root / "scripts" / "ci").mkdir(parents=True)
    for name in ("uc_sources_hash.py", "check_uc_integration_evidence.py", "check_lakeflow_connector_evidence.py"):
        shutil.copy(CI / name, root / "scripts" / "ci" / name)
    for rel in ("connectors/lakeflow/sources/geobrix/a.py", "python/geobrix/src/pkg/mod.py", "docs/a.md", "src/main/scala/A.scala"):
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("v1\n")
    git(root, "init", "-q", "-b", "main")
    git(root, "add", "-A")
    git(root, "commit", "-q", "-m", "init")
    git(root, "update-ref", "refs/remotes/origin/main", "HEAD")
    return root


def run_check(root, *args, rng="origin/main..HEAD"):
    env = {k: v for k, v in os.environ.items() if k not in ("QC_RANGE", "GBX_LFC_EVIDENCE")}
    if rng is not None:
        env["QC_RANGE"] = rng
    return subprocess.run([sys.executable, str(root / "scripts/ci/check_lakeflow_connector_evidence.py"), *args],
                          capture_output=True, text=True, env=env, cwd=root)


def write_ev(root, sources_hash, result="pass"):
    ev = {"head_sha": "a" * 40, "sources_hash": sources_hash, "result": result, "catalog": "c", "schema": "s",
          "pipeline_id": "pid", "finished": "2026-11-13T12:00:00Z", "tables": {"t": {}},
          "checks": [{"name": "update1:t", "ok": result == "pass", "detail": ""}]}
    path = root / "test-logs" / "lakeflow-connector" / "evidence.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ev))
    return path


def current(root):
    return run_check(root, "--hash", rng=None).stdout.strip()


def test_the_check_skips_a_range_not_based_on_main(repo):
    res = run_check(repo, rng="origin/streamline/0.6.0..HEAD")
    assert res.returncode == 0 and "not a main-bound range" in res.stdout


def test_the_check_fails_without_evidence_with_a_fix(repo):
    res = run_check(repo)
    assert res.returncode == 1 and "no Lakeflow connector evidence" in res.stderr
    assert "gbx-test-lakeflow-connector.sh --profile" in res.stderr


def test_the_check_passes_fresh_passing_evidence_and_prints_the_summary(repo):
    write_ev(repo, current(repo))
    res = run_check(repo)
    assert res.returncode == 0, res.stderr
    assert "gbx:test:lakeflow-connector PASS" in res.stdout and "checks 1/1" in res.stdout


def test_the_check_fails_a_failed_run_and_names_the_failed_checks(repo):
    write_ev(repo, current(repo), result="fail")
    res = run_check(repo)
    assert res.returncode == 1 and "did not pass" in res.stderr and "update1:t" in res.stderr


def test_evidence_goes_stale_on_a_connector_or_read_core_commit_not_a_docs_or_scala_one(repo):
    write_ev(repo, current(repo))
    for rel in ("docs/a.md", "src/main/scala/A.scala"):
        (repo / rel).write_text("v2\n")
        git(repo, "commit", "-q", "-am", rel)
        assert run_check(repo).returncode == 0
    (repo / "connectors/lakeflow/sources/geobrix/a.py").write_text("v2\n")
    git(repo, "commit", "-q", "-am", "connector")
    res = run_check(repo)
    assert res.returncode == 1 and "stale" in res.stderr
    write_ev(repo, current(repo))
    (repo / "python/geobrix/src/pkg/mod.py").write_text("v2\n")
    git(repo, "commit", "-q", "-am", "read core")
    assert run_check(repo).returncode == 1


def test_unreadable_evidence_fails(repo):
    write_ev(repo, current(repo)).write_text("{not json")
    assert run_check(repo).returncode == 1


def test_the_hash_covers_the_documented_paths():
    sys.path.insert(0, str(CI))
    import check_lakeflow_connector_evidence as c

    assert "connectors/lakeflow" in c.LFC_PATHS and "python/geobrix/src" in c.LFC_PATHS
    assert "examples/tests/lakeflow_connector_it.py" in c.LFC_PATHS
    assert all((REPO_ROOT / p).exists() for p in c.LFC_PATHS)
