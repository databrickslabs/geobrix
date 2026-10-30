"""CI's JVMs run the native payload's GDAL, and the two-architecture native gate.

The JAR's GDAL Java classes are the native build's (pom.xml gdal.java.version), and GDALManager refuses any other
JNI. CI builds the x86_64 native payload itself (.github/workflows/build-gdal-native.yml, called by the CI
workflows), cached on the native-input hash of scripts/native/native_inputs.py, and every job that runs a GeoBrix
JVM links /usr/lib/libgdalalljni.so to that payload's JNI before its tests (scala_build). CI is x86_64 only: the
aarch64 build is covered by gbx:native:verify-arches, run locally, whose stamp (scripts/native/arch-gate.json) CI
checks against the same inputs.

No Docker and no network: the workflows are read as YAML, native_inputs.py runs on a scratch git repository that
holds copies of the real input files, jvm_payload.py on a fake native tree.
"""

import ast
import base64
import hashlib
import importlib
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile

import pytest
import yaml
from conftest import REPO_ROOT

GITHUB = REPO_ROOT / ".github"
WORKFLOWS = GITHUB / "workflows"
BUILDER = WORKFLOWS / "build-gdal-native.yml"
FETCH = GITHUB / "actions" / "native_payload" / "action.yml"
SCALA_BUILD = GITHUB / "actions" / "scala_build" / "action.yml"
INPUTS = REPO_ROOT / "scripts" / "native" / "native_inputs.py"
JVM_PAYLOAD = REPO_ROOT / "scripts" / "native" / "jvm_payload.py"
INDEX_FETCH = REPO_ROOT / "scripts" / "native" / "index_fetch.py"
GATE = REPO_ROOT / "scripts" / "commands" / "gbx-native-verify-arches.sh"
HASH_RUN = "scripts/native/native_inputs.py hash --set build"
PAYLOAD_KEY = "native-out-x86_64-${{ steps.%s.outputs.hash }}"
ARCHES = ("aarch64", "x86_64")


def _load(path):
    return yaml.safe_load(path.read_text())


def _cache_steps(steps):
    """The steps of actions/cache/{restore,save} that hold the native build output."""
    return [
        s
        for s in steps
        if re.match(r"actions/cache/(restore|save)@", s.get("uses", ""))
        and "build/native/out/x86_64" in s["with"]["path"]
    ]


def _key_step_id(steps):
    (step,) = [s for s in steps if HASH_RUN in s.get("run", "")]
    return step["id"]


def _paths(step):
    return [p.strip() for p in step["with"]["path"].strip().splitlines()]


def test_the_payload_cache_is_keyed_on_the_native_build_inputs():
    """One key, the hash of the native build's inputs, wherever the payload cache is read or written; no fallback to
    another key (a payload built from other inputs is not this one); saved on pushes to main and the release branch
    and on pull requests from this repository (not forks); the pinned sources are saved also after a failed build."""
    (builder,) = _load(BUILDER)["jobs"].values()
    fetch = _load(FETCH)["runs"]["steps"]
    paths = None
    for steps in (builder["steps"], fetch):
        kid = _key_step_id(steps)
        caches = _cache_steps(steps)
        assert caches, "no cache step for build/native/out/x86_64"
        for c in caches:
            assert c["with"]["key"] == PAYLOAD_KEY % kid, c
            assert "restore-keys" not in c["with"], c
            paths = paths or _paths(c)
            assert _paths(c) == paths, "the cache paths differ, so the entry saved is not the one restored"
    (save,) = [c for c in _cache_steps(builder["steps"]) if "/save@" in c["uses"]]
    cond = re.sub(r"\s+", " ", save["if"])
    assert "github.event_name == 'push'" in cond
    assert "refs/heads/main" in cond and "refs/heads/streamline/0.6.0" in cond
    assert "github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository" in cond
    (sources,) = [
        s
        for s in builder["steps"]
        if s.get("uses", "").startswith("actions/cache/save@") and "build/native/src" in s["with"]["path"]
    ]
    cond = re.sub(r"\s+", " ", sources["if"])
    assert cond.startswith("always() ") and "hashFiles('build/native/src/**') != ''" in cond
    # the builder builds what the cache holds: both targets, with the command the developers use
    run = " ".join(s.get("run", "") for s in builder["steps"])
    assert "gbx-build-native.sh --arch x86_64" in run


def _run_key_step(tmp_path, step, python3):
    """A key step's `run:` as Actions runs it, with `python3` (and `uname -m`: x86_64) shims first on PATH."""
    shims = tmp_path / "shims"
    shims.mkdir(exist_ok=True)
    (shims / "python3").write_text("#!/bin/sh\n" + python3)
    (shims / "uname").write_text("#!/bin/sh\necho x86_64\n")
    for f in shims.iterdir():
        f.chmod(0o755)
    out = tmp_path / "github_output"
    out.write_text("")
    env = dict(os.environ, PATH=f"{shims}{os.pathsep}{os.environ['PATH']}", GITHUB_OUTPUT=str(out))
    res = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", step["run"]], env=env, cwd=tmp_path,
                         capture_output=True, text=True)
    return res, out.read_text()


@pytest.mark.parametrize("where", ["builder", "consumer"])
def test_a_failed_input_hash_fails_the_key_step(tmp_path, where):
    """A failed hash stops the step; it never writes an empty `hash=`, which would make every failing run share the
    constant key native-out-x86_64- (and restore a stale build)."""
    steps = _load(BUILDER)["jobs"]["build-x86_64"]["steps"] if where == "builder" else _load(FETCH)["runs"]["steps"]
    (step,) = [s for s in steps if HASH_RUN in s.get("run", "")]

    good, out = _run_key_step(tmp_path, step, "echo " + "ab" * 32 + "\n")
    assert good.returncode == 0, good.stdout + good.stderr
    assert f"hash={'ab' * 32}" in out

    bad, out = _run_key_step(tmp_path, step, "exit 2\n")
    assert bad.returncode != 0
    assert "hash=" not in out


def _native(module):
    """A stdlib script of scripts/native, imported."""
    here = str(REPO_ROOT / "scripts" / "native")
    if here not in sys.path:
        sys.path.insert(0, here)
    return importlib.import_module(module)


def _parser_directives(dockerfile):
    """The `# key=value` parser directives at the top of a Dockerfile (BuildKit reads no others)."""
    out = {}
    for line in dockerfile.read_text().splitlines():
        m = re.fullmatch(r"#\s*([a-zA-Z]+)\s*=\s*(\S+)\s*", line)
        if not m:
            break
        out[m.group(1).lower()] = m.group(2)
    return out


def test_the_direct_download_probe_names_a_download_with_no_working_origin():
    """Each direct download needs one origin that serves the file; a policy proxy's 403/407/451 is blocked, not
    reachable. The sdists are not direct (they come through the package index in CI); rasterio's test data is. The
    image build pulls no Dockerfile frontend from Docker Hub, which no step probes: build.Dockerfile has no `syntax`
    directive, so the Docker Engine's bundled frontend parses it."""
    assert "syntax" not in _parser_directives(REPO_ROOT / "scripts/native/build.Dockerfile")
    probe = _native("egress_probe")
    pins = {
        "ZLIB_URL": "https://blocked-a.example/zlib.tar.gz",
        "ZLIB_URL2": "https://mirror.example/zlib.tar.gz",  # the mirror serves it: zlib has an origin
        "PROJ_URL": "https://policy.example/proj.tar.gz",  # answers 403 (a proxy's refusal)
        "PROJ_URL2": "single-source",
        "RUSTUP_URL": "https://mirror.example/rustup/{triple}/rustup-init",
        "RASTERIO_SDIST_FILE": "rasterio-1.5.2.tar.gz",
    }
    testdata = {"RASTERIO_TESTDATA_URL": "https://mirror.example/rasterio/1.5.2.tar.gz"}
    dl = probe.downloads(pins, testdata, "x86_64")
    assert sorted(dl) == ["PROJ", "RASTERIO_TESTDATA", "RUSTUP", "ZLIB"]
    assert dl["RUSTUP"] == ["https://mirror.example/rustup/x86_64-unknown-linux-gnu/rustup-init"]
    assert all(probe.kind(d) != "a pinned source tarball" for d in ("RUSTUP", "RASTERIO_TESTDATA"))

    def check(url):
        host = url.split("/")[2]
        if host.startswith("blocked"):
            return False, "no connection"
        return (False, "refused (HTTP 403: a policy proxy)") if host.startswith("policy") else (True, "ok")

    errors = probe.probe(dl, check)
    assert len(errors) == 1 and errors[0].startswith("::error::direct egress blocked: policy.example (refused (HTTP 403")
    assert "pins.conf PROJ_URL" in errors[0] and "Direct fetches" in errors[0]
    assert probe.probe(dl, lambda url: (True, "ok")) == []


TIMEOUT = "timed out"


@pytest.mark.parametrize(
    "answers, ok",
    [
        ((206,), True), ((200,), True),
        # one GET: a policy proxy's refusal is blocked, another 4xx is no working origin
        ((403,), False), ((407,), False), ((451,), False), ((404,), False),
        # a 429, a 5xx or a timeout is retried once (curl --retry's intent), then counts as no working origin
        ((429, 206), True), ((503, 206), True), ((TIMEOUT, 206), True), ((503, 503), False),
    ],
)
def test_a_refusal_is_blocked_and_a_transient_failure_retried_once(monkeypatch, answers, ok):
    probe = _native("egress_probe")
    calls = []

    def urlopen(req, timeout):
        answer = answers[len(calls)]  # an IndexError: called more often than the row allows
        calls.append(answer)
        if answer == TIMEOUT:
            raise probe.urllib.error.URLError(TimeoutError(TIMEOUT))
        if answer >= 400:
            raise probe.urllib.error.HTTPError(req.full_url, answer, "x", {}, None)

        class R:
            def read(self, n):
                return b"x"

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        return R()

    monkeypatch.setattr(probe.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(probe, "RETRY_DELAY", 0, raising=False)
    assert probe.check_url("https://h.example/f.tar.gz", 5)[0] is ok
    assert len(calls) == len(answers)


def _jvm_jobs():
    for wf in sorted(WORKFLOWS.glob("*.yml")):
        jobs = _load(wf)["jobs"]
        for name, job in jobs.items():
            steps = job.get("steps", [])
            if any(s.get("uses") == "./.github/actions/scala_build" for s in steps):
                yield wf.name, name, job, jobs


def test_every_jvm_job_links_the_payload_jni_before_its_tests():
    """A job that runs scala_build (Maven's test JVMs, then python_build's) first fetches the payload the builder made
    in this run, and scala_build links /usr/lib/libgdalalljni.so to its JNI before any test; GDAL 3.11's JNI is not
    put on a JVM's library path anywhere in CI."""
    found = list(_jvm_jobs())
    assert {w for w, *_ in found} >= {"build_main.yml", "build_python.yml", "build_scala.yml"}
    builder_group = _load(BUILDER)["jobs"]["build-x86_64"]["runs-on"]
    for wf, name, job, jobs in found:
        where = f"{wf}:{name}"
        # one runner group for the saving and the restoring side: a cache entry's version includes its compression
        assert job["runs-on"] == builder_group, where
        needs = job.get("needs", [])
        needs = [needs] if isinstance(needs, str) else needs
        assert any(
            jobs[n].get("uses") == "./.github/workflows/build-gdal-native.yml" for n in needs
        ), f"{where} does not need the native payload builder"
        steps = job["steps"]
        uses = [s.get("uses", "") for s in steps]
        fetch = uses.index("./.github/actions/native_payload")
        scala = uses.index("./.github/actions/scala_build")
        assert fetch < scala, where
        assert (
            steps[scala]["with"]["native_root"] == "${{ steps.%s.outputs.root }}" % steps[fetch]["id"]
        ), where

    action = _load(SCALA_BUILD)
    assert "native_root" in action["inputs"]
    steps = action["runs"]["steps"]
    (link,) = [i for i, s in enumerate(steps) if "docker_native_jni.sh" in s.get("run", "")]
    assert steps[link]["env"]["GBX_NATIVE_ROOT"] == "${{ inputs.native_root }}"
    assert "GBX_JNI_REQUIRE=1" in steps[link]["run"]
    tests = [i for i, s in enumerate(steps) if re.search(r"\./mvnw -T 1C .*(install|scoverage:test)", s.get("run", ""))]
    assert tests and link < min(tests)

    copies = [
        str(p.relative_to(REPO_ROOT))
        for p in GITHUB.rglob("*.yml")
        if re.search(r"gdal311/libgdalalljni\.so\s+/usr/lib", p.read_text())
    ]
    assert not copies, f"GDAL 3.11's JNI still goes to /usr/lib in {copies}"


def test_ci_checks_the_arch_gate_stamp_and_builds_nothing():
    jobs = _load(WORKFLOWS / "build_main.yml")["jobs"]
    gate = [j for j in jobs.values() if "native_inputs.py check-stamp" in str(j.get("steps"))]
    assert len(gate) == 1
    steps = gate[0]["steps"]
    assert [s["uses"].split("@")[0] for s in steps if "uses" in s] == ["actions/checkout"]
    assert not re.search(r"gbx-build|docker|mvnw|pip install", str(steps))


# --- the native-input hash ------------------------------------------------------------------------------------------


def _inputs(*args, root=None):
    cmd = [sys.executable, str(INPUTS), *args] + (["--root", str(root)] if root else [])
    return subprocess.run(cmd, capture_output=True, text=True)


def _git(root, *args):
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


@pytest.fixture
def tree(tmp_path):
    """A scratch git repository holding copies of the real gate inputs, plus files that are no input: the JVM side no
    gate check runs (GDALManager.scala, the committed GDAL Java classes) and a script beside the inputs."""
    res = _inputs("files", "--set", "gate")
    assert res.returncode == 0, res.stderr
    root = tmp_path / "repo"
    for rel in res.stdout.split() + NOT_INPUTS:
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO_ROOT / rel, root / rel)
    _git(root, "init", "-q")
    _git(root, "add", "-A")
    return root


def _hashes(root):
    out = {}
    for s in ("build", "gate"):
        res = _inputs("hash", "--set", s, root=root)
        assert res.returncode == 0, res.stderr
        out[s] = res.stdout.strip()
    return out


def _edit(root, rel, fn):
    p = root / rel
    p.write_bytes(fn(p.read_bytes()))


GDALMANAGER = "src/main/scala/com/databricks/labs/gbx/rasterx/gdal/GDALManager.scala"
PURE_PYTHON = "python/geobrix/src/databricks/labs/gbx/core/crs.py"
NATIVE_PYTHON = ("python/geobrix/src/databricks/labs/gbx/_native/", "python/geobrix/src/_gbx_native_path.py")
NOT_INPUTS = [GDALMANAGER, PURE_PYTHON, "python/geobrix/README.md", "scripts/native/arch-gate.json"]
APPEND = lambda b: b + b"\n# x\n"  # noqa: E731
EDITS = {  # edit -> (does the build hash move, does the gate hash move)
    "a pin value": ("scripts/native/pins.conf", lambda b: b.replace(b"GDAL_VERSION=3", b"GDAL_VERSION=4", 1), True, True),
    "a pins.conf comment": ("scripts/native/pins.conf", lambda b: b"# a note\n" + b, False, False),
    "a recipe the Dockerfile copies": ("scripts/native/build-gdal.sh", APPEND, True, True),
    "the Dockerfile": ("scripts/native/build.Dockerfile", APPEND, True, True),
    "the env wheel's builder": ("scripts/native/build_env_wheel.py", APPEND, False, True),
    "an env file": ("python/geobrix/envs/env6/core.txt", APPEND, False, True),
    "the activation rules": ("python/geobrix/src/databricks/labs/gbx/_native/_rules.py", APPEND, False, True),
    "a check script": ("scripts/native/check-env-wheel.sh", APPEND, False, True),
    "GDALManager (no gate check runs it)": (GDALMANAGER, lambda b: b + b"\n// x\n", False, False),
    "a pure-Python package source (arch-neutral)": (PURE_PYTHON, APPEND, False, False),
    "pyproject.toml (the wheel's)": ("python/geobrix/pyproject.toml", APPEND, False, True),
    "README.md (no gate check reads it)": ("python/geobrix/README.md", APPEND, False, False),
    "the stamp itself": ("scripts/native/arch-gate.json", lambda b: b + b" ", False, False),
}


@pytest.mark.parametrize("edit", EDITS)
def test_the_native_input_hash_moves_with_the_inputs_only(tree, edit):
    rel, fn, build_moves, gate_moves = EDITS[edit]
    before = _hashes(tree)
    assert all(re.fullmatch(r"[0-9a-f]{64}", h) for h in before.values())
    _edit(tree, rel, fn)
    after = _hashes(tree)
    assert (after["build"] != before["build"], after["gate"] != before["gate"]) == (build_moves, gate_moves)


def test_the_gate_is_the_env_wheels_inputs_less_the_pure_python_package():
    """One list of the env wheel's inputs, build_env_wheel.py's GIT_INPUTS, with two exclusions: README.md and the
    pure-Python package tree (everything under python/geobrix/src but the payload's activation, _native/** and
    _gbx_native_path.py), which is arch-neutral and tested by x86_64 CI. Exactly:
    GIT_INPUTS - {README.md, pure Python} <= gate <= that + the build set + the check scripts."""
    tree = ast.parse((REPO_ROOT / "scripts/native/build_env_wheel.py").read_text())
    (git_inputs,) = [
        ast.literal_eval(n.value)
        for n in tree.body
        if isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "GIT_INPUTS" for t in n.targets)
    ]

    def tracked(*specs):
        res = subprocess.run(["git", "-C", str(REPO_ROOT), "ls-files", "--", *specs], capture_output=True, text=True)
        return set(res.stdout.split())

    pure = {f for f in tracked("python/geobrix/src") if not f.startswith(NATIVE_PYTHON)}
    low = tracked(*[s for s in git_inputs if s != "python/geobrix/README.md"]) - pure - {"scripts/native/arch-gate.json"}
    build = set(_inputs("files", "--set", "build").stdout.split())
    checks = {f"scripts/native/{f}" for f in ("verify-native.sh", "JniSmoke.java", "check-env-wheel.sh", "base_held.py")}
    gate = set(_inputs("files", "--set", "gate").stdout.split())
    assert pure and low and not low - gate, sorted(low - gate)[:10]
    assert not gate - (low | build | checks), sorted(gate - (low | build | checks))[:10]
    assert {"python/geobrix/pyproject.toml", "python/geobrix/requirements-build.txt", "python/geobrix/envs/pyproject.env.toml",
            "python/geobrix/src/_gbx_native_path.py", "scripts/native/JniSmoke.java"} <= gate
    assert not gate & pure and "python/geobrix/README.md" not in gate


# --- the binding sdists through the package index (CI) ---------------------------------------------------------------


class _Index:
    """A PEP 503 index on 127.0.0.1 that wants the netrc credentials: /simple/rasterio/ links the file."""

    def __init__(self, tmp_path, data):
        import http.server
        import threading

        index = self
        self.data, self.hits = data, []

        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                index.hits.append(self.path)
                if self.headers.get("Authorization") != "Basic " + base64.b64encode(b"ci:tok").decode():
                    self.send_response(401)
                    self.end_headers()
                    return
                if self.path == "/simple/rasterio/":
                    body = b'<html><a href="../../files/rasterio-1.5.2.tar.gz">rasterio-1.5.2.tar.gz</a></html>'
                elif self.path == "/files/rasterio-1.5.2.tar.gz":
                    body = index.data
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        self.server = http.server.HTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        netrc = tmp_path / "netrc"
        netrc.write_text("machine 127.0.0.1\nlogin ci\npassword tok\n")
        netrc.chmod(0o600)
        self.env = {"PIP_INDEX_URL": f"http://127.0.0.1:{self.server.server_port}/simple", "NETRC": str(netrc)}


@pytest.mark.parametrize("pinned", ["right", "wrong"])
def test_ci_fetches_the_binding_sdists_through_the_package_index(tmp_path, pinned):
    """With GBX_SDISTS_FROM=index (the builder's env), fetch-lib.sh's fetch_sdist reads the sdist from the index pip is
    configured with (its netrc credentials), never from the hard-coded file host (no curl), and keeps it only when its
    sha256 is the pin; a mismatch fails and writes nothing."""
    data = b"an sdist"
    want = hashlib.sha256(data).hexdigest() if pinned == "right" else "0" * 64
    index = _Index(tmp_path, data)
    shims = tmp_path / "shims"
    shims.mkdir()
    (shims / "curl").write_text(f"#!/bin/sh\necho curl >> {tmp_path / 'curl.log'}\nexit 7\n")
    (shims / "curl").chmod(0o755)
    dest = tmp_path / "sdists" / "rasterio-1.5.2.tar.gz"
    env = dict(os.environ, **index.env, GBX_SDISTS_FROM="index", PATH=f"{shims}{os.pathsep}{os.environ['PATH']}")
    script = f'source "{REPO_ROOT}/scripts/native/fetch-lib.sh"; fetch_sdist "{dest}" "{want}" rasterio-1.5.2.tar.gz'
    try:
        res = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True)
    finally:
        index.server.shutdown()

    assert not (tmp_path / "curl.log").exists(), "the sdist went to the hard-coded host"
    assert "/simple/rasterio/" in index.hits and "127.0.0.1" not in res.stdout + res.stderr
    if pinned == "right":
        assert res.returncode == 0, res.stdout + res.stderr
        assert dest.read_bytes() == data
    else:
        assert res.returncode == 1 and "PREFETCH FAILED" in res.stderr and "pins.conf says" in res.stderr
        assert not dest.exists()

    (job,) = _load(BUILDER)["jobs"].values()
    assert job["env"]["GBX_SDISTS_FROM"] == "index"


INDEX_CASES = {  # case -> (what `pip config list` prints, the netrc has the index's login, the outcome)
    # CI's branch: jfrog-pip-bootstrap sets PIP_CONFIG_FILE, not PIP_INDEX_URL; pip download's own index-url wins
    "pip config's index (CI)": ("download.index-url='{index}'\nglobal.index-url='http://127.0.0.1:1/simple'\n", True,
                                "fetched"),
    "no index configured": ("global.timeout='60'\n", True, "no package index is configured"),
    "a 401: no credentials for the index": ("global.index-url='{index}'\n", False, "HTTPError 401"),
}


@pytest.mark.parametrize("case", INDEX_CASES)
def test_the_sdist_index_is_pips_and_fails_closed(tmp_path, case):
    config, login, outcome = INDEX_CASES[case]
    fetcher = _native("index_fetch")
    data = b"an sdist"
    index = _Index(tmp_path, data)
    env = {"NETRC": index.env["NETRC"] if login else str(tmp_path / "no-netrc")}
    dest = tmp_path / "sdists" / "rasterio-1.5.2.tar.gz"
    args = ("rasterio", dest.name, hashlib.sha256(data).hexdigest(), dest)
    try:
        if outcome == "fetched":
            assert fetcher.fetch(*args, env=env, pip_config=config.format(index=index.env["PIP_INDEX_URL"])) == outcome
            assert dest.read_bytes() == data
        else:
            with pytest.raises(fetcher.Fail, match=outcome):
                fetcher.fetch(*args, env=env, pip_config=config.format(index=index.env["PIP_INDEX_URL"]))
            assert not dest.exists() and not dest.with_name(dest.name + ".part").exists()
    finally:
        index.server.shutdown()
    assert bool(index.hits) is (case != "no index configured")


@pytest.mark.parametrize("index", ["//ci:tok3n@secret-host.example/simple", "http://ci:tok3n@secret-host.example/simple"])
def test_no_index_url_or_credential_reaches_the_log(tmp_path, index):
    """An index URL urllib cannot open (scheme-less: ValueError; a login in it: InvalidURL) fails closed with the
    exception's class only: the text of either names the URL, and a public repository's CI log is public."""
    env = dict(os.environ, PIP_INDEX_URL=index, NETRC=str(tmp_path / "no-netrc"))
    res = subprocess.run(
        [sys.executable, str(INDEX_FETCH), "rasterio", "rasterio-1.5.2.tar.gz", "0" * 64, str(tmp_path / "f")],
        env=env, capture_output=True, text=True,
    )
    out = res.stdout + res.stderr
    assert res.returncode == 1 and "PREFETCH FAILED" in res.stderr
    assert not re.search(r"secret-host|tok3n|Traceback", out), out


def _logs(tmp_path, bad=None):
    """Synthetic logs of the four checks per architecture; `bad` = (arch, check) whose log has a failure."""
    args = []
    for a in ARCHES:
        ok = bad != (a, "verify_native")
        log = tmp_path / f"verify-native-{a}.txt"
        log.write_text("ok: one\nok: two\n" + (f"VERIFY OK arch={a} gdal=3.13.3\n" if ok else "VERIFY FAILED: x\n"))
        args += ["--verify-native", f"{a}={log}"]
        for env in (5, 6):
            ok = bad != (a, f"env{env}")
            rep = tmp_path / f"env{env}-verify-{a}.txt"
            tail = f"ENV_WHEEL_VERIFIED env={env} arch={a} failures=0\n" if ok else "CHECK FAILED: y\n"
            rep.write_text("ok: a\nok: b\nok: c\n" + tail)
            args += ["--env-wheel", f"{env}:{a}={rep}"]
    return args


@pytest.mark.parametrize(
    "case, edit, code",
    [
        ("unchanged inputs", None, 0),
        ("a native input changed", ("scripts/native/build-deps.sh", APPEND), 1),
        ("an env file changed", ("python/geobrix/envs/env5/base.txt", APPEND), 1),
        ("a comment in pins.conf", ("scripts/native/pins.conf", lambda b: b"# a note\n" + b), 0),
        ("only GDALManager changed", (GDALMANAGER, APPEND), 0),
        ("a pure-Python package source changed", (PURE_PYTHON, APPEND), 0),
        ("the payload's activation changed", ("python/geobrix/src/databricks/labs/gbx/_native/_rules.py", APPEND), 1),
    ],
)
def test_ci_fails_a_stale_arch_gate_stamp(tree, tmp_path, case, edit, code):
    res = _inputs("write-stamp", "--built-from", "0" * 40, *_logs(tmp_path), root=tree)
    assert res.returncode == 0, res.stdout + res.stderr
    stamp = json.loads((tree / "scripts/native/arch-gate.json").read_text())
    assert stamp["native_inputs_sha256"] == _hashes(tree)["gate"]
    assert stamp["results"]["x86_64"]["env6"] == {"passed": 3, "failed": 0}
    assert stamp["results"]["aarch64"]["verify_native"] == {"passed": 2, "failed": 0}
    if edit:
        _edit(tree, *edit)

    res = _inputs("check-stamp", root=tree)

    assert res.returncode == code, res.stdout + res.stderr
    if code:
        assert edit[0] in res.stdout and "gbx-native-verify-arches.sh" in res.stdout


@pytest.mark.parametrize("bad", [("aarch64", "verify_native"), ("x86_64", "env5")])
def test_a_failed_check_writes_no_stamp(tree, tmp_path, bad):
    stamp = tree / "scripts/native/arch-gate.json"
    before = stamp.read_bytes()
    res = _inputs("write-stamp", "--built-from", "0" * 40, *_logs(tmp_path, bad=bad), root=tree)
    assert res.returncode == 1
    assert stamp.read_bytes() == before
    assert bad[0] in res.stdout


def _commit(root, msg):
    _git(root, "add", "-A")
    _git(root, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", msg)
    return subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()


def _wheels(root, sha, dirty=False, manifest=b'{"arch": "a"}'):
    """dist/env5 and dist/env6 wheels built at `sha`, and the stage-native trees they were built from."""
    for env in (5, 6):
        d = root / "dist" / f"env{env}"
        d.mkdir(parents=True)
        with zipfile.ZipFile(d / f"geobrix-0.6.0+env{env}-py3-none-any.whl", "w") as zf:
            zf.writestr("databricks/labs/gbx/_build_info.py", f'GIT_SHA = "{sha}"\nGIT_DIRTY = {dirty}\n')
            for a in ARCHES:
                zf.writestr(f"databricks/labs/gbx/_native/{a}/MANIFEST.json", manifest)
    for a in ARCHES:
        m = root / "build/native/out" / a / "stage-native/gdal-native/MANIFEST.json"
        m.parent.mkdir(parents=True)
        m.write_bytes(b'{"arch": "a"}')


ENV_FILE = "python/geobrix/envs/env6/core.txt"
PROVENANCE = {  # case -> (what happens after the wheels' commit, exit code, a word of the message)
    "one build from HEAD's inputs": (None, 0, "BUILT_FROM="),
    "a dirty wheel": ("dirty", 1, "dirty"),
    "an env file committed after the wheels": ("commit-env", 1, ENV_FILE),
    "only a check script committed after (no rebuild)": ("commit-check", 0, "BUILT_FROM="),
    "an uncommitted native input": ("uncommitted", 1, "PRECONDITION MISSING"),
    # neither is hashed: a gate run's own stamp, not yet committed, and an untracked file beside the inputs
    "only the stamp changed (uncommitted)": ("stamp", 0, "BUILT_FROM="),
    "an untracked scratch file in scripts/native": ("scratch", 0, "BUILT_FROM="),
    "the tree is not the wheels' build": ("manifest", 1, "is not the"),
}


@pytest.mark.parametrize("case", PROVENANCE)
def test_the_gate_verifies_only_trees_and_wheels_built_from_heads_inputs(tree, case):
    after, code, word = PROVENANCE[case]
    sha = _commit(tree, "inputs")
    _wheels(tree, sha, dirty=after == "dirty", manifest=b"{}" if after == "manifest" else b'{"arch": "a"}')
    (tree / ".gitignore").write_text("/dist/\n/build/\n")
    _commit(tree, "ignore")
    if after == "commit-env":
        _edit(tree, ENV_FILE, APPEND)
        _commit(tree, "env")
    elif after == "commit-check":
        _edit(tree, "scripts/native/verify-native.sh", APPEND)
        _commit(tree, "check")
    elif after == "uncommitted":
        _edit(tree, ENV_FILE, APPEND)
    elif after == "stamp":
        _edit(tree, "scripts/native/arch-gate.json", lambda b: b + b" ")
    elif after == "scratch":
        (tree / "scripts/native/scratch.log").write_text("a local note\n")

    res = _inputs("provenance", root=tree)

    assert res.returncode == code, res.stdout + res.stderr
    assert word in res.stdout


# --- the JVM payload ------------------------------------------------------------------------------------------------


def _native_tree(root, extra_lib=None, tampered=None):
    lib = root / "lib"
    for d in ("pkgconfig", "cmake"):
        (lib / d).mkdir(parents=True)
        (lib / d / "x").write_text("dev only")
    for name in ("libgdal-0123abcd.so.39", "libgdalalljni.so"):
        (lib / name).write_bytes(b"\x7fELF " + name.encode())
    if extra_lib:
        (lib / extra_lib).write_bytes(b"\x7fELF stray")
    (root / "hdf5-plugins").mkdir()
    (root / "hdf5-plugins" / "lib__nch5zstd.so").write_bytes(b"\x7fELF plugin")
    (root / "bin").mkdir()
    (root / "bin" / "projsync").write_bytes(b"\x7fELF projsync")
    (root / "bin" / "gdalinfo").write_bytes(b"\x7fELF gdalinfo")
    for rel in ("share/proj/proj.db", "share/gdal/osmconf.ini", "include/gdal.h"):
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(rel)
    files = {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob("*") if p.is_file()
    }
    if tampered:  # the bytes on disk are no longer the build's (a partly restored cache entry)
        (root / tampered).write_bytes(b"truncated")
    manifest = {"arch": "x86_64", "jni": "libgdalalljni.so", "sonames": {"libgdal.so.39": "libgdal-0123abcd.so.39"},
                "versions": {"gdal": "3.13.3"}, "files": files}
    (root / "MANIFEST.json").write_text(json.dumps(manifest))


@pytest.mark.parametrize(
    "stray, tampered",
    [(None, None), ("libsqlite3.so.0", None), (None, "lib/libgdal-0123abcd.so.39"), (None, "share/proj/proj.db")],
)
def test_the_jvm_payload_is_the_layout_gdalmanager_reads(tmp_path, stray, tampered):
    """<root>/_native/<arch>/lib/libgdalalljni.so with <root>/_native/share beside it: GDALManager's payload layout (its
    data is <dir>/../share/{proj,gdal}), as the env wheel lays it out; a lib/ that is not MANIFEST.json's, or a file
    whose bytes are not the ones MANIFEST.json records, is refused."""
    native, out = tmp_path / "gdal-native", tmp_path / "payload"
    _native_tree(native, stray, tampered)

    res = subprocess.run(
        [sys.executable, str(JVM_PAYLOAD), "--native", str(native), "--arch", "x86_64", "--out", str(out)],
        capture_output=True, text=True,
    )

    if stray or tampered:
        assert res.returncode == 1 and (stray or tampered) in res.stdout + res.stderr
        return
    assert res.returncode == 0, res.stdout + res.stderr
    d = out / "_native" / "x86_64"
    assert sorted(p.name for p in (d / "lib").iterdir()) == ["libgdal-0123abcd.so.39", "libgdalalljni.so"]
    assert sorted(p.name for p in (d / "bin").iterdir()) == ["projsync"]
    assert (d / "hdf5-plugins" / "lib__nch5zstd.so").is_file() and (d / "MANIFEST.json").is_file()
    assert (out / "_native/share/proj/proj.db").is_file() and (out / "_native/share/gdal/osmconf.ini").is_file()
    assert f"root={out}" in res.stdout


# --- the gate command -----------------------------------------------------------------------------------------------


def _gate(tmp_path, *args, docker_ok=True):
    fake = tmp_path / "bin"
    fake.mkdir(exist_ok=True)
    (fake / "docker").write_text("#!/bin/sh\nexit %d\n" % (0 if docker_ok else 1))
    (fake / "docker").chmod(0o755)
    env = dict(os.environ, PATH=f"{fake}{os.pathsep}{os.environ['PATH']}")
    return subprocess.run(["bash", str(GATE), *args], capture_output=True, text=True, env=env)


def _stamp_bytes():
    p = REPO_ROOT / "scripts/native/arch-gate.json"
    return p.read_bytes() if p.exists() else None


def test_the_gate_help_runs_nothing(tmp_path):
    before = _stamp_bytes()
    res = _gate(tmp_path, "--help")
    assert res.returncode == 0
    assert "arch-gate.json" in res.stdout and "--verify" in res.stdout
    assert _stamp_bytes() == before


def test_the_gate_without_docker_stops_before_any_check(tmp_path):
    before = _stamp_bytes()
    res = _gate(tmp_path, docker_ok=False)
    assert res.returncode == 1
    assert "PRECONDITION MISSING" in res.stdout
    assert _stamp_bytes() == before
