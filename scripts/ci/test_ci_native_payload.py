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

import json
import os
import re
import shutil
import subprocess
import sys

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
    another key (a payload built from other inputs is not this one); saved only on pushes to main and the release
    branch."""
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
    # the builder builds what the cache holds: both targets, with the command the developers use
    run = " ".join(s.get("run", "") for s in builder["steps"])
    assert "gbx-build-native.sh --arch x86_64" in run


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
    for wf, name, job, jobs in found:
        where = f"{wf}:{name}"
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


# --- the native-input hash ------------------------------------------------------------------------------------------


def _inputs(*args, root=None):
    cmd = [sys.executable, str(INPUTS), *args] + (["--root", str(root)] if root else [])
    return subprocess.run(cmd, capture_output=True, text=True)


def _git(root, *args):
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


@pytest.fixture
def tree(tmp_path):
    """A scratch git repository holding copies of the real gate inputs, plus one file that is no input."""
    res = _inputs("files", "--set", "gate")
    assert res.returncode == 0, res.stderr
    root = tmp_path / "repo"
    for rel in res.stdout.split():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO_ROOT / rel, root / rel)
    (root / "scripts/native/build_env_wheel.py").write_text("# not a native input\n")
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
EDITS = {  # edit -> (does the build hash move, does the gate hash move)
    "a pin value": ("scripts/native/pins.conf", lambda b: b.replace(b"GDAL_VERSION=3", b"GDAL_VERSION=4", 1), True, True),
    "a pins.conf comment": ("scripts/native/pins.conf", lambda b: b"# a note\n" + b, False, False),
    "a recipe the Dockerfile copies": ("scripts/native/build-gdal.sh", lambda b: b + b"\n# x\n", True, True),
    "the Dockerfile": ("scripts/native/build.Dockerfile", lambda b: b + b"\n# x\n", True, True),
    "GDALManager": (GDALMANAGER, lambda b: b + b"\n// x\n", False, True),
    "the activation rules": ("python/geobrix/src/databricks/labs/gbx/_native/_rules.py", lambda b: b + b"\n# x\n", False, True),
    "a file that is no input": ("scripts/native/build_env_wheel.py", lambda b: b + b"# x\n", False, False),
}


@pytest.mark.parametrize("edit", EDITS)
def test_the_native_input_hash_moves_with_the_inputs_only(tree, edit):
    rel, fn, build_moves, gate_moves = EDITS[edit]
    before = _hashes(tree)
    assert all(re.fullmatch(r"[0-9a-f]{64}", h) for h in before.values())
    _edit(tree, rel, fn)
    after = _hashes(tree)
    assert (after["build"] != before["build"], after["gate"] != before["gate"]) == (build_moves, gate_moves)


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
        ("a native input changed", ("scripts/native/build-deps.sh", lambda b: b + b"\n# x\n"), 1),
        ("a comment in pins.conf", ("scripts/native/pins.conf", lambda b: b"# a note\n" + b), 0),
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
        assert "build-deps.sh" in res.stdout and "gbx-native-verify-arches.sh" in res.stdout


@pytest.mark.parametrize("bad", [("aarch64", "verify_native"), ("x86_64", "env5")])
def test_a_failed_check_writes_no_stamp(tree, tmp_path, bad):
    res = _inputs("write-stamp", "--built-from", "0" * 40, *_logs(tmp_path, bad=bad), root=tree)
    assert res.returncode == 1
    assert not (tree / "scripts/native/arch-gate.json").exists()
    assert bad[0] in res.stdout


# --- the JVM payload ------------------------------------------------------------------------------------------------


def _native_tree(root, extra_lib=None):
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
    manifest = {"arch": "x86_64", "jni": "libgdalalljni.so", "sonames": {"libgdal.so.39": "libgdal-0123abcd.so.39"},
                "versions": {"gdal": "3.13.3"}}
    (root / "MANIFEST.json").write_text(json.dumps(manifest))


@pytest.mark.parametrize("stray", [None, "libsqlite3.so.0"])
def test_the_jvm_payload_is_the_layout_gdalmanager_reads(tmp_path, stray):
    """<root>/_native/<arch>/lib/libgdalalljni.so with <root>/_native/share beside it: GDALManager's payload layout (its
    data is <dir>/../share/{proj,gdal}), as the env wheel lays it out; a lib/ that is not MANIFEST.json's is refused."""
    native, out = tmp_path / "gdal-native", tmp_path / "payload"
    _native_tree(native, stray)

    res = subprocess.run(
        [sys.executable, str(JVM_PAYLOAD), "--native", str(native), "--arch", "x86_64", "--out", str(out)],
        capture_output=True, text=True,
    )

    if stray:
        assert res.returncode == 1 and stray in res.stdout + res.stderr
        return
    assert res.returncode == 0, res.stdout + res.stderr
    d = out / "_native" / "x86_64"
    assert sorted(p.name for p in (d / "lib").iterdir()) == ["libgdal-0123abcd.so.39", "libgdalalljni.so"]
    assert sorted(p.name for p in (d / "bin").iterdir()) == ["projsync"]
    assert (d / "hdf5-plugins" / "lib__nch5zstd.so").is_file() and (d / "MANIFEST.json").is_file()
    assert (out / "_native/share/proj/proj.db").is_file() and (out / "_native/share/gdal/osmconf.ini").is_file()
    assert f"root={out}" in res.stdout
