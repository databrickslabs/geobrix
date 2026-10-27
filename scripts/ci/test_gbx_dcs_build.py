"""Tests for scripts/commands/gbx-dcs-build.sh: the refusals before a build, and (with
--target base) the image-history check for a package index and tag-after-verify.

The command runs with a fake `docker` (see conftest.py): nothing reaches a daemon or a
registry, and the repository is always gbx-test/none. The package index used here is a
made-up value; the tests assert the command never prints it.
"""

import hashlib
import importlib.util
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_REPO = (
    "gbx-test/none"  # conftest.py runs every command with GBX_DCS_REPO set to this
)
BASE_TAG = f"{TEST_REPO}:base-env6-arm"
RC_TAG = f"{TEST_REPO}:0.0.2-env6-arm-rc"
INDEX = "https://ci-user@idx.internal:8443/simple"
INDEX_HOST = "idx.internal"
SECRET_READ = (
    'RUN /bin/sh -c export PIP_INDEX_URL="$(cat /run/secrets/pip_index_url)" '
    "&& pip install /tmp/geobrix.whl"
)
# The public base image's own history names its build-time index host.
BASE_HISTORY = [
    f"ENV UV_DEFAULT_INDEX=https://{INDEX_HOST}/base",
    "RUN /bin/sh -c apt-get install -y python3",
    "ADD file:base in /",
]


def _base_build(cmd, own, env=None):
    cmd.state.update(history_own=own, history_base=BASE_HISTORY)
    return cmd.run("--target", "base", env=env)


def _tagged(cmd):
    return [c for c in cmd.calls if c[0] == "tag"]


# --- refusals before any build (default --target release) ---------------------------


def test_two_jars_are_refused(dcs_build):
    dcs_build.jar("0.0.2")
    dcs_build.jar("0.0.3")
    dcs_build.wheel("0.0.2")
    res = dcs_build.run()
    assert res.returncode == 1
    assert (
        "exactly one target/geobrix-*-jar-with-dependencies.jar, found 2" in res.output
    )
    assert dcs_build.commands() == ["info"]


def test_jar_and_wheel_versions_must_match(dcs_build):
    dcs_build.jar("0.0.2")
    dcs_build.wheel("0.0.3")
    res = dcs_build.run()
    assert res.returncode == 1
    assert "JAR version 0.0.2 != wheel version 0.0.3" in res.output
    assert dcs_build.commands() == ["info"]


def test_a_jar_bundling_the_scala_library_is_refused(dcs_build):
    dcs_build.jar("0.0.2", ["com/databricks/labs/gbx/A.class", "scala/Predef.class"])
    dcs_build.wheel("0.0.2")
    res = dcs_build.run()
    assert res.returncode == 1
    assert "scala/Predef.class" in res.output and "rebuild the JAR" in res.output
    assert dcs_build.commands() == ["info"]


# --- --target base: history check, then the tag ---------------------------------------


def test_clean_history_is_tagged_after_every_check(dcs_build):
    """The base image's own index host is not this build's; the Dockerfile's literal
    secret read holds no value. The final tag comes after both history reads."""
    res = _base_build(
        dcs_build, [SECRET_READ, "COPY gdal /usr"], env={"PIP_INDEX_URL": INDEX}
    )
    assert res.returncode == 0, res.output
    assert f"IMAGE={BASE_TAG}" in res.output
    cmds = dcs_build.commands()
    assert cmds.index("tag") > max(i for i, c in enumerate(cmds) if c == "history")
    ((_, tmp, final),) = _tagged(dcs_build)
    assert tmp.startswith("gbx-dcs-build-tmp:") and final == BASE_TAG
    build = next(c for c in dcs_build.calls if c[0] == "build")
    assert any(a.startswith("id=pip_index_url,src=") for a in build)
    assert INDEX not in res.output and INDEX_HOST not in res.output
    assert not any(INDEX in a for c in dcs_build.calls for a in c)


@pytest.mark.parametrize(
    "leak, counts",
    [
        (f"ENV PIP_INDEX_URL={INDEX}", "PIP_INDEX_URL values: 1"),
        (f"RUN /bin/sh -c pip install -i https://{INDEX_HOST}/x pkg", "index host: 1"),
    ],
    ids=["index-value", "index-host"],
)
def test_a_package_index_in_this_builds_layers_is_not_tagged(dcs_build, leak, counts):
    res = _base_build(dcs_build, [leak, "COPY gdal /usr"], env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 1
    assert "VERIFY FAILED: package index found in the history" in res.output
    assert counts in res.output and f"not tagged as {BASE_TAG}" in res.output
    assert _tagged(dcs_build) == []
    assert INDEX not in res.output and INDEX_HOST not in res.output


def test_layers_that_cannot_be_told_from_the_base_are_not_tagged(dcs_build):
    """No history line of its own: the check would inspect nothing, so it fails."""
    res = _base_build(dcs_build, [])
    assert res.returncode == 1
    assert "could not isolate this build's layers" in res.output
    assert _tagged(dcs_build) == []


def test_a_failed_image_check_is_not_tagged(dcs_build):
    dcs_build.state["run_rc"] = 1
    res = _base_build(dcs_build, ["COPY gdal /usr"])
    assert res.returncode == 1
    assert "VERIFY FAILED: GDAL 3.11.4" in res.output
    assert _tagged(dcs_build) == [] and "history" not in dcs_build.commands()
    assert any(
        c[0] == "rmi" and c[1].startswith("gbx-dcs-build-tmp:") for c in dcs_build.calls
    )


def test_a_failed_build_is_not_tagged(dcs_build):
    dcs_build.state["build_rc"] = 3
    res = _base_build(dcs_build, ["COPY gdal /usr"])
    assert res.returncode == 3
    assert "BUILD FAILED: docker build exited 3" in res.output
    assert _tagged(dcs_build) == []


# --- an option without its value is a usage error ------------------------------------


@pytest.mark.parametrize("option", ["--arch", "--target", "--log"])
def test_an_option_without_its_value_is_a_usage_error(dcs_build, option):
    """`--arch` at the end used to die on an unbound `$2` (exit 1, no message)."""
    res = dcs_build.run(option)
    assert res.returncode == 2, res.output
    assert f"{option} needs a value" in res.output and "Usage:" in res.output
    assert "unbound variable" not in res.output
    assert dcs_build.calls == []


# --- --target release: the -rc tag comes after every verify step -----------------------


def _release_build(cmd, env=None):
    cmd.jar("0.0.2")
    cmd.wheel("0.0.2")
    cmd.state.update(
        history_own=["COPY geobrix /databricks"], history_base=BASE_HISTORY
    )
    return cmd.run(env=env)


def test_the_rc_tag_comes_after_every_release_verify_step(dcs_build):
    res = _release_build(dcs_build)
    assert res.returncode == 0, res.output
    assert f"IMAGE={RC_TAG}" in res.output
    cmds = dcs_build.commands()
    checks = [i for i, c in enumerate(cmds) if c in ("pull", "run", "history")]
    assert cmds.count("run") == 4 and cmds.count("history") == 2
    ((_, tmp, final),) = _tagged(dcs_build)
    assert tmp.startswith("gbx-dcs-build-tmp:") and final == RC_TAG
    assert cmds.index("tag") > max(checks)


@pytest.mark.parametrize(
    "failing_run, message",
    [
        (0, "VERIFY FAILED: GDAL 3.11.4"),
        (1, "VERIFY FAILED: image contents"),
        (2, "VERIFY FAILED: could not read requests"),
        (3, "VERIFY FAILED: Python environment"),
    ],
)
def test_a_failed_release_verify_step_leaves_no_rc_tag(dcs_build, failing_run, message):
    dcs_build.state["run_rcs"] = [1 if i == failing_run else 0 for i in range(4)]
    res = _release_build(dcs_build)
    assert res.returncode == 1
    assert message in res.output and f"not tagged as {RC_TAG}" in res.output
    assert _tagged(dcs_build) == []
    assert any(
        c[0] == "rmi" and c[1].startswith("gbx-dcs-build-tmp:") for c in dcs_build.calls
    )


def test_the_base_image_is_pulled_before_it_is_read(dcs_build):
    """docker build leaves the base in BuildKit's cache, so the checks that read it need a pull."""
    res = _release_build(dcs_build)
    assert res.returncode == 0, res.output
    cmds = dcs_build.commands()
    assert cmds.index("build") < cmds.index("pull") < cmds.index("run")
    assert cmds.index("pull") < cmds.index("history")
    dcs_build.state["pull_rc"] = 1
    (dcs_build.log).unlink()
    res = dcs_build.run()
    assert res.returncode == 1 and "could not pull" in res.output
    assert "run" not in dcs_build.commands() and _tagged(dcs_build) == []


# --- the JAR that is checked, hashed and built is the copy in the build context -----------


def _python3_shim(cmd):
    """A python3 that records the JAR check's arguments, then runs the real interpreter."""
    seen = cmd.bin.parent / "python3.args"
    shim = cmd.bin / "python3"
    shim.write_text(
        "#!/bin/sh\n"
        f'case "$1" in *check_jar_packaging.py) echo "$1 $2" >> "{seen}" ;; esac\n'
        f'exec "{sys.executable}" "$@"\n'
    )
    shim.chmod(0o755)
    return seen


def test_the_checked_and_hashed_jar_is_the_copy_in_the_build_context(dcs_build):
    seen = _python3_shim(dcs_build)
    res = _release_build(dcs_build)
    assert res.returncode == 0, res.output
    checker, jar = seen.read_text().split()
    assert Path(checker).name == "check_jar_packaging.py"
    name = "geobrix-0.0.2-jar-with-dependencies.jar"
    assert Path(jar).name == name
    assert Path(jar).parent != dcs_build.root / "target"  # the copy, not target/
    sha = hashlib.sha256((dcs_build.root / "target" / name).read_bytes()).hexdigest()
    contents = next(c for c in dcs_build.calls if c[0] == "run" and sha in c[-1])
    assert f"{sha}  /databricks/jars/{name}" in contents[-1]


def test_the_hash_is_of_the_copy_not_of_target(dcs_build):
    """Rebuild target/ after the copy (during the JAR check): the image gets the copy, so the
    digest the verify step expects must be the copy's, not the new file's."""
    target_jar = dcs_build.jar("0.0.2")
    name = target_jar.name
    dcs_build.wheel("0.0.2")
    dcs_build.state.update(history_own=["COPY geobrix /x"], history_base=BASE_HISTORY)
    copied_sha = hashlib.sha256(target_jar.read_bytes()).hexdigest()
    shim = dcs_build.bin / "python3"
    shim.write_text(
        "#!/bin/sh\n"
        f'case "$1" in *check_jar_packaging.py) printf rebuilt >> "{target_jar}" ;; esac\n'
        f'exec "{sys.executable}" "$@"\n'
    )
    shim.chmod(0o755)
    res = dcs_build.run()
    assert res.returncode == 0, res.output
    assert hashlib.sha256(target_jar.read_bytes()).hexdigest() != copied_sha
    contents = next(
        c for c in dcs_build.calls if c[0] == "run" and "sha256sum -c" in c[-1]
    )
    assert f"{copied_sha}  /databricks/jars/{name}" in contents[-1]


def test_a_missing_python3_is_named_not_blamed_on_the_jar(dcs_build):
    dcs_build.jar("0.0.2")
    dcs_build.wheel("0.0.2")
    res = dcs_build.run(env={"PATH": dcs_build.path_without("python3")})
    assert res.returncode == 1, res.output
    assert "PRECONDITION MISSING: python3" in res.output
    assert "rebuild the JAR" not in res.output
    assert dcs_build.commands() == ["info"]


# --- libgdalalljni.so must load, not only exist -----------------------------------------


def _run_verify_script(cmd, tmp_path, ldd_output):
    """Run the image-verify script the build sent to `docker run` on this machine, with the
    library path, gdalinfo and ldd stood in for."""
    script = next(
        c[-1] for c in cmd.calls if c[0] == "run" and "libgdalalljni" in c[-1]
    )
    lib = tmp_path / "libgdalalljni.so"
    lib.write_text("")
    tools = tmp_path / "verify-bin"
    tools.mkdir(exist_ok=True)
    (tools / "gdalinfo").write_text(
        "#!/bin/sh\necho 'GDAL 3.11.4, released 2025/01/01'\n"
    )
    (tools / "ldd").write_text(
        f"#!/bin/sh\necho \"$@\" > {tmp_path}/ldd.args\ncat <<'EOF'\n{ldd_output}\nEOF\n"
    )
    for tool in tools.iterdir():
        tool.chmod(0o755)
    res = subprocess.run(
        ["sh", "-c", script.replace("/usr/lib/libgdalalljni.so", str(lib))],
        env={"PATH": f"{tools}{os.pathsep}/usr/bin{os.pathsep}/bin"},
        capture_output=True,
        text=True,
    )
    return res, (tmp_path / "ldd.args").read_text().strip(), str(lib)


def test_the_image_check_fails_on_a_jni_library_that_cannot_load(dcs_build, tmp_path):
    res = _base_build(dcs_build, ["COPY gdal /usr"])
    assert res.returncode == 0, res.output
    ok = "\tlibgdal.so.37 => /usr/local/lib/libgdal.so.37 (0x00007f)"
    run, args, lib = _run_verify_script(dcs_build, tmp_path, ok)
    assert run.returncode == 0, run.stdout + run.stderr
    assert args == lib  # ldd was asked about the library itself
    broken = "\tlibgdal.so.37 => not found"
    run, _, _ = _run_verify_script(dcs_build, tmp_path, broken)
    assert run.returncode != 0
    assert "libgdal.so.37" in run.stdout + run.stderr  # names what is unresolved


def test_a_jni_library_that_cannot_load_is_reported_as_a_verify_failure(dcs_build):
    dcs_build.state["run_rc"] = 1
    res = _base_build(dcs_build, ["COPY gdal /usr"])
    assert res.returncode == 1
    assert "unable to load its libraries" in res.output
    assert f"not tagged as {BASE_TAG}" in res.output and _tagged(dcs_build) == []


# --- pip does not echo the package index into the build log ---------------------------------


def test_the_wheel_install_is_quiet_so_the_index_stays_out_of_the_build_log():
    """`pip install` prints "Looking in indexes: <url>" at its default verbosity."""
    dockerfile = (REPO_ROOT / "scripts" / "docker" / "dcs" / "Dockerfile").read_text()
    installs = [
        line
        for line in dockerfile.replace("\\\n", " ").splitlines()
        if "pip install" in line
    ]
    assert installs, "expected a pip install in the DCS Dockerfile"
    for line in installs:
        flags = line.split("pip install", 1)[1].split("&&")[0].split()
        assert "-q" in flags or "--quiet" in flags, line


def _wheel_install_script():
    """The Dockerfile's RUN statement that installs the wheel, without its --mount flags."""
    text = (REPO_ROOT / "scripts" / "docker" / "dcs" / "Dockerfile").read_text()
    run = next(
        line
        for line in text.replace("\\\n", " ").splitlines()
        if line.startswith("RUN") and "pip install" in line
    )
    return re.sub(r"^RUN\s+(--\S+\s+)*", "", run)


FAKE_PIP = """#!/bin/sh
# Stands in for /databricks/python3/bin/python: `-m pip ...` is faked, anything else runs
# the real interpreter (the redactor is a Python script).
if [ "$1" = "-m" ] && [ "$2" = "pip" ]; then
    case "$3" in
        list) echo "numpy==2.0.0" ;;
        check) echo "No broken requirements found." ;;
        install)
            if [ "${FAKE_PIP_INSTALL_RC:-0}" -ne 0 ]; then
                echo "WARNING: Retrying after connection broken: ${FAKE_INDEX}/geobrix/" >&2
                echo "ERROR: Could not fetch URL ${FAKE_INDEX}/geobrix/: no route to host"
                echo "ERROR: user ${FAKE_USER} refused by ${FAKE_HOST}" >&2
            fi
            exit "${FAKE_PIP_INSTALL_RC:-0}" ;;
    esac
    exit 0
fi
exec "%s" "$@"
"""


def _run_wheel_install(tmp_path, install_rc, script=None):
    """Run the Dockerfile's wheel-install RUN script on this machine, with the secret, the
    interpreter and /tmp stood in for and a fake pip that fails the way an unreachable index does.
    """
    work = tmp_path / "image-tmp"
    work.mkdir()
    secret = tmp_path / "pip_index_url"
    secret.write_text(INDEX)
    python = tmp_path / "python"
    python.write_text(FAKE_PIP % sys.executable)
    python.chmod(0o755)
    redactor = REPO_ROOT / "scripts" / "docker" / "dcs" / "redact_pip_log.py"
    script = script or _wheel_install_script()
    # /tmp/ first: the other replacements are paths under the pytest temp dir, which may be in /tmp.
    for image_path, local in (
        ("/tmp/", f"{work}/"),
        ("/run/secrets/pip_index_url", secret),
        ("/databricks/python3/bin/python", python),
        ("/mnt/redact_pip_log.py", redactor),
    ):
        script = script.replace(image_path, str(local))
    return subprocess.run(
        ["sh", "-c", script],
        env={
            "PATH": f"/usr/bin{os.pathsep}/bin",
            "GEOBRIX_VERSION": "0.0.2",
            "FAKE_PIP_INSTALL_RC": str(install_rc),
            "FAKE_INDEX": INDEX,
            "FAKE_HOST": INDEX_HOST,
            "FAKE_USER": "ci-user",
        },
        capture_output=True,
        text=True,
    )


def test_a_failing_wheel_install_prints_its_errors_without_the_package_index(tmp_path):
    """pip's warnings and errors on an unreachable index name the index; the build log must not."""
    res = _run_wheel_install(tmp_path, install_rc=1)
    out = res.stdout + res.stderr
    assert res.returncode != 0  # the failure still fails the build
    assert (
        "Could not fetch URL" in out and "<package-index>" in out
    )  # and says what failed
    for leaked in (INDEX, INDEX_HOST, "ci-user"):
        assert leaked not in out


def test_a_successful_wheel_install_prints_nothing_about_the_package_index(tmp_path):
    res = _run_wheel_install(tmp_path, install_rc=0)
    assert res.returncode == 0, res.stdout + res.stderr
    assert INDEX_HOST not in res.stdout + res.stderr


def test_the_log_redactor_removes_every_part_of_the_index():
    spec = importlib.util.spec_from_file_location(
        "redact_pip_log",
        REPO_ROOT / "scripts" / "docker" / "dcs" / "redact_pip_log.py",
    )
    redactor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(redactor)
    url = "https://ci-user:s3cret@idx.internal:8443/simple/"
    text = (
        f"Looking in indexes: {url}\nfetch https://idx.internal:8443/simple/x\n"
        "user ci-user password s3cret host idx.internal\n"
    )
    out = redactor.redact(text, url)
    for leaked in ("ci-user", "s3cret", "idx.internal", "8443"):
        assert leaked not in out
    assert out.count("<package-index>") >= 4
    assert redactor.redact(text, "") == text  # no index configured: nothing to remove


def test_the_build_context_holds_the_dockerfile_and_the_log_redactor(
    dcs_build, tmp_path
):
    listing = tmp_path / "context.json"
    dcs_build.state["build_context_log"] = str(listing)
    res = _release_build(dcs_build)
    assert res.returncode == 0, res.output
    held = json.loads(listing.read_text())
    assert {"Dockerfile", "redact_pip_log.py"} <= set(held)
    assert any(n.endswith("-jar-with-dependencies.jar") for n in held)


# --- Ctrl-C and SIGTERM clean up what the build created ------------------------------------


def _interrupt_a_build(cmd, tmp_path, sig, extra_args=()):
    """Start a build whose `docker build` hangs, signal its whole process group the way a
    terminal does for Ctrl-C, and return (process, output, the $TMPDIR it used)."""
    tmp = tmp_path / "tmp"
    tmp.mkdir()
    marker = tmp_path / "building"
    cmd.state["build_hang"] = str(marker)
    cmd.jar("0.0.2")
    cmd.wheel("0.0.2")
    proc = cmd.start(*extra_args, env={"TMPDIR": str(tmp), "PIP_INDEX_URL": INDEX})
    try:
        deadline = time.time() + 30
        while not marker.exists() and time.time() < deadline:
            time.sleep(0.1)
        assert marker.exists(), "the build never started"
        held = sorted(p.name for p in tmp.iterdir())
        assert len(held) == 2 and any(n.startswith("gbx-dcs-pip-index.") for n in held)
        os.killpg(proc.pid, sig)  # the whole group, as Ctrl-C does
        out, _ = proc.communicate(timeout=30)
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.communicate()
    return proc, out, tmp


def _assert_cleaned_up(cmd, proc, out, tmp, code):
    assert proc.returncode == code, out
    assert list(tmp.iterdir()) == []
    assert INDEX not in out
    assert _tagged(cmd) == []
    assert (
        any(  # and the temporary tag, which a build that got as far as verify may hold
            c[0] == "rmi" and c[1].startswith("gbx-dcs-build-tmp:") for c in cmd.calls
        )
    )


@pytest.mark.parametrize(
    "sig, code", [(signal.SIGINT, 130), (signal.SIGTERM, 143)], ids=["int", "term"]
)
def test_an_interrupted_build_removes_its_temp_files(dcs_build, tmp_path, sig, code):
    """The build context and the 0600 file holding the package index live in $TMPDIR until the
    build ends; an interrupt must not leave them (the second one holds the index)."""
    proc, out, tmp = _interrupt_a_build(dcs_build, tmp_path, sig)
    _assert_cleaned_up(dcs_build, proc, out, tmp, code)


@pytest.mark.parametrize(
    "sig, code", [(signal.SIGINT, 130), (signal.SIGTERM, 143)], ids=["int", "term"]
)
def test_an_interrupted_build_with_a_log_still_cleans_up(
    dcs_build, tmp_path, sig, code
):
    """With --log, stdout is a pipe into `tee`, and a group SIGTERM kills the tee as well. The
    shell then writes to a pipe with no reader: it must not die of SIGPIPE before the trap has
    removed the temp files (bash 3.2 reports the killed `docker` to that pipe first), nor
    exit with a signal status."""
    log = tmp_path / "logs" / "build.log"
    proc, out, tmp = _interrupt_a_build(
        dcs_build, tmp_path, sig, extra_args=("--log", str(log))
    )
    _assert_cleaned_up(dcs_build, proc, out, tmp, code)
    assert INDEX not in log.read_text()
    if (
        sig == signal.SIGINT
    ):  # the tee survives Ctrl-C (a background job ignores SIGINT)
        assert "Interrupted" in log.read_text()


def test_an_interrupt_still_cleans_up_when_the_test_runner_ignores_sigint(
    dcs_build, tmp_path
):
    """A pytest started with SIGINT ignored (`cmd &` in a non-interactive shell) would hand that
    disposition to the child, and bash cannot trap a signal that was ignored at entry.
    """
    before = signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        proc, out, tmp = _interrupt_a_build(dcs_build, tmp_path, signal.SIGINT)
    finally:
        signal.signal(signal.SIGINT, before)
    _assert_cleaned_up(dcs_build, proc, out, tmp, 130)
