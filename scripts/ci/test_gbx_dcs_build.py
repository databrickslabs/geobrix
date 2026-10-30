"""Tests for scripts/commands/gbx-dcs-build.sh: the refusals before a build, what the build context
holds, the verify steps, the image-history check for a package index and tag-after-verify.

The command runs with a fake `docker` (see conftest.py): nothing reaches a daemon or a
registry, and the repository is always gbx-test/none. The package index used here is a
made-up value; the tests assert the command never prints it. The wheels the image installs
come from a cache the test fills (conftest.DcsCommand.lock), so nothing is downloaded.
"""

import hashlib
import json
import os
import signal
import sys
import time
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_REPO = (
    "gbx-test/none"  # conftest.py runs every command with GBX_DCS_REPO set to this
)
RC_TAG = f"{TEST_REPO}:0.0.2-env6-arm-rc"
INDEX = "https://ci-user@idx.internal:8443/simple"
INDEX_HOST = "idx.internal"
# The public base image's own history names its build-time index host.
BASE_HISTORY = [
    f"ENV UV_DEFAULT_INDEX=https://{INDEX_HOST}/base",
    "RUN /bin/sh -c apt-get install -y python3",
    "ADD file:base in /",
]


def _tagged(cmd):
    return [c for c in cmd.calls if c[0] == "tag"]


# --- a release build needs one env6 wheel, a clean JAR in it and the lock --------------------


def _release_build(cmd, *args, env=None, version="0.0.2"):
    cmd.env_wheel(version)
    cmd.lock()
    cmd.state.update(
        history_own=["COPY geobrix /databricks"], history_base=BASE_HISTORY
    )
    return cmd.run(*args, env=env)


def test_two_wheels_are_refused(dcs_build):
    dcs_build.env_wheel("0.0.2")
    dcs_build.env_wheel("0.0.3")
    res = dcs_build.run()
    assert res.returncode == 1
    assert "exactly one dist/env6/geobrix-*+env6-py3-none-any.whl" in res.output
    assert "found 2" in res.output and dcs_build.commands() == ["info"]


def test_a_missing_wheel_is_refused(dcs_build):
    res = dcs_build.run()
    assert res.returncode == 1 and "found 0" in res.output
    assert dcs_build.commands() == ["info"]


def test_a_jar_bundling_the_scala_library_is_refused(dcs_build):
    dcs_build.env_wheel("0.0.2", ["com/databricks/labs/gbx/A.class", "scala/Predef.class"])
    dcs_build.lock()
    res = dcs_build.run()
    assert res.returncode == 1
    assert "scala/Predef.class" in res.output and "rebuild the wheel" in res.output
    assert dcs_build.commands() == ["info"]


def test_a_wheel_whose_jar_has_another_version_is_refused(dcs_build):
    whl = dcs_build.env_wheel("0.0.2")
    renamed = whl.with_name("geobrix-0.0.3+env6-py3-none-any.whl")
    whl.rename(renamed)
    dcs_build.lock()
    res = dcs_build.run()
    assert res.returncode == 1
    assert "JAR version 0.0.2 != wheel version 0.0.3" in res.output
    assert dcs_build.commands() == ["info"]


def test_a_wheel_without_a_jar_is_refused(dcs_build):
    whl = dcs_build.env_wheel("0.0.2")
    with zipfile.ZipFile(whl, "w") as zf:
        zf.writestr("databricks/labs/gbx/__init__.py", "")
    dcs_build.lock()
    res = dcs_build.run()
    assert res.returncode == 1 and "could not take the JAR out of" in res.output
    assert dcs_build.commands() == ["info"]


def test_a_missing_lock_is_named_with_the_way_to_make_it(dcs_build):
    dcs_build.env_wheel("0.0.2")
    res = dcs_build.run()
    assert res.returncode == 1
    assert "PRECONDITION MISSING" in res.output and "dcs-aarch64.lock" in res.output
    assert "--relock" in res.output and dcs_build.commands() == ["info"]


def test_a_lock_that_does_not_carry_the_built_rasterio_is_refused(dcs_build):
    dcs_build.env_wheel("0.0.2")
    dcs_build.lock()
    built = next((dcs_build.root / "build/native/out/aarch64/bindings/dist").glob("rasterio-*.whl"))
    built.write_bytes(b"a rasterio built after the lock")
    res = dcs_build.run()
    assert res.returncode == 1
    assert "WHEELHOUSE FAILED" in res.output and "relock" in res.output
    assert dcs_build.commands() == ["info"]


def test_a_missing_python3_is_named_not_blamed_on_the_jar(dcs_build):
    dcs_build.env_wheel("0.0.2")
    dcs_build.lock()
    res = dcs_build.run(env={"PATH": dcs_build.path_without("python3")})
    assert res.returncode == 1, res.output
    assert "PRECONDITION MISSING: python3" in res.output
    assert "rebuild the wheel" not in res.output
    assert dcs_build.commands() == ["info"]


def test_the_base_target_is_retired(dcs_build):
    """The image compiles no GDAL any more, so there is no base stage to stop at."""
    res = dcs_build.run("--target", "base")
    assert res.returncode == 1 and "Unknown option: --target" in res.output
    assert dcs_build.calls == []


def test_relock_regenerates_the_lock_before_anything_is_built(dcs_build):
    """--relock runs dcs_lock.py relock; the fake docker lists no packages, so it stops there."""
    dcs_build.env_wheel("0.0.2")
    dcs_build.lock()
    res = dcs_build.run("--relock")
    assert res.returncode == 1
    assert "RELOCK FAILED" in res.output and "build" not in dcs_build.commands()
    assert dcs_build.commands()[:2] == ["info", "run"]  # info, then the base image's package list


# --- an option without its value is a usage error ------------------------------------


@pytest.mark.parametrize("option", ["--arch", "--log"])
def test_an_option_without_its_value_is_a_usage_error(dcs_build, option):
    """`--arch` at the end used to die on an unbound `$2` (exit 1, no message)."""
    res = dcs_build.run(option)
    assert res.returncode == 2, res.output
    assert f"{option} needs a value" in res.output and "Usage:" in res.output
    assert "unbound variable" not in res.output
    assert dcs_build.calls == []


# --- the build: what the context holds and what the image is told ----------------------------


def test_the_build_context_holds_the_wheels_the_lock_and_the_jar(dcs_build, tmp_path):
    listing, modes = tmp_path / "context.json", tmp_path / "modes.json"
    dcs_build.state.update(build_context_log=str(listing), build_context_modes=str(modes))
    res = _release_build(dcs_build)
    assert res.returncode == 0, res.output
    held = set(json.loads(listing.read_text()))
    assert held == {
        "Dockerfile",
        "dcs.lock",
        "geobrix-wheel",
        "wheelhouse",
        "geobrix-0.0.2-jar-with-dependencies.jar",
    }
    # the JAR is group- and world-readable in the context, so the image stores it once, readable by the JVM
    assert json.loads(modes.read_text())["geobrix-0.0.2-jar-with-dependencies.jar"] == "0o644"


def test_the_wheelhouse_has_the_needed_wheels_and_our_rasterio(dcs_build, tmp_path):
    tree = tmp_path / "tree.json"
    dcs_build.state["build_context_tree"] = str(tree)
    res = _release_build(dcs_build)
    assert res.returncode == 0, res.output
    files = json.loads(tree.read_text())
    # the base image has click and idna 3.11: only idna (the lock moves it) and our rasterio are staged
    assert [f for f in files if f.startswith("wheelhouse/")] == [
        "wheelhouse/idna-3.20-py3-none-any.whl",
        "wheelhouse/rasterio-1.5.2+gbx1-cp312-cp312-linux_aarch64.whl",
    ]
    assert "geobrix-wheel/geobrix-0.0.2+env6-py3-none-any.whl" in files
    build = next(c for c in dcs_build.calls if c[0] == "build")
    assert "linux/arm64" in build
    args = " ".join(build)
    assert "GBX_ARCH=aarch64" in args and "GEOBRIX_VERSION=0.0.2" in args
    assert "BASE_IMAGE=databricksruntime/environment:v6-standard-arm" in args


def test_the_amd64_build_uses_the_x86_base_and_lock(dcs_build):
    dcs_build.env_wheel("0.0.2")
    dcs_build.lock("x86_64")
    dcs_build.state.update(history_own=["COPY geobrix /databricks"], history_base=BASE_HISTORY)
    res = dcs_build.run("--arch", "amd64")
    assert res.returncode == 0, res.output
    assert f"IMAGE={TEST_REPO}:0.0.2-env6-x86-rc" in res.output
    build = next(c for c in dcs_build.calls if c[0] == "build")
    assert "GBX_ARCH=x86_64" in " ".join(build)
    assert "BASE_IMAGE=databricksruntime/environment:v6-standard" in " ".join(build)


# --- the -rc tag comes after every verify step -----------------------------------------


def test_the_rc_tag_comes_after_every_release_verify_step(dcs_build):
    res = _release_build(dcs_build)
    assert res.returncode == 0, res.output
    assert f"IMAGE={RC_TAG}" in res.output
    cmds = dcs_build.commands()
    checks = [i for i, c in enumerate(cmds) if c in ("pull", "run", "history")]
    assert cmds.count("run") == 2 and cmds.count("history") == 2
    ((_, tmp, final),) = _tagged(dcs_build)
    assert tmp.startswith("gbx-dcs-build-tmp:") and final == RC_TAG
    assert cmds.index("tag") > max(checks)


@pytest.mark.parametrize(
    "failing_run, message",
    [
        (0, "VERIFY FAILED: the image does not carry the payload's stack"),
        (1, "VERIFY FAILED: image contents"),
    ],
)
def test_a_failed_release_verify_step_leaves_no_rc_tag(dcs_build, failing_run, message):
    dcs_build.state["run_rcs"] = [1 if i == failing_run else 0 for i in range(2)]
    res = _release_build(dcs_build)
    assert res.returncode == 1
    assert message in res.output and f"not tagged as {RC_TAG}" in res.output
    assert _tagged(dcs_build) == []
    assert any(
        c[0] == "rmi" and c[1].startswith("gbx-dcs-build-tmp:") for c in dcs_build.calls
    )


def test_a_failed_build_is_not_tagged(dcs_build):
    dcs_build.state["build_rc"] = 3
    res = _release_build(dcs_build)
    assert res.returncode == 3
    assert "BUILD FAILED: docker build exited 3" in res.output
    assert _tagged(dcs_build) == []


def test_the_oracle_runs_offline_with_the_lock_mounted_read_only(dcs_build):
    res = _release_build(dcs_build)
    assert res.returncode == 0, res.output
    oracle = next(c for c in dcs_build.calls if c[0] == "run" and "/mnt/dcs/verify_image.py" in c)
    assert oracle[oracle.index("--network") + 1] == "none"
    mounts = [oracle[i + 1] for i, a in enumerate(oracle) if a == "-v"]
    assert len(mounts) == 3 and all(m.endswith(":ro") for m in mounts)
    assert any(m.endswith("dcs-aarch64.lock:/mnt/dcs.lock:ro") for m in mounts)
    assert any(m.endswith("dcs-base-aarch64.txt:/mnt/dcs-base.txt:ro") for m in mounts)
    # GeoBrix's own rasterio build is what the image must carry (the pin file's version and revision)
    assert oracle[oracle.index("-e") + 1] == "GBX_RASTERIO=1.5.2+gbx1"
    assert oracle[-1] == "/mnt/dcs/verify_image.py"


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


# --- the JAR the image gets is the wheel's own, checked and hashed as the copy in the context ---


def test_the_hash_the_image_must_match_is_the_wheels_jar(dcs_build):
    res = _release_build(dcs_build)
    assert res.returncode == 0, res.output
    name = "geobrix-0.0.2-jar-with-dependencies.jar"
    with zipfile.ZipFile(next((dcs_build.root / "dist/env6").glob("*.whl"))) as z:
        sha = hashlib.sha256(z.read(f"databricks/labs/gbx/_jar/{name}")).hexdigest()
    contents = next(c for c in dcs_build.calls if c[0] == "run" and "sha256sum -c" in c[-1])
    # the classpath copy and the wheel's copy beside it are the same file; one JAR outside the wheel
    assert f"{sha}  /databricks/jars/{name}" in contents[-1]
    assert f"{sha}  $site/databricks/labs/gbx/_jar/{name}" in contents[-1]
    assert "-not -path '*/databricks/labs/gbx/_jar/*'" in contents[-1]
    assert 'v == \"0.0.2+env6\"' in contents[-1]


# --- the image history carries no package index ---------------------------------------------

INDEX = "https://ci-user@idx.internal:8443/simple"
INDEX_HOST = "idx.internal"


def test_a_clean_history_is_tagged_and_the_index_stays_out_of_the_output(dcs_build):
    """The base image's own index host is not this build's."""
    res = _release_build(dcs_build, env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 0, res.output
    assert INDEX not in res.output and INDEX_HOST not in res.output
    assert not any(INDEX in a for c in dcs_build.calls for a in c)
    build = next(c for c in dcs_build.calls if c[0] == "build")
    assert "--secret" not in build  # the image build takes no index at all


@pytest.mark.parametrize(
    "leak, counts",
    [
        (f"ENV PIP_INDEX_URL={INDEX}", "PIP_INDEX_URL values: 1"),
        (f"RUN /bin/sh -c pip install -i https://{INDEX_HOST}/x pkg", "index host: 1"),
    ],
    ids=["index-value", "index-host"],
)
def test_a_package_index_in_this_builds_layers_is_not_tagged(dcs_build, leak, counts):
    dcs_build.env_wheel("0.0.2")
    dcs_build.lock()
    dcs_build.state.update(history_own=[leak, "COPY gdal /usr"], history_base=BASE_HISTORY)
    res = dcs_build.run(env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 1
    assert "VERIFY FAILED: package index found in the history" in res.output
    assert counts in res.output and f"not tagged as {RC_TAG}" in res.output
    assert _tagged(dcs_build) == []
    assert INDEX not in res.output and INDEX_HOST not in res.output


def test_layers_that_cannot_be_told_from_the_base_are_not_tagged(dcs_build):
    """No history line of its own: the check would inspect nothing, so it fails."""
    dcs_build.env_wheel("0.0.2")
    dcs_build.lock()
    dcs_build.state.update(history_own=[], history_base=BASE_HISTORY)
    res = dcs_build.run()
    assert res.returncode == 1
    assert "could not isolate this build's layers" in res.output
    assert _tagged(dcs_build) == []


# --- the Dockerfile ---------------------------------------------------------------------------

DOCKERFILE = REPO_ROOT / "scripts" / "docker" / "dcs" / "Dockerfile"


def test_the_wheels_are_mounted_for_the_install_not_copied_into_a_layer():
    text = DOCKERFILE.read_text()
    code = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))
    for source in ("geobrix-wheel", "wheelhouse", "dcs.lock"):
        assert f"--mount=type=bind,source={source}," in code, source
    assert "COPY wheelhouse" not in code and "COPY dcs.lock" not in code and "COPY geobrix-wheel" not in code
    assert code.count("COPY ") == 1 and "/databricks/jars/" in code  # the JAR is the one COPY


def test_the_hdf5_plugin_directory_is_a_link_to_the_payload_not_a_copy():
    """A copy would lose the plugins' RUNPATH ($ORIGIN/../lib), so they could not find the payload's libraries."""
    code = DOCKERFILE.read_text()
    assert "ln -sfn" in code and "/usr/local/hdf5/lib/plugin" in code
    assert "cp " not in "\n".join(ln for ln in code.splitlines() if not ln.lstrip().startswith("#"))


def test_the_dockerfile_has_no_syntax_directive():
    """The bundled Dockerfile frontend covers the features used; the directive pulled an image from Docker Hub."""
    assert not DOCKERFILE.read_text().startswith("# syntax=")


# --- Ctrl-C and SIGTERM clean up what the build created ------------------------------------


def _interrupt_a_build(cmd, tmp_path, sig, extra_args=()):
    """Start a build whose `docker build` hangs, signal its whole process group the way a
    terminal does for Ctrl-C, and return (process, output, the $TMPDIR it used)."""
    tmp = tmp_path / "tmp"
    tmp.mkdir()
    marker = tmp_path / "building"
    cmd.state["build_hang"] = str(marker)
    cmd.env_wheel("0.0.2")
    cmd.lock()
    proc = cmd.start(*extra_args, env={"TMPDIR": str(tmp), "PIP_INDEX_URL": INDEX})
    try:
        deadline = time.time() + 30
        while not marker.exists() and time.time() < deadline:
            time.sleep(0.1)
        assert marker.exists(), "the build never started"
        held = sorted(p.name for p in tmp.iterdir())
        assert len(held) == 1 and held[0].startswith("gbx-dcs-build.")
        os.killpg(proc.pid, sig)  # the whole group, as Ctrl-C does
        out, _ = proc.communicate(timeout=30)
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.communicate()
    return proc, out, tmp


def _assert_cleaned_up(cmd, proc, out, tmp, code):
    assert proc.returncode == code, out
    assert "write error" not in out  # the closing line goes to a pipe the signal may have closed
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
    """The build context lives in $TMPDIR until the build ends; an interrupt must not leave it."""
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
