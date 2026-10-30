"""scripts/docker/extras/docker_native_jni.sh and its wiring: the dev container's JVM GDAL is the env6 payload's.

The JAR's GDAL Java classes are the native build's (pom.xml gdal.java.version), and a JVM binds them to the first
libgdalalljni.so on its library path (/usr/lib). The script links that path to the env6 payload's JNI when the
payload is built, and otherwise leaves it alone and prints the fix. gbx:docker:start runs it after the Maven
setup on every start path (running, stopped, created), and gbx:docker:restart after a restart.

No Docker: the script runs against a scratch checkout and a scratch link path (GBX_CONTAINER_ROOT, GBX_JNI_LINK,
GBX_JNI_ARCH); the wiring is checked through the fake docker (_fake_docker_platform.py).
"""

import os
import subprocess

import pytest
from conftest import REPO_ROOT
from test_docker_platform import TAG, Sandbox

SCRIPT = REPO_ROOT / "scripts" / "docker" / "extras" / "docker_native_jni.sh"
JNI_STEP = ["/bin/bash", "/root/geobrix/scripts/docker/extras/docker_native_jni.sh"]
MAVEN_STEP = "docker_maven_setup.sh"


def run(root, link, arch="aarch64"):
    env = dict(
        os.environ,
        GBX_CONTAINER_ROOT=str(root),
        GBX_JNI_LINK=str(link),
        GBX_JNI_ARCH=arch,
    )
    return subprocess.run(
        ["bash", str(SCRIPT)], env=env, capture_output=True, text=True
    )


def payload_jni(root, arch="aarch64"):
    jni = root / "build/env-wheel/env6/payload/_native" / arch / "lib/libgdalalljni.so"
    jni.parent.mkdir(parents=True)
    jni.write_bytes(b"\x7fELF payload jni")
    return jni


def test_the_jvm_link_points_at_the_env6_payloads_jni(tmp_path):
    root = tmp_path / "root"
    jni = payload_jni(root)
    system = tmp_path / "usr-local-lib-jni" / "libgdalalljni.so"
    system.parent.mkdir()
    system.write_bytes(b"\x7fELF system 3.11 jni")
    link = tmp_path / "usr-lib" / "libgdalalljni.so"
    link.parent.mkdir()
    link.symlink_to(system)  # the image's own link

    res = run(root, link)

    assert res.returncode == 0, res.stdout + res.stderr
    assert os.readlink(link) == str(jni)
    assert link.read_bytes() == b"\x7fELF payload jni"
    # idempotent: a second start leaves the same link
    assert run(root, link).returncode == 0 and os.readlink(link) == str(jni)


def test_the_link_follows_the_containers_architecture(tmp_path):
    root = tmp_path / "root"
    payload_jni(root, "aarch64")
    jni = payload_jni(root, "x86_64")
    link = tmp_path / "libgdalalljni.so"
    assert run(root, link, arch="x86_64").returncode == 0
    assert os.readlink(link) == str(jni)


def test_without_a_payload_the_link_is_left_alone_and_the_fix_printed(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    system = tmp_path / "system-jni.so"
    system.write_bytes(b"system")
    link = tmp_path / "libgdalalljni.so"
    link.symlink_to(system)

    res = run(root, link)

    assert res.returncode == 0
    assert os.readlink(link) == str(system)
    assert "NOT LINKED" in res.stdout and "gbx-build-wheel.sh --env 6" in res.stdout


@pytest.mark.parametrize("built", [True, False])
def test_ci_links_the_payload_gbx_native_root_names_or_fails(tmp_path, built):
    """CI's form (scala_build): GBX_NATIVE_ROOT names the payload, and GBX_JNI_REQUIRE=1 makes a missing one an error
    (a JVM left on GDAL 3.11's JNI would abort every GDAL suite), not the dev container's printed fix."""
    payload = tmp_path / "jvm-payload"
    jni = payload / "_native" / "x86_64" / "lib" / "libgdalalljni.so"
    if built:
        jni.parent.mkdir(parents=True)
        jni.write_bytes(b"\x7fELF ci payload jni")
    link = tmp_path / "libgdalalljni.so"
    env = dict(
        os.environ,
        GBX_CONTAINER_ROOT=str(tmp_path / "no-checkout"),
        GBX_NATIVE_ROOT=str(payload),
        GBX_JNI_REQUIRE="1",
        GBX_JNI_LINK=str(link),
        GBX_JNI_ARCH="x86_64",
    )

    res = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)

    if built:
        assert res.returncode == 0, res.stdout + res.stderr
        assert os.readlink(link) == str(jni)
    else:
        assert res.returncode == 1
        assert not link.is_symlink() and str(jni) in res.stdout


def _steps(sb):
    """The container steps the command ran, in order: 'maven' and 'jni'."""
    out = []
    for c in sb.of("exec"):
        if c[-2:] == JNI_STEP:
            out.append("jni")
        elif any(MAVEN_STEP in a for a in c):
            out.append("maven")
    return out


@pytest.mark.parametrize("state", ["running", "stopped", "absent"])
def test_every_start_links_the_jvm_gdal_after_the_maven_setup(tmp_path, state):
    sb = Sandbox(tmp_path, "arm64")
    sb.image(f"{TAG}-arm64", "arm64")
    if state != "absent":
        sb.container("arm64", running=state == "running")

    res = sb.run(sb.commands / "gbx-docker-start.sh")

    assert res.returncode == 0, res.output
    assert _steps(sb) == ["maven", "jni"]


def test_a_restart_links_the_jvm_gdal_after_the_maven_setup(tmp_path):
    sb = Sandbox(tmp_path, "arm64")
    sb.image(f"{TAG}-arm64", "arm64")
    sb.container("arm64")

    res = sb.run(sb.commands / "gbx-docker-restart.sh")

    assert res.returncode == 0, res.output
    assert _steps(sb) == ["maven", "jni"]
