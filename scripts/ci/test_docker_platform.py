"""Tests for the CPU-platform handling of the geobrix-dev image scripts.

The image is built and run for the host's architecture (an arm64 Mac gets a native arm64 image,
not an emulated amd64 one), with one tag per architecture and a GBX_DOCKER_PLATFORM override.
Covered: scripts/docker/gbx-platform.sh (resolution and tags), build_smart.sh (the platform and
tags it builds, and that a package index travels as a BuildKit secret, never a build-arg),
start_docker_with_volumes.sh / start_docker.sh (which image they run), and the
gbx-docker-rebuild / gbx-docker-start commands (they do not disturb a container of the other
architecture).

Everything runs against a fake `docker` (_fake_docker_platform.py) and a fake `uname`: no daemon,
no build. The package index used here is a made-up value; the tests assert it is never printed.
"""

import json
import os
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from check_dev_container_locks import parse_lock
from conftest import REPO_ROOT

DOCKER_DIR = REPO_ROOT / "scripts" / "docker"
COMMANDS = REPO_ROOT / "scripts" / "commands"
FAKE = Path(__file__).with_name("_fake_docker_platform.py")
INDEX = "https://ci-user:s3cret@idx.internal:8443/simple"
TAG = "geobrix-dev:ubuntu24-gdal311-spark"
STAGES = [
    "base",
    "system-deps",
    "hadoop-builder",
    "gdal-builder",
    "final",
]


def good_probe(arch: str) -> list:
    """What the native-stack probe prints for an image that is as intended: the system GDAL/PROJ/GEOS,
    a JNI that resolves libgdal.so.37, a manylinux rasterio wheel, and the lock's wheel versions.
    """
    lock = (
        REPO_ROOT
        / "python"
        / "geobrix"
        / (
            "requirements-dev-container-aarch64.txt"
            if arch == "arm64"
            else "requirements-dev-container.txt"
        )
    )
    pins = parse_lock(lock)[0]
    plat = "aarch64" if arch == "arm64" else "x86_64"
    return [
        "gdal=3.11.4",
        "proj=9.4.0",
        "geos=3.12.1",
        "jni_libgdal=/usr/local/lib/libgdal.so.37",
        "jni_unresolved=0",
        "rasterio_gdal=3.12.1",
        f"rasterio_tags=cp312-cp312-manylinux_2_17_{plat}.manylinux2014_{plat}",
        "rasterio_libs=true",
        f"pyogrio={pins['pyogrio']}",
        f"pyproj={pins['pyproj']}",
        f"shapely={pins['shapely']}",
    ]


class Sandbox:
    """A fake docker and uname first on PATH, and the state/log files they use."""

    def __init__(self, tmp_path: Path, host_arch: str = "arm64"):
        self.tmp = tmp_path
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        docker = self.bin / "docker"
        docker.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{FAKE}" "$@"\n')
        docker.chmod(0o755)
        uname = self.bin / "uname"
        uname.write_text(
            '#!/bin/sh\ncase "$1" in -m) echo "$FAKE_UNAME_M" ;; -s) echo "${FAKE_UNAME_S:-Linux}" ;; '
            '*) exec /usr/bin/uname "$@" ;; esac\n'
        )
        uname.chmod(0o755)
        # `sysctl -n hw.optional.arm64`: set FAKE_HW_ARM64 to answer, leave it unset for "no such key"
        sysctl = self.bin / "sysctl"
        sysctl.write_text(
            '#!/bin/sh\n[ -n "$FAKE_HW_ARM64" ] && echo "$FAKE_HW_ARM64" || exit 1\n'
        )
        sysctl.chmod(0o755)
        self.log = tmp_path / "docker.log"
        self.salt_file = tmp_path / "salt" / "pip-index-fpr-salt"
        self.state_file = tmp_path / "docker.json"
        self.state = {
            "images": {},
            "container": None,
            "history": [],
            "probe": {a: good_probe(a) for a in ("arm64", "amd64")},
        }
        self.host_arch = host_arch
        self.cwd = tmp_path
        # These tests run the scripts in place, and this checkout may be a linked worktree with a
        # .gbx-local.env of its own: pin the container settings to the main checkout's.
        self.local_env = tmp_path / "gbx-local.env"
        self.local_env.write_text(
            f"GBX_DEV_CONTAINER=geobrix-dev\nGBX_DEV_VOLUMES_DIR={tmp_path / 'Volumes'}\n"
        )

    def image(self, ref: str, arch: str):
        self.state["images"][ref] = {"arch": arch}

    def container(self, arch: str, running: bool = True, mount: str = ""):
        self.state["container"] = {
            "image": f"sha256:{arch}",
            "arch": arch,
            "running": running,
            "mount": mount,
        }

    def _exec(self, argv, env=None, cwd=None):
        self.state_file.write_text(json.dumps(self.state))
        full = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("GBX_") and k != "PIP_INDEX_URL"
        }
        full.update(
            PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            FAKE_UNAME_M=self.host_arch,
            FAKE_DOCKER_LOG=str(self.log),
            FAKE_DOCKER_STATE=str(self.state_file),
            GBX_PIP_INDEX_SALT_FILE=str(self.salt_file),
            GBX_LOCAL_ENV_FILE=str(self.local_env),
            TERM="dumb",
        )
        full.update(env or {})
        res = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            env=full,
            cwd=cwd or self.cwd,
            timeout=120,
        )
        res.output = res.stdout + res.stderr
        return res

    def run(self, script, *args, env=None, cwd=None):
        return self._exec(["bash", str(script), *args], env=env, cwd=cwd)

    def sh(self, snippet: str, env=None):
        """Run a snippet with gbx-platform.sh sourced."""
        src = f'source "{DOCKER_DIR / "gbx-platform.sh"}"; {snippet}'
        return self._exec(["bash", "-c", src], env=env)

    @property
    def calls(self) -> list:
        if not self.log.exists():
            return []
        return [json.loads(x) for x in self.log.read_text().splitlines()]

    def of(self, cmd: str) -> list:
        return [c for c in self.calls if c[0] == cmd]

    @property
    def secrets(self) -> list:
        log = Path(str(self.log) + ".secrets")
        if not log.exists():
            return []
        return [json.loads(x) for x in log.read_text().splitlines()]

    def final_state(self) -> dict:
        return json.loads(self.state_file.read_text())


@pytest.fixture
def arm(tmp_path):
    return Sandbox(tmp_path, "arm64")


@pytest.fixture
def x86(tmp_path):
    return Sandbox(tmp_path, "x86_64")


def flag_value(call, name):
    return call[call.index(name) + 1]


# --- gbx-platform.sh: platform resolution -------------------------------------------------


@pytest.mark.parametrize(
    "uname_m,want",
    [
        ("arm64", "linux/arm64"),
        ("aarch64", "linux/arm64"),
        ("x86_64", "linux/amd64"),
        ("amd64", "linux/amd64"),
    ],
)
def test_the_default_is_the_host_architecture(tmp_path, uname_m, want):
    res = Sandbox(tmp_path, uname_m).sh("gbx_docker_platform")
    assert (res.returncode, res.stdout.strip()) == (0, want)


def test_a_rosetta_shell_on_apple_silicon_is_still_arm64(tmp_path):
    """uname -m says x86_64 in a shell started under Rosetta; the CPU says arm64."""
    env = {"FAKE_UNAME_S": "Darwin", "FAKE_HW_ARM64": "1"}
    res = Sandbox(tmp_path, "x86_64").sh("gbx_docker_platform", env=env)
    assert (res.returncode, res.stdout.strip()) == (0, "linux/arm64")


def test_an_intel_mac_and_a_linux_x86_host_stay_amd64(tmp_path):
    sb = Sandbox(tmp_path, "x86_64")
    for env in (
        {"FAKE_UNAME_S": "Darwin"},  # Intel Mac: the key is absent
        {"FAKE_UNAME_S": "Darwin", "FAKE_HW_ARM64": "0"},
        {"FAKE_UNAME_S": "Linux", "FAKE_HW_ARM64": "1"},  # only macOS is asked
    ):
        res = sb.sh("gbx_docker_platform", env=env)
        assert (res.returncode, res.stdout.strip()) == (0, "linux/amd64"), env


@pytest.mark.parametrize(
    "spelling,want",
    [
        ("linux/amd64", "linux/amd64"),
        ("amd64", "linux/amd64"),
        ("x86_64", "linux/amd64"),
        ("linux/arm64", "linux/arm64"),
        ("arm64", "linux/arm64"),
        ("aarch64", "linux/arm64"),
        ("linux/arm64/v8", "linux/arm64"),
    ],
)
@pytest.mark.parametrize("host", ["arm64", "x86_64"])
def test_gbx_docker_platform_overrides_the_host(tmp_path, host, spelling, want):
    res = Sandbox(tmp_path, host).sh(
        "gbx_docker_platform", env={"GBX_DOCKER_PLATFORM": spelling}
    )
    assert (res.returncode, res.stdout.strip()) == (0, want)


def test_an_explicit_platform_beats_the_environment(arm):
    res = arm.sh(
        'gbx_docker_platform "linux/amd64"', env={"GBX_DOCKER_PLATFORM": "linux/arm64"}
    )
    assert res.stdout.strip() == "linux/amd64"


def test_an_unknown_override_is_refused_naming_its_source(arm):
    res = arm.sh("gbx_docker_platform", env={"GBX_DOCKER_PLATFORM": "linux/riscv64"})
    assert res.returncode == 2
    assert "GBX_DOCKER_PLATFORM" in res.stderr and "linux/riscv64" in res.stderr
    res = arm.sh('gbx_docker_platform "windows/amd64"')
    assert res.returncode == 2 and "--platform" in res.stderr


def test_an_unsupported_host_has_no_default(tmp_path):
    res = Sandbox(tmp_path, "riscv64").sh("gbx_docker_platform")
    assert res.returncode == 2 and "riscv64" in res.stderr
    assert "GBX_DOCKER_PLATFORM" in res.stderr  # says how to proceed


def test_there_is_one_tag_per_architecture(arm):
    res = arm.sh("gbx_dev_image_tag linux/arm64; gbx_dev_image_tag amd64")
    assert res.stdout.split() == [f"{TAG}-arm64", f"{TAG}-amd64"]


# --- gbx-platform.sh: which image to run ------------------------------------------------------


def test_run_image_prefers_the_per_arch_tag(arm):
    arm.image(f"{TAG}-amd64", "amd64")
    arm.image(TAG, "amd64")
    assert arm.sh("gbx_dev_run_image linux/amd64").stdout.strip() == f"{TAG}-amd64"


def test_the_old_unsuffixed_amd64_image_is_an_amd64_fallback(arm):
    arm.image(TAG, "amd64")
    assert arm.sh("gbx_dev_run_image linux/amd64").stdout.strip() == TAG


def test_the_old_tag_is_never_used_for_arm64_or_when_it_is_not_amd64(arm):
    arm.image(TAG, "amd64")
    # arm64 asks for its own tag even though only the old one exists
    assert arm.sh("gbx_dev_run_image linux/arm64").stdout.strip() == f"{TAG}-arm64"
    arm.image(TAG, "arm64")
    assert arm.sh("gbx_dev_run_image linux/amd64").stdout.strip() == f"{TAG}-amd64"


def test_a_missing_image_says_how_to_build_it_or_run_the_other(arm):
    res = arm.sh("gbx_require_dev_image linux/arm64")
    assert res.returncode == 1
    assert f"{TAG}-arm64" in res.stderr
    assert "gbx-docker-rebuild.sh --platform linux/arm64" in res.stderr
    assert "GBX_DOCKER_PLATFORM=linux/amd64" in res.stderr


# --- build_smart.sh ---------------------------------------------------------------------------


def build(sb, *args, env=None):
    return sb.run(DOCKER_DIR / "build_smart.sh", "--no-pull", *args, env=env)


def test_the_build_defaults_to_the_host_architecture(arm):
    res = build(arm)
    assert res.returncode == 0, res.output
    builds = arm.of("build")
    assert [flag_value(b, "--target") for b in builds] == STAGES
    assert all(flag_value(b, "--platform") == "linux/arm64" for b in builds)
    assert [flag_value(b, "-t") for b in builds] == [
        f"geobrix-checkpoint:{s}-arm64" for s in STAGES
    ]
    assert ["tag", "geobrix-checkpoint:final-arm64", f"{TAG}-arm64"] in arm.calls
    assert not [
        c for c in arm.of("tag") if c[-1] == TAG
    ]  # the old tag is never written
    assert not any("--build-arg" in c for c in builds)


def test_an_x86_host_builds_amd64(x86):
    assert build(x86).returncode == 0
    assert all(flag_value(b, "--platform") == "linux/amd64" for b in x86.of("build"))
    assert ["tag", "geobrix-checkpoint:final-amd64", f"{TAG}-amd64"] in x86.calls


def test_the_environment_and_the_flag_choose_the_platform(arm):
    assert build(arm, env={"GBX_DOCKER_PLATFORM": "linux/amd64"}).returncode == 0
    assert ["tag", "geobrix-checkpoint:final-amd64", f"{TAG}-amd64"] in arm.calls
    arm.log.unlink()
    res = build(arm, "--platform", "linux/arm64", env={"GBX_DOCKER_PLATFORM": "amd64"})
    assert res.returncode == 0
    assert ["tag", "geobrix-checkpoint:final-arm64", f"{TAG}-arm64"] in arm.calls
    arm.log.unlink()
    assert build(arm, "--platform=amd64").returncode == 0
    assert all(flag_value(b, "--platform") == "linux/amd64" for b in arm.of("build"))


def test_the_base_pull_names_the_platform(arm):
    res = arm.run(DOCKER_DIR / "build_smart.sh")
    assert res.returncode == 0
    assert ["pull", "--platform", "linux/arm64", "ubuntu:24.04"] in arm.calls


def test_an_unknown_platform_builds_nothing(arm):
    res = build(arm, "--platform", "linux/s390x")
    assert res.returncode == 2 and "linux/s390x" in res.stderr
    assert arm.calls == []


def test_a_platform_docker_flag_is_refused(arm):
    """A second --platform after `--` would build one platform and tag another."""
    res = build(arm, "--", "--platform", "linux/amd64")
    assert res.returncode == 2 and "--platform" in res.output
    assert arm.of("build") == []


def test_other_docker_flags_pass_through(arm):
    assert build(arm, "--", "--build-arg", "CORES=4", "--no-cache").returncode == 0
    for b in arm.of("build"):
        assert ["--build-arg", "CORES=4", "--no-cache"] == b[
            b.index("--build-arg") : -1
        ]


@pytest.mark.parametrize(
    "flags",
    [
        ("--build-arg", f"PIP_INDEX_URL={INDEX}"),
        (f"--build-arg=PIP_INDEX_URL={INDEX}",),
    ],
)
def test_the_package_index_is_refused_as_a_build_arg(arm, flags):
    res = build(arm, "--", *flags)
    assert res.returncode == 2 and "PIP_INDEX_URL" in res.output
    assert arm.of("build") == []
    assert INDEX not in res.output and "idx.internal" not in res.output


def test_the_package_index_goes_in_as_a_private_secret_file(arm):
    res = build(arm, env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 0, res.output
    builds = arm.of("build")
    assert all(
        flag_value(b, "--secret").startswith("id=pip_index_url,src=") for b in builds
    )
    # the build saw the URL in a private file, and the file is gone afterwards
    assert len(arm.secrets) == len(STAGES)
    assert all(s["content"] == INDEX and s["mode"] == 0o600 for s in arm.secrets)
    assert not any(Path(s["src"]).exists() for s in arm.secrets)
    # and nowhere else: not an argument of any docker call, not in the output
    assert not any(INDEX in a or "idx.internal" in a for c in arm.calls for a in c)
    assert INDEX not in res.output and "idx.internal" not in res.output


def salt_of(sb) -> bytes:
    return bytes.fromhex(sb.salt_file.read_text().strip())


def fingerprint(sb, url):
    """The 12 hex characters build_smart.sh passes: HMAC-SHA256 of the index keyed with the salt."""
    import hashlib
    import hmac

    return hmac.new(salt_of(sb), url.encode(), hashlib.sha256).hexdigest()[:12]


def final_build_arg(sb):
    final = [b for b in sb.of("build") if flag_value(b, "--target") == "final"][0]
    return flag_value(final, "--build-arg")


def test_a_non_secret_fingerprint_of_the_index_keys_the_final_stage_only(arm):
    """BuildKit's cache ignores secrets, so a changed index must change a build-arg."""
    res = build(arm, env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 0, res.output
    by_stage = {flag_value(b, "--target"): b for b in arm.of("build")}
    want = f"GBX_PIP_INDEX_FPR={fingerprint(arm, INDEX)}"
    assert flag_value(by_stage["final"], "--build-arg") == want
    assert len(want.split("=")[1]) == 12
    assert not any("--build-arg" in b for s, b in by_stage.items() if s != "final")
    # the fingerprint is not the index, and the index is still nowhere in any argument
    assert not any(INDEX in a or "idx.internal" in a for c in arm.calls for a in c)


def test_the_fingerprint_is_salted_so_it_cannot_confirm_a_guessed_index(arm):
    """`docker history` shows the value; a plain hash of a guessed URL must not match it."""
    import hashlib

    res = build(arm, env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 0, res.output
    value = final_build_arg(arm).split("=")[1]
    assert value != hashlib.sha256(INDEX.encode()).hexdigest()[:12]
    assert value != hashlib.sha1(INDEX.encode()).hexdigest()[:12]


def test_the_salt_is_random_private_and_never_shown(arm):
    res = build(arm, env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 0, res.output
    salt_hex = arm.salt_file.read_text().strip()
    assert len(salt_hex) == 64 and len(salt_of(arm)) == 32  # 32 random bytes
    assert stat.S_IMODE(arm.salt_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(arm.salt_file.parent.stat().st_mode) == 0o700
    # never printed, never an argument of a docker call
    assert salt_hex not in res.output
    assert not any(salt_hex in a for c in arm.calls for a in c)
    assert INDEX not in res.output and "idx.internal" not in res.output


def test_the_same_index_keeps_its_fingerprint_so_the_cache_holds(arm):
    assert build(arm, env={"PIP_INDEX_URL": INDEX}).returncode == 0
    first, salt = final_build_arg(arm), arm.salt_file.read_text()
    arm.log.unlink()
    assert build(arm, env={"PIP_INDEX_URL": INDEX}).returncode == 0
    assert final_build_arg(arm) == first
    assert arm.salt_file.read_text() == salt  # generated once, then reused


def test_another_index_or_another_machine_gives_another_fingerprint(arm):
    other = "https://other.internal/simple"
    assert build(arm, env={"PIP_INDEX_URL": INDEX}).returncode == 0
    first = final_build_arg(arm)
    arm.log.unlink()
    assert build(arm, env={"PIP_INDEX_URL": other}).returncode == 0
    assert final_build_arg(arm) == f"GBX_PIP_INDEX_FPR={fingerprint(arm, other)}"
    assert final_build_arg(arm) != first
    # another machine: another salt, so the same index fingerprints differently
    arm.log.unlink()
    arm.salt_file.unlink()
    assert build(arm, env={"PIP_INDEX_URL": INDEX}).returncode == 0
    assert final_build_arg(arm) != first


def test_no_index_means_no_fingerprint_and_no_salt_file(arm):
    assert build(arm).returncode == 0
    assert not any("GBX_PIP_INDEX_FPR" in a for c in arm.of("build") for a in c)
    assert not arm.salt_file.exists()


def test_an_existing_salt_directory_keeps_its_permissions(arm):
    arm.salt_file.parent.mkdir(mode=0o755)
    arm.salt_file.parent.chmod(0o755)
    assert build(arm, env={"PIP_INDEX_URL": INDEX}).returncode == 0
    assert (
        stat.S_IMODE(arm.salt_file.parent.stat().st_mode) == 0o755
    )  # not ours to change
    assert stat.S_IMODE(arm.salt_file.stat().st_mode) == 0o600


def test_without_an_index_there_is_no_secret(arm):
    assert build(arm).returncode == 0
    assert not any("--secret" in b for b in arm.of("build"))
    assert arm.secrets == []


def test_an_index_in_the_image_history_fails_the_build_untagged(arm):
    arm.state["history"] = [f"ENV PIP_INDEX_URL={INDEX}", "RUN apt-get update"]
    res = build(arm, env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 1
    assert "history" in res.output and f"{TAG}-arm64" in res.output
    assert INDEX not in res.output and "idx.internal" not in res.output
    assert [c for c in arm.of("tag")] == []
    assert "geobrix-checkpoint:final-arm64" not in arm.final_state()["images"]


@pytest.mark.parametrize(
    "leak",
    [
        "RUN pip config set global.index-url https://idx.internal:8443/other",  # the host only
        "RUN curl https://ci-user:s3cret@idx.internal:8443/simple/x",  # host, other path
        "ENV PIP_INDEX_URL=https://somewhere.else/simple",  # a value, not this build's
        "ARG UV_INDEX_URL=https://somewhere.else/simple",
    ],
)
def test_the_check_also_matches_the_host_and_any_assigned_index(arm, leak):
    arm.state["history"] = [leak, "RUN apt-get update"]
    res = build(arm, env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 1, leak
    assert "not tagged" in res.output and arm.of("tag") == []
    assert "s3cret" not in res.output and "idx.internal" not in res.output


def test_an_assigned_index_is_caught_even_when_none_is_exported(arm):
    arm.state["history"] = ["ENV PIP_INDEX_URL=https://somewhere.else/simple"]
    res = build(arm)
    assert res.returncode == 1 and arm.of("tag") == []
    assert "somewhere.else" not in res.output


@pytest.mark.parametrize("key", ["history_rc", "inspect_rc"])
def test_a_build_that_cannot_be_read_back_is_not_tagged(arm, key):
    """The check fails closed. The history is non-empty and clean, so the guard against EMPTY output cannot
    be what stops the build: it is the failed docker command itself."""
    arm.state["history"] = ["RUN apt-get update", "COPY lock /tmp/"]
    arm.state[key] = 1
    res = build(arm, env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 1
    assert "Could not read" in res.output and "not tagged" in res.output
    assert arm.of("tag") == []
    assert INDEX not in res.output and "idx.internal" not in res.output


def test_the_public_index_is_not_mistaken_for_a_leak(arm):
    """The Dockerfile's own default names pypi.org in the history; that is no private mirror."""
    arm.state["history"] = ["RUN idx=https://pypi.org/simple/; echo done"]
    res = build(arm, env={"PIP_INDEX_URL": "https://pypi.org/simple/"})
    assert res.returncode == 0, res.output
    assert ["tag", "geobrix-checkpoint:final-arm64", f"{TAG}-arm64"] in arm.calls


def test_a_clean_history_is_tagged_after_the_check(arm):
    arm.state["history"] = [
        'RUN /bin/sh -c idx="$(cat /run/secrets/pip_index_url)"',
        "RUN apt-get update",
    ]
    res = build(arm, env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 0, res.output
    calls = [c[0] for c in arm.calls]
    assert calls.index("history") < calls.index("tag")


# --- start_docker_with_volumes.sh / start_docker.sh -----------------------------------------


def started(sb, script, *args, env=None, cwd=None):
    res = sb.run(DOCKER_DIR / script, *args, env=env, cwd=cwd)
    return res, sb.of("run")


def test_start_runs_the_host_architectures_image(arm):
    arm.image(f"{TAG}-arm64", "arm64")
    res, runs = started(arm, "start_docker_with_volumes.sh")
    assert res.returncode == 0, res.output
    assert flag_value(runs[0], "--platform") == "linux/arm64"
    assert runs[0][-2:] == [f"{TAG}-arm64", "/bin/bash"]


def test_start_on_an_x86_host_runs_the_amd64_image(x86):
    x86.image(f"{TAG}-amd64", "amd64")
    res, runs = started(x86, "start_docker_with_volumes.sh")
    assert res.returncode == 0, res.output
    assert flag_value(runs[0], "--platform") == "linux/amd64"
    assert runs[0][-2] == f"{TAG}-amd64"


def test_start_honours_the_override_and_the_flag(arm):
    arm.image(f"{TAG}-arm64", "arm64")
    arm.image(f"{TAG}-amd64", "amd64")
    _, runs = started(
        arm, "start_docker_with_volumes.sh", env={"GBX_DOCKER_PLATFORM": "linux/amd64"}
    )
    assert (flag_value(runs[0], "--platform"), runs[0][-2]) == (
        "linux/amd64",
        f"{TAG}-amd64",
    )
    arm.log.unlink()
    _, runs = started(
        arm,
        "start_docker_with_volumes.sh",
        "--privileged",
        "--platform",
        "linux/arm64",
        env={"GBX_DOCKER_PLATFORM": "linux/amd64"},
    )
    assert (
        flag_value(runs[0], "--platform") == "linux/arm64" and "--privileged" in runs[0]
    )
    assert runs[0][-2] == f"{TAG}-arm64"


def test_start_falls_back_to_the_old_amd64_tag(arm):
    arm.image(TAG, "amd64")
    _, runs = started(
        arm, "start_docker_with_volumes.sh", env={"GBX_DOCKER_PLATFORM": "amd64"}
    )
    assert (flag_value(runs[0], "--platform"), runs[0][-2]) == ("linux/amd64", TAG)


def test_start_without_the_native_image_runs_nothing_and_says_what_to_do(arm):
    arm.image(TAG, "amd64")  # only the old amd64 image exists
    res, runs = started(arm, "start_docker_with_volumes.sh")
    assert res.returncode == 1 and runs == []
    assert f"{TAG}-arm64" in res.output
    assert "gbx-docker-rebuild.sh --platform linux/arm64" in res.output
    assert "GBX_DOCKER_PLATFORM=linux/amd64" in res.output


def test_start_refuses_an_unknown_option_and_platform(arm):
    arm.image(f"{TAG}-arm64", "arm64")
    res, runs = started(arm, "start_docker_with_volumes.sh", "--bogus")
    assert res.returncode == 2 and runs == []
    res, runs = started(
        arm, "start_docker_with_volumes.sh", "--platform", "linux/s390x"
    )
    assert res.returncode == 2 and runs == []


def test_start_docker_sh_uses_the_same_choice(arm):
    arm.image(f"{TAG}-arm64", "arm64")
    arm.image(f"{TAG}-amd64", "amd64")
    res, runs = started(arm, "start_docker.sh", cwd=REPO_ROOT)
    assert res.returncode == 0, res.output
    assert (flag_value(runs[0], "--platform"), runs[0][-2]) == (
        "linux/arm64",
        f"{TAG}-arm64",
    )
    arm.log.unlink()
    _, runs = started(
        arm, "start_docker.sh", env={"GBX_DOCKER_PLATFORM": "amd64"}, cwd=REPO_ROOT
    )
    assert (flag_value(runs[0], "--platform"), runs[0][-2]) == (
        "linux/amd64",
        f"{TAG}-amd64",
    )


# --- gbx:docker:rebuild ----------------------------------------------------------------------


def rebuild(sb, *args, env=None):
    return sb.run(COMMANDS / "gbx-docker-rebuild.sh", "--no-pull", *args, env=env)


def lifecycle(sb):
    return [c[0] for c in sb.calls if c[0] in ("stop", "rm")]


def test_rebuilding_the_other_architecture_leaves_the_container_alone(arm):
    arm.container("amd64")
    res = rebuild(arm, "--platform", "linux/arm64")
    assert res.returncode == 0, res.output
    assert lifecycle(arm) == []
    assert arm.final_state()["container"]["running"] is True
    assert all(flag_value(b, "--platform") == "linux/arm64" for b in arm.of("build"))
    assert f"{TAG}-arm64" in res.output
    assert "gbx-docker-start.sh --recreate --platform linux/arm64" in res.output


def test_rebuilding_the_same_architecture_replaces_the_container_as_before(arm):
    arm.container("arm64")
    res = rebuild(arm)
    assert res.returncode == 0, res.output
    assert lifecycle(arm) == ["stop", "rm"]
    assert arm.final_state()["container"] is None


def test_keep_container_never_touches_it(arm):
    arm.container("arm64")
    res = rebuild(arm, "--keep-container")
    assert res.returncode == 0, res.output
    assert lifecycle(arm) == []
    assert arm.final_state()["container"]["running"] is True


def test_rebuild_refuses_a_bad_platform_before_touching_anything(arm):
    arm.container("arm64")
    res = rebuild(arm, "--platform", "sparc")
    assert res.returncode == 2
    assert arm.calls == []


def test_a_platform_in_the_extra_docker_flags_is_refused_before_anything_is_touched(
    arm,
):
    """It would override the platform the container-removal decision was made for."""
    arm.container("arm64")
    for flags in (("--platform", "linux/amd64"), ("--platform=linux/amd64",)):
        res = rebuild(arm, "--", *flags)
        assert res.returncode == 2 and "--platform" in res.output, flags
        assert arm.calls == []  # not even a container removal
    res = rebuild(arm, "--", "--build-arg", f"PIP_INDEX_URL={INDEX}")
    assert res.returncode == 2 and arm.calls == []
    assert INDEX not in res.output


def test_extra_docker_flags_reach_docker_after_a_separator(arm):
    arm.container("amd64")
    res = rebuild(arm, "--platform", "linux/arm64", "--", "--build-arg", "CORES=4")
    assert res.returncode == 0, res.output
    builds = arm.of("build")
    assert builds and all(["--build-arg", "CORES=4"] == b[-3:-1] for b in builds)
    assert all(flag_value(b, "--platform") == "linux/arm64" for b in builds)


def test_rebuild_start_passes_the_platform_on(arm):
    arm.image(f"{TAG}-arm64", "arm64")
    res = rebuild(arm, "--start")
    assert res.returncode == 0, res.output
    runs = arm.of("run")
    assert runs and flag_value(runs[0], "--platform") == "linux/arm64"


# --- gbx:docker:start ------------------------------------------------------------------------


def start(sb, *args, env=None):
    return sb.run(COMMANDS / "gbx-docker-start.sh", *args, env=env)


def test_start_keeps_a_running_container_of_the_other_architecture_and_says_so(arm):
    arm.image(f"{TAG}-arm64", "arm64")
    arm.container("amd64")
    res = start(arm)
    assert res.returncode == 0, res.output
    assert lifecycle(arm) == [] and arm.of("run") == []
    assert "amd64" in res.output and "arm64" in res.output
    assert "--recreate --platform linux/arm64" in res.output


def test_recreate_switches_the_container_to_the_requested_architecture(arm):
    arm.image(f"{TAG}-arm64", "arm64")
    arm.container("amd64")
    res = start(arm, "--recreate")
    assert res.returncode == 0, res.output
    assert lifecycle(arm) == ["stop", "rm"]
    runs = arm.of("run")
    assert flag_value(runs[0], "--platform") == "linux/arm64"
    assert runs[0][-2] == f"{TAG}-arm64"
    assert arm.final_state()["container"]["arch"] == "arm64"


def test_start_creates_from_the_requested_platform(arm):
    arm.image(f"{TAG}-amd64", "amd64")
    res = start(arm, "--platform", "linux/amd64")
    assert res.returncode == 0, res.output
    runs = arm.of("run")
    assert (flag_value(runs[0], "--platform"), runs[0][-2]) == (
        "linux/amd64",
        f"{TAG}-amd64",
    )


def test_start_refuses_a_bad_platform(arm):
    res = start(arm, "--platform", "linux/s390x")
    assert res.returncode == 2 and arm.calls == []
    res = start(arm, env={"GBX_DOCKER_PLATFORM": "nope"})
    assert res.returncode == 2 and arm.calls == []


# --- a recreate checks the image BEFORE it stops or removes the working container -----------


def untouched(sb, arch="amd64"):
    state = sb.final_state()
    return (
        lifecycle(sb) == []
        and state["container"]["running"] is True
        and (state["container"]["arch"] == arch)
    )


def test_recreate_without_the_target_image_leaves_the_container_alone(arm):
    arm.image(f"{TAG}-amd64", "amd64")  # the arm64 image was never built
    arm.container("amd64")
    res = start(arm, "--recreate", "--platform", "linux/arm64")
    assert res.returncode == 1
    assert f"{TAG}-arm64" in res.output and "left untouched" in res.output
    assert "gbx-docker-rebuild.sh --platform linux/arm64" in res.output
    assert untouched(arm) and arm.of("run") == []


def test_recreate_with_the_target_image_still_replaces_the_container(arm):
    arm.image(f"{TAG}-arm64", "arm64")
    arm.container("amd64")
    res = start(arm, "--recreate", "--platform", "linux/arm64")
    assert res.returncode == 0, res.output
    assert lifecycle(arm) == ["stop", "rm"] and arm.of("run")


def test_a_worktree_mismatch_recreate_checks_the_image_first_too(arm):
    arm.container("amd64", mount="/some/other/worktree")
    # only the OTHER architecture's image exists: the replacement keeps amd64, so nothing is recreated
    arm.image(f"{TAG}-arm64", "arm64")
    res = start(arm)
    assert res.returncode == 1 and "left untouched" in res.output
    assert f"{TAG}-amd64" in res.output
    assert untouched(arm) and arm.of("run") == []
    arm.image(f"{TAG}-amd64", "amd64")
    res = start(arm)
    assert res.returncode == 0, res.output
    assert lifecycle(arm) == [
        "stop",
        "rm",
    ]  # with the image there, it recreates as before


def test_a_worktree_mismatch_recreate_keeps_the_architecture_of_the_container(arm):
    """Only the mount is wrong: an amd64 container on an arm64 host stays amd64."""
    arm.image(f"{TAG}-arm64", "arm64")
    arm.image(f"{TAG}-amd64", "amd64")
    arm.container("amd64", mount="/some/other/worktree")
    res = start(arm)
    assert res.returncode == 0, res.output
    run = arm.of("run")[0]
    assert (flag_value(run, "--platform"), run[-2]) == ("linux/amd64", f"{TAG}-amd64")


def test_a_worktree_mismatch_recreate_honours_an_explicit_platform(arm):
    arm.image(f"{TAG}-arm64", "arm64")
    arm.image(f"{TAG}-amd64", "amd64")
    arm.container("amd64", mount="/some/other/worktree")
    res = start(arm, "--platform", "linux/arm64")
    assert res.returncode == 0, res.output
    run = arm.of("run")[0]
    assert (flag_value(run, "--platform"), run[-2]) == ("linux/arm64", f"{TAG}-arm64")


def restart(sb, *args, env=None):
    return sb.run(COMMANDS / "gbx-docker-restart.sh", *args, env=env)


def test_restart_privileged_checks_the_image_before_removing_the_container(arm):
    arm.container("amd64")
    res = restart(arm, "--privileged")
    assert res.returncode == 1
    assert f"{TAG}-amd64" in res.output and "left untouched" in res.output
    assert untouched(arm) and arm.of("run") == []
    arm.image(f"{TAG}-amd64", "amd64")
    res = restart(arm, "--privileged")
    assert res.returncode == 0, res.output
    assert lifecycle(arm) == ["stop", "rm"] and arm.of("run")


def test_restart_privileged_recreates_on_the_architecture_it_replaces(arm):
    """Read from the existing container's image, not the host default: an amd64 container on an arm64
    host comes back amd64, even though the arm64 image exists and is the default."""
    arm.image(f"{TAG}-arm64", "arm64")
    arm.image(f"{TAG}-amd64", "amd64")
    arm.container("amd64")
    res = restart(arm, "--privileged")
    assert res.returncode == 0, res.output
    run = arm.of("run")[0]
    assert (flag_value(run, "--platform"), run[-2]) == ("linux/amd64", f"{TAG}-amd64")
    assert "--privileged" in run
    assert arm.final_state()["container"]["arch"] == "amd64"


def test_restart_privileged_is_not_moved_by_the_platform_environment_either(arm):
    arm.image(f"{TAG}-arm64", "arm64")
    arm.image(f"{TAG}-amd64", "amd64")
    arm.container("amd64")
    res = restart(arm, "--privileged", env={"GBX_DOCKER_PLATFORM": "linux/arm64"})
    assert res.returncode == 0, res.output
    assert flag_value(arm.of("run")[0], "--platform") == "linux/amd64"


def test_restart_privileged_on_an_x86_host_keeps_an_arm64_container_arm64(x86):
    x86.image(f"{TAG}-arm64", "arm64")
    x86.image(f"{TAG}-amd64", "amd64")
    x86.container("arm64")
    res = restart(x86, "--privileged")
    assert res.returncode == 0, res.output
    run = x86.of("run")[0]
    assert (flag_value(run, "--platform"), run[-2]) == ("linux/arm64", f"{TAG}-arm64")


def test_restart_privileged_refuses_when_the_architecture_cannot_be_read(arm):
    arm.image(f"{TAG}-arm64", "arm64")
    arm.container("amd64")
    arm.state["container_image_gone"] = True
    res = restart(arm, "--privileged")
    assert res.returncode == 1 and "Cannot tell which architecture" in res.output
    assert "--recreate --platform" in res.output
    assert untouched(arm) and arm.of("run") == []


# --- the post-build native-stack probe -----------------------------------------------------------
#
# After the stages are built and the index check passes, build_smart.sh runs a throwaway container from
# the finished image (never the running geobrix-dev) and compares its native stack with what the image is
# meant to hold. Any difference leaves the image untagged.


def probe_calls(sb):
    return [c for c in sb.of("run") if "--rm" in c]


def test_the_probe_runs_in_a_throwaway_container_of_the_finished_image(arm):
    arm.container("amd64")  # a running geobrix-dev that the probe must not touch
    res = build(arm)
    assert res.returncode == 0, res.output
    probes = probe_calls(arm)
    assert len(probes) == 1
    assert probes[0] == [
        "run",
        "--rm",
        "-i",
        "--platform",
        "linux/arm64",
        "--entrypoint",
        "bash",
        "geobrix-checkpoint:final-arm64",
        "-s",
    ]
    # no `--name geobrix-dev`, no exec into any container
    assert not any("--name" in c for c in arm.of("run"))
    assert arm.of("exec") == []
    assert ["tag", "geobrix-checkpoint:final-arm64", f"{TAG}-arm64"] in arm.calls
    assert "Native stack as expected" in res.output
    # it comes after the index check and before the tag
    order = [c[0] for c in arm.calls if c[0] in ("history", "run", "tag")]
    assert order == ["history", "run", "tag"]


def test_the_probe_on_an_x86_host_uses_the_amd64_platform(x86):
    res = build(x86)
    assert res.returncode == 0, res.output
    assert flag_value(probe_calls(x86)[0], "--platform") == "linux/amd64"
    assert ["tag", "geobrix-checkpoint:final-amd64", f"{TAG}-amd64"] in x86.calls


def probe_with(arch, **changes):
    lines = dict(line.split("=", 1) for line in good_probe(arch))
    lines.update(changes)
    return [f"{k}={v}" for k, v in lines.items()]


PROBE_FAILURES = {
    "GDAL differs": (dict(gdal="3.11.3"), "GDAL: want '3.11.4', got '3.11.3'"),
    "GDAL missing": (dict(gdal=""), "GDAL: want '3.11.4', got 'nothing'"),
    "PROJ differs": (dict(proj="9.4.1"), "PROJ: want '9.4.0', got '9.4.1'"),
    "GEOS differs": (dict(geos="3.12.2"), "GEOS: want '3.12.1', got '3.12.2'"),
    "JNI resolves no libgdal": (
        dict(jni_libgdal=""),
        "libgdalalljni.so does not resolve libgdal.so.37",
    ),
    "JNI resolves another libgdal": (
        dict(jni_libgdal="/usr/lib/libgdal.so.34"),
        "libgdalalljni.so does not resolve libgdal.so.37",
    ),
    "JNI has unresolved libraries": (
        dict(jni_unresolved="2"),
        "libgdalalljni.so has unresolved libraries",
    ),
    "rasterio built from source": (
        dict(rasterio_tags="cp312-cp312-linux_aarch64"),
        "rasterio is not a manylinux wheel",
    ),
    "rasterio without vendored libs": (
        dict(rasterio_libs="false"),
        "no vendored rasterio.libs",
    ),
    "rasterio on the system GDAL": (
        dict(rasterio_gdal="3.11.4"),
        "rasterio's GDAL: want '3.12.1', got '3.11.4'",
    ),
    "rasterio not importable": (
        dict(rasterio_gdal="", rasterio_tags="", rasterio_libs=""),
        "rasterio's GDAL",
    ),
    "pyogrio off the lock": (dict(pyogrio="0.12.1"), "pyogrio (lock): want"),
    "pyproj off the lock": (dict(pyproj="3.8.0"), "pyproj (lock): want"),
    "shapely off the lock": (dict(shapely="2.2.0"), "shapely (lock): want"),
    "shapely missing": (dict(shapely=""), "shapely (lock): want"),
}


@pytest.mark.parametrize("name", sorted(PROBE_FAILURES))
def test_a_native_stack_that_is_not_as_intended_is_not_tagged(arm, name):
    changes, expected = PROBE_FAILURES[name]
    arm.state["probe"]["arm64"] = probe_with("arm64", **changes)
    res = build(arm)
    assert res.returncode == 1, res.output
    assert expected in res.output, res.output
    assert "not tagged" in res.output and f"{TAG}-arm64" in res.output
    assert arm.of("tag") == []  # the final tag is never applied
    # the checkpoint is kept for inspection
    assert "geobrix-checkpoint:final-arm64" in arm.final_state()["images"]


def test_every_failed_expectation_is_listed(arm):
    arm.state["probe"]["arm64"] = probe_with(
        "arm64", gdal="3.10.0", proj="9.0.0", rasterio_tags="cp312-cp312-linux_aarch64"
    )
    res = build(arm)
    assert res.returncode == 1
    for expected in ("GDAL: want", "PROJ: want", "rasterio is not a manylinux wheel"):
        assert expected in res.output
    assert arm.of("tag") == []


def test_a_probe_that_cannot_run_or_prints_nothing_fails_closed(arm):
    arm.state["probe_rc"] = 125
    res = build(arm)
    assert res.returncode == 1 and "Could not run the native-stack probe" in res.output
    assert arm.of("tag") == []
    arm.state["probe_rc"] = 0
    arm.state["probe"]["arm64"] = []
    arm.log.unlink()
    res = build(arm)
    assert res.returncode == 1 and "Could not run the native-stack probe" in res.output
    assert arm.of("tag") == []


def scratch_tree(tmp_path, pyproj_pin_changed_in: str):
    """A copy of the build script, its helpers, the Dockerfile and the two locks under tmp_path, with
    pyproj pinned to another version in one lock (the real locks pin the same versions).
    """
    root = tmp_path / "tree"
    (root / "scripts" / "docker").mkdir(parents=True)
    (root / "python" / "geobrix").mkdir(parents=True)
    for name in (
        "build_smart.sh",
        "gbx-container.sh",
        "gbx-platform.sh",
        "gbx-native-probe.sh",
        "Dockerfile",
    ):
        shutil.copy(DOCKER_DIR / name, root / "scripts" / "docker" / name)
    for lock in (
        "requirements-dev-container.txt",
        "requirements-dev-container-aarch64.txt",
    ):
        text = (REPO_ROOT / "python" / "geobrix" / lock).read_text()
        if lock == pyproj_pin_changed_in:
            text = re.sub(r"(?m)^pyproj==\S+", "pyproj==0.0.1", text)
        (root / "python" / "geobrix" / lock).write_text(text)
    return root / "scripts" / "docker" / "build_smart.sh"


@pytest.mark.parametrize(
    "lock,host,fails",
    [
        ("requirements-dev-container-aarch64.txt", "arm64", True),
        ("requirements-dev-container-aarch64.txt", "x86_64", False),
        ("requirements-dev-container.txt", "x86_64", True),
        ("requirements-dev-container.txt", "arm64", False),
    ],
)
def test_each_image_is_checked_against_its_own_architectures_lock(
    tmp_path, lock, host, fails
):
    sb = Sandbox(tmp_path, host)
    script = scratch_tree(tmp_path, lock)
    res = sb.run(script, "--no-pull")
    if fails:
        assert res.returncode == 1 and "pyproj (lock): want '0.0.1'" in res.output
        assert sb.of("tag") == []
    else:
        assert res.returncode == 0, res.output


def test_a_native_stack_failure_does_not_mask_an_index_leak_and_the_reverse(arm):
    """The index check runs first and removes the checkpoint; the probe never runs on a leaking image."""
    arm.state["history"] = [f"ENV PIP_INDEX_URL={INDEX}"]
    arm.state["probe"]["arm64"] = probe_with("arm64", gdal="0")
    res = build(arm, env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 1 and "recorded in the image history" in res.output
    assert probe_calls(arm) == []
    assert INDEX not in res.output and "idx.internal" not in res.output
