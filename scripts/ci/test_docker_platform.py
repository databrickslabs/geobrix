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
import subprocess
import sys
from pathlib import Path

import pytest
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
    "pdal-builder",
    "final",
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
        self.state_file = tmp_path / "docker.json"
        self.state = {"images": {}, "container": None, "history": []}
        self.host_arch = host_arch
        self.cwd = (
            tmp_path  # start_docker_with_volumes.sh creates sample-data under its cwd
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


def fingerprint(url):
    import hashlib

    return hashlib.sha256(url.encode()).hexdigest()[:12]


def test_a_non_secret_fingerprint_of_the_index_keys_the_final_stage_only(arm):
    """BuildKit's cache ignores secrets, so a changed index must change a build-arg."""
    res = build(arm, env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 0, res.output
    by_stage = {flag_value(b, "--target"): b for b in arm.of("build")}
    want = f"GBX_PIP_INDEX_FPR={fingerprint(INDEX)}"
    assert flag_value(by_stage["final"], "--build-arg") == want
    assert len(want.split("=")[1]) == 12
    assert not any("--build-arg" in b for s, b in by_stage.items() if s != "final")
    # the fingerprint is not the index, and the index is still nowhere in any argument
    assert not any(INDEX in a or "idx.internal" in a for c in arm.calls for a in c)


def test_another_index_gives_another_fingerprint_and_none_gives_none(arm):
    other = "https://other.internal/simple"
    assert build(arm, env={"PIP_INDEX_URL": other}).returncode == 0
    final = [b for b in arm.of("build") if flag_value(b, "--target") == "final"][0]
    assert flag_value(final, "--build-arg") == f"GBX_PIP_INDEX_FPR={fingerprint(other)}"
    assert fingerprint(other) != fingerprint(INDEX)
    arm.log.unlink()
    assert build(arm).returncode == 0
    assert not any("GBX_PIP_INDEX_FPR" in a for c in arm.of("build") for a in c)


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
    """The check fails closed: no history or config to read means nothing was verified."""
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
    res = start(arm)  # the default platform is arm64, whose image does not exist
    assert res.returncode == 1 and "left untouched" in res.output
    assert untouched(arm) and arm.of("run") == []
    arm.image(f"{TAG}-arm64", "arm64")
    res = start(arm)
    assert res.returncode == 0, res.output
    assert lifecycle(arm) == [
        "stop",
        "rm",
    ]  # with the image there, it recreates as before


def restart(sb, *args, env=None):
    return sb.run(COMMANDS / "gbx-docker-restart.sh", *args, env=env)


def test_restart_privileged_checks_the_image_before_removing_the_container(arm):
    arm.container("amd64")
    res = restart(arm, "--privileged")
    assert res.returncode == 1
    assert f"{TAG}-arm64" in res.output and "left untouched" in res.output
    assert untouched(arm) and arm.of("run") == []
    arm.image(f"{TAG}-arm64", "arm64")
    res = restart(arm, "--privileged")
    assert res.returncode == 0, res.output
    assert lifecycle(arm) == ["stop", "rm"] and arm.of("run")
