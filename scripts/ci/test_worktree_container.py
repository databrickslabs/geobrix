"""Tests for the per-checkout dev container (scripts/docker/gbx-container.sh and what sources it).

The main checkout keeps geobrix-dev on 5005/8888/4040. A linked git worktree names its own container,
ports and mounts in an untracked .gbx-local.env, or every container command refuses: one checkout
must never stop, recreate or remount another's container.

Run against the fake docker CLIs (_fake_docker_platform.py, _fake_docker_local.py) from a scratch copy
of the scripts, so the root is a plain directory (not this checkout, which may itself be a worktree), or
from a real `git worktree add` of one.
"""

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from conftest import REPO_ROOT
from test_docker_platform import TAG, Sandbox

COMMANDS = REPO_ROOT / "scripts" / "commands"
DOCKER_DIR = REPO_ROOT / "scripts" / "docker"
HOLD = ["/bin/bash", "/root/geobrix/scripts/docker/gbx-lock.sh", "hold"]


def local_env(tmp_path: Path, *lines: str) -> Path:
    path = tmp_path / "local.env"
    path.write_text("\n".join(lines) + "\n")
    return path


# --- the run arguments and every later call follow the checkout ------------------------------------

NO_FILE = None
STREAMLINE = [
    "GBX_DEV_CONTAINER=geobrix-dev-streamline",
    "GBX_DEV_PORT_JDWP=5006",
    "GBX_DEV_PORT_JUPYTER=8889",
    "GBX_DEV_PORT_SPARK_UI=4041",
    'GBX_DEV_EXTRA_MOUNTS="/data/bench:/Volumes/bench:ro"',
    "GBX_DEV_DOCKER_ENV=A=1",
    "GBX_DEV_IMAGE_TAG=ubuntu24-gdal311-spark",
]


@pytest.mark.parametrize(
    "script, lines, tail",
    [
        # with no env file the argv is exactly what the scripts always ran (the oracle)
        (
            "start_docker_with_volumes.sh",
            NO_FILE,
            "--name geobrix-dev -p 5005:5005 -p 8888:8888 -p 4040:4040"
            " -v {root}:/root/geobrix -v {root}/sample-data/Volumes:/Volumes -itd",
        ),
        (
            "start_docker.sh",
            NO_FILE,
            "--name geobrix-dev -p 5005:5005 -p 8888:8888 -p 4040:4040 -v {root}:/root/geobrix -itd",
        ),
        # a file renames the container, moves the ports, adds a mount and an environment variable
        (
            "start_docker_with_volumes.sh",
            STREAMLINE,
            "--name geobrix-dev-streamline -p 5006:5005 -p 8889:8888 -p 4041:4040"
            " -v {root}:/root/geobrix -v {root}/sample-data/Volumes:/Volumes"
            " -v /data/bench:/Volumes/bench:ro -e A=1 -itd",
        ),
        (
            "start_docker.sh",
            STREAMLINE,
            "--name geobrix-dev-streamline -p 5006:5005 -p 8889:8888 -p 4041:4040"
            " -v {root}:/root/geobrix -e A=1 -itd",
        ),
    ],
    ids=["volumes-default", "plain-default", "volumes-file", "plain-file"],
)
def test_the_run_arguments_follow_the_checkout(tmp_path, script, lines, tail):
    sb = Sandbox(tmp_path, "arm64")
    repo = sb.repo
    sb.image(f"{TAG}-arm64", "arm64")
    env_file = local_env(tmp_path, *lines) if lines else tmp_path / "no-such.env"

    res = sb.run(
        repo / "scripts" / "docker" / script,
        env={"GBX_LOCAL_ENV_FILE": str(env_file)},
        cwd=tmp_path,
    )

    assert res.returncode == 0, res.output
    want = ["run", "--platform", "linux/arm64", *tail.format(root=repo).split()]
    assert sb.of("run") == [want + [f"{TAG}-arm64", "/bin/bash"]]


def test_every_call_of_a_locked_exec_names_the_checkouts_container(sandbox):
    """The lock's hold exec, the work exec and the ps check all use the configured name."""
    env_file = local_env(
        sandbox.exec_log.parent, "GBX_DEV_CONTAINER=geobrix-dev-streamline"
    )

    res = sandbox.run(
        "gbx-docker-exec.sh",
        "mvn -v",
        GBX_LOCAL_ENV_FILE=str(env_file),
        FAKE_DOCKER_CONTAINER="geobrix-dev-streamline",
    )

    assert res.returncode == 0, res.output
    argvs = [
        json.loads(x)
        for x in Path(str(sandbox.exec_log) + ".argv").read_text().splitlines()
    ]
    execs = [a for a in argvs if a[0] == "exec"]
    assert len(execs) == 2 and all(
        "geobrix-dev-streamline" in a for a in execs
    )  # hold, then the work
    assert HOLD in sandbox.execs()
    assert not [a for a in argvs if "geobrix-dev" in a]


# --- a linked worktree must name its own container --------------------------------------------------


def git(cwd, *args):
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    return subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )


def load(root, env_file=None):
    """gbx_load_local_env <root> in a bash: (returncode, stderr, GBX_DEV_CONTAINER afterwards)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("GBX_")}
    if env_file:
        env["GBX_LOCAL_ENV_FILE"] = str(env_file)
    res = subprocess.run(
        [
            "bash",
            "-c",
            f'source "{DOCKER_DIR / "gbx-container.sh"}"; gbx_load_local_env "$1"; rc=$?;'
            ' echo "container=[$GBX_DEV_CONTAINER]"; exit $rc',
            "bash",
            str(root),
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    return res.returncode, res.stderr, res.stdout


def init_repo(tmp_path: Path) -> Path:
    main = tmp_path / "main"
    main.mkdir()
    git(main, "init", "-q")
    return main


def commit_all(repo: Path):
    git(repo, "add", "-A")
    git(
        repo,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@t",
        "commit",
        "-q",
        "--allow-empty",
        "-m",
        "x",
    )


@pytest.mark.parametrize(
    "content, container",
    [
        (None, None),  # no file at all
        ("", None),  # an empty file
        ("GBX_DEV_PORT_JDWP=5006\nGBX_CARGO_REGISTRY=x\n", None),  # names no container
        (
            "GBX_DEV_CONTAINER=geobrix-dev\n",
            None,
        ),  # names the main checkout's container
        (
            "GBX_DEV_CONTAINER=geobrix-dev-wt\nGBX_DEV_PORT_JDWP=5006\n",
            "geobrix-dev-wt",
        ),
    ],
    ids=["no-file", "empty", "no-name", "main-name", "own-name"],
)
def test_a_linked_worktree_must_name_a_container_of_its_own(
    tmp_path, content, container
):
    main = init_repo(tmp_path)
    commit_all(main)
    wt = tmp_path / "wt"
    git(main, "worktree", "add", "-q", str(wt))
    if content is not None:
        (wt / ".gbx-local.env").write_text(content)

    rc, err, out = load(wt)

    if container:
        assert (rc, err, out) == (0, "", f"container=[{container}]\n")
    else:
        assert rc == 1 and "GBX_DEV_CONTAINER" in err
        assert (
            "container=[]" in out
        )  # a command that forgot to require it acts on nothing


def test_the_main_checkout_needs_no_container_name(tmp_path):
    main = init_repo(tmp_path)
    commit_all(main)
    default = (0, "", "container=[geobrix-dev]\n")

    assert load(main) == default
    (main / ".gbx-local.env").write_text("GBX_DEV_PORT_JDWP=5006\n")
    assert load(main) == default


@pytest.mark.parametrize(
    "content, shown, secret",
    [
        ("SECRET_TOKEN=hunter2-value\n", "SECRET_TOKEN", "hunter2-value"),
        ("hunter2-value\n", "line 1", "hunter2-value"),
        ("GBX_DEV_PORT_JDWP=50x5\n", "GBX_DEV_PORT_", "50x5"),
        ("GBX_DEV_CONTAINER=bad name;x\n", "container name", "bad name"),
    ],
    ids=["unknown-key", "no-equals", "port", "name"],
)
def test_a_bad_local_env_file_is_refused_without_printing_its_values(
    tmp_path, content, shown, secret
):
    path = tmp_path / "bad.env"
    path.write_text(content)

    rc, err, out = load(tmp_path, path)

    assert rc == 1 and shown in err
    assert secret not in err + out
    assert "container=[]" in out


# --- no script names the main container ---------------------------------------------------------------


def container_scripts():
    for base in (COMMANDS, DOCKER_DIR):
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d != "m2"]
            for name in filenames:
                if name.endswith(".sh") and name != "gbx-container.sh":
                    yield Path(dirpath) / name


# An image reference is geobrix-dev:<tag>; geobrix-dev:/path is a docker cp into the container.
IMAGE_REFERENCE = re.compile(r"geobrix-dev:(?=[A-Za-z0-9<$])")


def test_no_script_hard_codes_the_container_name():
    """Only an image reference (geobrix-dev:<tag>) and the repository name may spell it."""
    offenders = []
    for path in container_scripts():
        for n, line in enumerate(path.read_text().splitlines(), 1):
            code = line.strip()
            if code.startswith("#"):
                continue
            code = IMAGE_REFERENCE.sub("", code).replace(
                'GBX_DEV_IMAGE_REPO="geobrix-dev"', ""
            )
            if "geobrix-dev" in code:
                offenders.append(
                    f"{path.relative_to(REPO_ROOT)}:{n}: {line.strip()[:80]}"
                )
    assert offenders == []


REQUIRES_LOCAL_ENV = re.compile(
    r"^\s*(?:(?:if|then|else|elif|do)\s+)?(?:!\s*)?(?:check_docker|gbx_require_local_env)\b(?![\w-])",
    re.M,
)


def test_every_command_that_uses_the_container_requires_a_usable_local_env():
    """common.sh loads the file without failing (host-only commands never need it); a command that
    reaches the container must CALL check_docker or gbx_require_local_env (a comment or a help line
    naming it does not count), or a refused load goes unnoticed."""

    def calls_it(text):
        code = "\n".join(
            line for line in text.splitlines() if not line.lstrip().startswith("#")
        )
        return REQUIRES_LOCAL_ENV.search(code)

    missing = [
        p.name
        for p in sorted(COMMANDS.glob("gbx-*.sh"))
        if "$GBX_DEV_CONTAINER" in p.read_text() and not calls_it(p.read_text())
    ]
    assert missing == []


# --- a linked worktree only touches a container that mounts it ---------------------------------------


def linked_checkout(sb: Sandbox, tmp_path: Path, container: str, mount) -> Path:
    """A real linked worktree of a git repo holding the scripts, with a .gbx-local.env naming `container`,
    and the fake docker holding that container with /root/geobrix mounted from `mount` (default: the
    worktree itself)."""
    main = init_repo(tmp_path)
    shutil.copytree(sb.repo / "scripts", main / "scripts")
    shutil.copytree(sb.repo / "python", main / "python")
    commit_all(main)
    wt = tmp_path / "wt"
    git(main, "worktree", "add", "-q", str(wt))
    (wt / ".gbx-local.env").write_text(f"GBX_DEV_CONTAINER={container}\n")
    sb.image(f"{TAG}-arm64", "arm64")
    sb.image(f"{TAG}-amd64", "amd64")
    sb.container("arm64", mount=str(mount or wt), name=container)
    return wt


@pytest.mark.parametrize(
    "script, args",
    [
        ("gbx-docker-exec.sh", ["echo hi"]),
        ("gbx-docker-start.sh", []),
        ("gbx-docker-start.sh", ["--recreate"]),
        ("gbx-docker-restart.sh", ["--privileged"]),
        ("gbx-docker-rebuild.sh", ["--no-pull"]),
    ],
    ids=["exec", "start", "start-recreate", "restart-privileged", "rebuild"],
)
def test_a_linked_worktree_neither_uses_nor_removes_a_container_mounted_elsewhere(
    tmp_path, script, args
):
    sb = Sandbox(tmp_path, "arm64")
    wt = linked_checkout(sb, tmp_path, "gbx-dev-wt", mount="/some/other/checkout")

    res = sb.run(wt / "scripts" / "commands" / script, *args)

    assert res.returncode == 1, res.output
    assert "/root/geobrix" in res.output and "/some/other/checkout" in res.output
    touched = ("exec", "stop", "rm", "run", "start", "restart", "build", "tag")
    assert [c[0] for c in sb.calls if c[0] in touched] == []
    assert sb.final_state()["container"]["running"] is True


def test_a_linked_worktree_uses_the_container_that_mounts_it(tmp_path):
    sb = Sandbox(tmp_path, "arm64")
    wt = linked_checkout(sb, tmp_path, "gbx-dev-wt", mount=None)

    res = sb.run(wt / "scripts" / "commands" / "gbx-docker-exec.sh", "echo hi")

    assert res.returncode == 0, res.output
    assert [c[1] for c in sb.of("exec")] == ["gbx-dev-wt"]


# --- a container whose architecture cannot be used is left alone ---------------------------------------


@pytest.mark.parametrize(
    "script, args, arch, gone",
    [
        (
            "gbx-docker-start.sh",
            [],
            "amd64",
            True,
        ),  # worktree mismatch, unreadable architecture
        (
            "gbx-docker-start.sh",
            [],
            "386",
            False,
        ),  # worktree mismatch, unsupported architecture
        ("gbx-docker-restart.sh", ["--privileged"], "386", False),
    ],
    ids=["start-unreadable", "start-386", "restart-386"],
)
def test_an_unusable_container_architecture_is_refused(
    tmp_path, script, args, arch, gone
):
    sb = Sandbox(tmp_path, "arm64")
    sb.image(f"{TAG}-arm64", "arm64")
    sb.image(f"{TAG}-amd64", "amd64")
    sb.container(arch, mount="/some/other/worktree")
    sb.state["container_image_gone"] = gone

    res = sb.run(sb.commands / script, *args)

    assert res.returncode == 1, res.output
    assert (
        "Cannot tell which architecture" in res.output
        and "left untouched" in res.output
    )
    assert [c[0] for c in sb.calls if c[0] in ("stop", "rm", "run", "start")] == []
    assert sb.final_state()["container"]["running"] is True
