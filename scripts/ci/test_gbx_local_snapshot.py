"""Tests for gbx:local:snapshot (scripts/snapshot/snapshot.py and its command).

Everything runs on scratch git repositories under tmp_path: no network, no real home folder
(GBX_SNAPSHOT_DIR always points into tmp_path), and no real gitleaks (a stub stands in).
"""

import importlib.util
import os
import shutil
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
from conftest import REPO_ROOT

SNAP_PY = REPO_ROOT / "scripts" / "snapshot" / "snapshot.py"
spec = importlib.util.spec_from_file_location("gbx_snapshot", SNAP_PY)
snap = importlib.util.module_from_spec(spec)
sys.modules["gbx_snapshot"] = snap
spec.loader.exec_module(snap)

GIT_ENV = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@example.com",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@example.com",
}


# Keep the developer's own git settings (a global core.hooksPath, for one) out of the scratch repos.
GIT_ISOLATION = {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}


def sh(cwd, *cmd, **kw):
    env = dict(os.environ, **GIT_ENV, **GIT_ISOLATION)
    env.update(kw.pop("env", {}))
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        check=kw.pop("check", True),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        **kw,
    )


def write(root, rel, text="x", mode=None):
    p = Path(root) / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    if mode:
        p.chmod(mode)
    return p


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A repo with a bare 'remote' that already has its first commit, and a snapshot folder."""
    remote = tmp_path / "remote.git"
    sh(tmp_path, "git", "init", "-q", "--bare", str(remote))
    root = tmp_path / "work"
    root.mkdir()
    sh(root, "git", "init", "-q", "-b", "main")
    sh(root, "git", "remote", "add", "origin", str(remote))
    write(root, "README.md", "hi")
    write(root, ".gitignore", ".superpowers/\n")
    sh(root, "git", "add", "-A")
    sh(root, "git", "commit", "-q", "-m", "first")
    sh(root, "git", "push", "-q", "origin", "main")
    dest = tmp_path / "snaps"
    for key in snap.CONFIG_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("GBX_SNAPSHOT_DIR", str(dest))
    monkeypatch.setenv("GBX_SNAPSHOT_GITLEAKS", str(tmp_path / "no-such-gitleaks"))
    for k, v in dict(GIT_ENV, **GIT_ISOLATION).items():
        monkeypatch.setenv(k, v)
    return SimpleRepo(root, dest, monkeypatch)


class SimpleRepo:
    def __init__(self, root, dest, monkeypatch):
        self.root, self.dest, self.mp = root, dest, monkeypatch

    def run(self, *args):
        return snap.main(["--root", str(self.root)] + list(args))

    def newest_zip(self):
        return snap.snapshots(self.dest)[0]

    def names(self):
        with zipfile.ZipFile(str(self.newest_zip())) as zf:
            return set(zf.namelist())


# --- include / exclude -----------------------------------------------------------------------------
def test_working_tree_keeps_gitignored_local_files_and_drops_run_state(repo):
    r = repo.root
    write(r, ".superpowers/specs/design.md")  # gitignored, but only on this machine
    write(r, ".gbx-local.env", "GBX_SNAPSHOT_KEEP=2\n")
    write(r, ".claude/settings.local.json", "{}")
    write(r, "sample-data/big.tif")
    write(r, "build/out.txt")
    write(r, "target/classes/A.class")
    write(r, "scripts/docker/m2/repo/x.jar")
    write(r, "python/geobrix/lib/geobrix-0.6.0-jar-with-dependencies.jar")
    write(r, "python/geobrix/lib/README.md")
    write(r, "docs/node_modules/pkg/index.js")
    write(r, "src/__pycache__/m.cpython-312.pyc")
    write(r, "test-logs/run.log")
    write(r, "a/b.pyc")
    write(r, ".DS_Store")
    plan = snap.collect(r, 500 * 1024 * 1024)
    kept = {rel for _, rel, _ in plan.files}
    assert {
        ".superpowers/specs/design.md",
        ".gbx-local.env",
        ".claude/settings.local.json",
        "README.md",
        "python/geobrix/lib/README.md",
    } <= kept
    for gone in (
        "sample-data/big.tif",
        "build/out.txt",
        "target/classes/A.class",
        "scripts/docker/m2/repo/x.jar",
        "python/geobrix/lib/geobrix-0.6.0-jar-with-dependencies.jar",
        "docs/node_modules/pkg/index.js",
        "src/__pycache__/m.cpython-312.pyc",
        "test-logs/run.log",
        "a/b.pyc",
        ".DS_Store",
    ):
        assert gone not in kept
    assert not any(rel.startswith(".git/") for rel in kept)


def test_real_cluster_config_and_scratch_input_are_left_out(repo):
    r = repo.root
    write(r, "examples/tests/databricks_cluster_config.env", "A=1")
    write(r, "examples/tests/databricks_cluster_config.example.env", "A=")
    write(r, ".superpowers/input/draft.md")
    write(r, ".superpowers/input/deep/more.txt")
    write(r, ".superpowers/specs/design.md")
    kept = {rel for _, rel, _ in snap.collect(r, 10**9).files}
    assert "examples/tests/databricks_cluster_config.env" not in kept
    assert not [k for k in kept if k.startswith(".superpowers/input")]
    assert {
        "examples/tests/databricks_cluster_config.example.env",
        ".superpowers/specs/design.md",
    } <= kept


def test_nested_virtual_environments_and_node_modules_are_left_out(repo):
    r = repo.root
    write(r, ".superpowers/zarr-spike/.venv/lib/site.py")
    write(r, ".superpowers/zarr-spike/.venv/pyvenv.cfg", "home = /x")
    write(r, ".superpowers/notes/venv-by-another-name/pyvenv.cfg", "home = /x")
    write(r, ".superpowers/notes/venv-by-another-name/bin/python")
    write(r, ".superpowers/a/b/c/node_modules/p/index.js")
    write(r, ".superpowers/zarr-spike/run.py")
    write(r, ".venv-pyrx/bin/python")
    write(r, "connectors/lakeflow/x.egg-info/PKG-INFO")
    kept = {rel for _, rel, _ in snap.collect(r, 10**9).files}
    assert ".superpowers/zarr-spike/run.py" in kept
    assert not [
        k for k in kept if "venv" in k or "node_modules" in k or "egg-info" in k
    ]


def test_files_over_the_size_cap_are_skipped_and_named(repo, capsys):
    write(repo.root, "big.bin", "x" * 3 * 1024 * 1024)
    write(repo.root, "small.txt")
    plan = snap.collect(repo.root, 2 * 1024 * 1024)
    assert [rel for rel, _ in plan.skipped_large] == ["big.bin"]
    assert "small.txt" in {rel for _, rel, _ in plan.files}


def test_symlinks_are_stored_as_links_not_followed(repo):
    r = repo.root
    (r / "outside").mkdir()
    write(r, "outside/secret.txt", "s")
    os.symlink(str(r / "outside"), str(r / "linkdir"))
    os.symlink("README.md", str(r / "linkfile"))
    plan = snap.collect(r, 10**9)
    kept = {rel for _, rel, _ in plan.files}
    assert {"linkdir", "linkfile"} <= kept
    assert "linkdir/secret.txt" not in kept


# --- run, bundle, restore --------------------------------------------------------------------------
def test_run_writes_a_named_zip_with_tree_git_settings_and_no_bundle_when_pushed(repo):
    write(repo.root, ".superpowers/plan.md", "plan")
    assert repo.run("run") == 0
    z = repo.newest_zip()
    assert snap.SNAPSHOT_RE.match(z.name) and "-main-" in z.name
    names = repo.names()
    assert "worktree/.superpowers/plan.md" in names
    assert "git-meta/config" in names and "SNAPSHOT-INFO.txt" in names
    assert "git-meta/unpushed.bundle" not in names
    assert not [n for n in names if ".git/objects" in n]
    assert [p.name for p in repo.dest.iterdir() if p.name.startswith(".")] == [
        ".snapshot.lock"
    ]


def test_unpushed_commits_and_the_stash_are_bundled(repo):
    r = repo.root
    write(r, "new.txt", "n")
    sh(r, "git", "add", "-A")
    sh(r, "git", "commit", "-q", "-m", "local only")
    write(r, "README.md", "changed")
    sh(r, "git", "stash", "-q")
    assert repo.run("run") == 0
    assert "git-meta/unpushed.bundle" in repo.names()
    out = repo.dest.parent / "restored"
    assert repo.run("restore", repo.newest_zip().name, str(out)) == 0
    bundle = out / "git-meta" / "unpushed.bundle"
    heads = sh(r, "git", "bundle", "list-heads", str(bundle)).stdout
    assert "refs/heads/main" in heads and "refs/stash" in heads
    assert (out / "worktree" / "new.txt").read_text() == "n"


def test_pushed_work_alone_makes_no_bundle(repo):
    assert snap.bundle_args(repo.root) is None
    write(repo.root, "x.txt")
    sh(repo.root, "git", "add", "-A")
    sh(repo.root, "git", "commit", "-q", "-m", "more")
    assert snap.bundle_args(repo.root) is not None


def test_restore_keeps_symlinks_and_the_executable_bit_and_refuses_a_full_dir(
    repo, tmp_path
):
    write(repo.root, "tool.sh", "#!/bin/sh\n", mode=0o755)
    os.symlink("README.md", str(repo.root / "link"))
    assert repo.run("run") == 0
    out = tmp_path / "out"
    assert repo.run("restore", str(repo.newest_zip()), str(out)) == 0
    assert os.readlink(str(out / "worktree" / "link")) == "README.md"
    assert (out / "worktree" / "tool.sh").stat().st_mode & stat.S_IXUSR
    assert repo.run("restore", str(repo.newest_zip()), str(out)) == 2  # not empty now


def test_a_file_older_than_1980_is_stored(repo):
    old = write(repo.root, "ancient.txt", "old")
    os.utime(str(old), (0, 0))
    assert repo.run("run") == 0
    assert "worktree/ancient.txt" in repo.names()


def test_dry_run_writes_nothing(repo, capsys):
    write(repo.root, "sample-data/x.tif")
    assert repo.run("run", "--dry-run") == 0
    out = capsys.readouterr().out
    assert "sample-data" in out and "estimated zip" in out
    assert not repo.dest.exists() or snap.snapshots(repo.dest) == []


def test_a_second_run_while_one_holds_the_lock_exits_busy(repo):
    import fcntl

    repo.dest.mkdir()
    with open(str(repo.dest / ".snapshot.lock"), "w") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        assert repo.run("run") == snap.EXIT_BUSY
    assert snap.snapshots(repo.dest) == []


# --- retention -------------------------------------------------------------------------------------
def test_prune_keeps_the_newest_n_and_ignores_other_files(tmp_path):
    names = [
        "geobrix-snapshot-2026110%dT120000Z-main-abc1234.zip" % d for d in range(1, 7)
    ]
    for n in names:
        (tmp_path / n).write_text("z")
    keepers = [
        "notes.zip",
        "geobrix-snapshot-notes.zip",
        "geobrix-snapshot-20261101T120000Z-main.zip",
    ]
    for n in keepers:
        (tmp_path / n).write_text("k")
    removed = snap.prune(tmp_path, 3)
    assert sorted(p.name for p in removed) == names[:3]
    left = sorted(p.name for p in tmp_path.iterdir())
    assert left == sorted(names[3:] + keepers)


def test_run_prunes_after_a_successful_write(repo):
    repo.dest.mkdir()
    old = [
        "geobrix-snapshot-2026010%dT000000Z-main-abc1234.zip" % d for d in range(1, 5)
    ]
    for n in old:
        (repo.dest / n).write_text("z")
    (repo.dest / "mine.txt").write_text("keep me")
    (repo.root / ".gbx-local.env").write_text("GBX_SNAPSHOT_KEEP=2\n")
    assert repo.run("run") == 0
    left = [p.name for p in snap.snapshots(repo.dest)]
    assert len(left) == 2 and left[1] == old[3]
    assert (repo.dest / "mine.txt").exists()


def test_settings_come_from_the_env_file_and_the_environment_wins(tmp_path):
    (tmp_path / ".gbx-local.env").write_text(
        '# c\nGBX_SNAPSHOT_DIR="~/somewhere"\nGBX_SNAPSHOT_KEEP=5\nGBX_SNAPSHOT_MAX_FILE_MB=7\nGBX_DEV_CONTAINER=x\n'
    )
    cfg = snap.Config(tmp_path, env={})
    assert (cfg.keep, cfg.max_file_mb) == (5, 7)
    assert cfg.dest == Path(os.path.expanduser("~/somewhere"))
    cfg = snap.Config(tmp_path, env={"GBX_SNAPSHOT_KEEP": "9"})
    assert cfg.keep == 9
    assert snap.Config(tmp_path / "none", env={}).dest.name == "GeoBrix-Snapshots"
    with pytest.raises(SystemExit):
        snap.Config(tmp_path, env={"GBX_SNAPSHOT_KEEP": "0"})


# --- secret scan -----------------------------------------------------------------------------------
def fake(prefix, body):  # built here so this file holds no token-shaped text
    return prefix + body


@pytest.mark.parametrize(
    "text, rule",
    [
        (fake("ghp_", "a1B2" * 9), "GitHub token"),
        (fake("-----BEGIN RSA PRIV", "ATE KEY-----"), "private key"),
        (fake("AKIA", "ABCDEFGHIJKLMNOP"), "AWS access key id"),
        (fake("dapi", "0123456789abcdef" * 2), "Databricks token"),
    ],
)
def test_a_secret_refuses_the_run_and_the_output_never_shows_it(
    repo, capsys, text, rule
):
    write(repo.root, ".superpowers/notes.md", "ok\nline two\n" + text + "\n")
    assert repo.run("run") == snap.EXIT_REFUSED
    out = capsys.readouterr().out
    assert ".superpowers/notes.md:3" in out and rule in out
    assert text not in out
    assert snap.snapshots(repo.dest) == []


def test_secrets_in_the_real_cluster_config_and_scratch_input_are_not_scanned(repo):
    token = fake("dapi", "0123456789abcdef" * 2)
    write(repo.root, "examples/tests/databricks_cluster_config.env", "T=" + token)
    write(repo.root, ".superpowers/input/paste.md", token)
    assert repo.run("run") == 0
    assert not [n for n in repo.names() if "cluster_config.env" in n or "input/" in n]


def test_a_remote_url_with_a_password_in_git_settings_refuses(repo, capsys):
    sh(
        repo.root,
        "git",
        "remote",
        "add",
        "leaky",
        "https://user:" + "hunter2hunter2@example.com/x.git",
    )
    assert repo.run("run") == snap.EXIT_REFUSED
    assert "git-meta/config" in capsys.readouterr().out


def test_documented_example_keys_and_binary_files_do_not_trip_the_scan(repo):
    write(repo.root, "doc.md", fake("AKIA", "IOSFODNN7" + "EXAMPLE"))
    (repo.root / "blob.bin").write_bytes(b"\0\0" + fake("ghp_", "a1B2" * 9).encode())
    assert repo.run("run") == 0


def test_gitleaks_findings_refuse_too_and_are_reported_as_file_and_line(
    repo, capsys, tmp_path
):
    write(repo.root, "conf/app.yml", "a: 1\nb: 2\n")
    stub = tmp_path / "gitleaks"
    stub.write_text(
        "#!/bin/sh\n"
        "while [ $# -gt 0 ]; do case $1 in --report-path) out=$2;; --source) src=$2;; esac; shift; done\n"
        'echo "[{\\"File\\":\\"$src/conf/app.yml\\",\\"StartLine\\":2,\\"RuleID\\":\\"jwt\\"}]" > "$out"\n'
        "exit 1\n"
    )
    stub.chmod(0o755)
    repo.mp.setenv("GBX_SNAPSHOT_GITLEAKS", str(stub))
    assert repo.run("run") == snap.EXIT_REFUSED
    assert "conf/app.yml:2  gitleaks:jwt" in capsys.readouterr().out


def test_gitleaks_noise_and_sample_keys_do_not_count(repo, tmp_path):
    write(repo.root, "notes.md", "a\nb\n")
    stub = tmp_path / "gitleaks"
    stub.write_text(
        "#!/bin/sh\n"
        "while [ $# -gt 0 ]; do case $1 in --report-path) out=$2;; --source) src=$2;; esac; shift; done\n"
        'echo "[{\\"File\\":\\"$src/notes.md\\",\\"StartLine\\":1,\\"RuleID\\":\\"generic-api-key\\",\\"Secret\\":\\"abc\\"},'
        '{\\"File\\":\\"$src/notes.md\\",\\"StartLine\\":2,\\"RuleID\\":\\"aws-access-token\\",\\"Secret\\":\\"AKIA' 'IOSFODNN7' 'EXAMPLE\\"}]" > "$out"\n'
        "exit 1\n"
    )
    stub.chmod(0o755)
    repo.mp.setenv("GBX_SNAPSHOT_GITLEAKS", str(stub))
    assert repo.run("run") == 0


def test_reviewed_paths_can_be_allowed_and_nothing_else(repo, capsys):
    write(
        repo.root,
        "examples/tests/cluster.env",
        "#" + fake("dapi", "0123456789abcdef" * 2) + "\n",
    )
    assert repo.run("run") == snap.EXIT_REFUSED
    repo.mp.setenv("GBX_SNAPSHOT_SCAN_ALLOW", "examples/tests/*.env")
    assert repo.run("run") == 0
    assert "skipped findings in examples/tests/cluster.env" in capsys.readouterr().out
    write(repo.root, "other.md", fake("ghp_", "a1B2" * 9))
    assert repo.run("run") == snap.EXIT_REFUSED


def test_a_gitleaks_that_cannot_run_is_noted_and_the_built_in_scan_still_decides(
    repo, capsys, tmp_path
):
    stub = tmp_path / "gitleaks"
    stub.write_text("#!/bin/sh\nexit 3\n")
    stub.chmod(0o755)
    repo.mp.setenv("GBX_SNAPSHOT_GITLEAKS", str(stub))
    assert repo.run("run") == 0
    assert "gitleaks exited 3" in capsys.readouterr().out


# --- hook ------------------------------------------------------------------------------------------
LFS_HOOK = (
    "#!/bin/sh\n"
    "command -v git-lfs >/dev/null 2>&1 || { exit 2; }\n"
    'git lfs pre-push "$@"\n'
)


@pytest.fixture
def hook_repo(repo, tmp_path):
    """The repo with an LFS-style pre-push hook, a stub snapshot command, and a stub git-lfs."""
    hook = repo.root / ".git" / "hooks" / "pre-push"
    hook.write_text(LFS_HOOK)
    hook.chmod(0o755)
    cmd = write(
        repo.root,
        "scripts/commands/gbx-local-snapshot.sh",
        '#!/bin/sh\necho "$@" > "$(dirname "$0")/ran.txt"\n',
    )
    cmd.chmod(0o755)
    bindir = tmp_path / "bin"
    bindir.mkdir()
    lfs = bindir / "git-lfs"
    lfs.write_text('#!/bin/sh\nexit "${FAKE_LFS_RC:-0}"\n')
    lfs.chmod(0o755)
    repo.hook, repo.bindir, repo.ran = hook, bindir, cmd.parent / "ran.txt"
    return repo


def run_hook(r, lfs_rc):
    env = {"PATH": "%s:%s" % (r.bindir, os.environ["PATH"]), "FAKE_LFS_RC": str(lfs_rc)}
    res = sh(r.root, "sh", str(r.hook), "origin", "url", check=False, env=env, input="")
    deadline = 50
    import time

    while deadline and not r.ran.exists() and lfs_rc == 0:  # the snapshot is detached
        time.sleep(0.1)
        deadline -= 1
    return res.returncode


def test_install_is_idempotent_and_uninstall_removes_only_the_block(hook_repo):
    r = hook_repo
    before = r.hook.read_text()
    assert r.run("install-hook") == 0
    once = r.hook.read_text()
    assert r.run("install-hook") == 0
    assert r.hook.read_text() == once
    assert once.startswith(before) and once.count(snap.HOOK_BEGIN) == 1
    assert os.access(str(r.hook), os.X_OK)
    assert r.run("uninstall-hook") == 0
    assert r.hook.read_text() == before
    assert r.run("uninstall-hook") == 0
    assert r.hook.read_text() == before


def test_hooks_outside_the_repository_are_neither_saved_nor_edited(repo, tmp_path):
    shared = tmp_path / "shared-hooks"
    shared.mkdir()
    (shared / "pre-push").write_text("#!/bin/sh\n")
    sh(repo.root, "git", "config", "core.hooksPath", str(shared))
    assert repo.run("install-hook") == 2
    assert (shared / "pre-push").read_text() == "#!/bin/sh\n"
    assert not [n for _, n in snap.git_meta_files(repo.root) if n.startswith("hooks/")]


def test_install_creates_a_hook_when_there_is_none(repo):
    hook = repo.root / ".git" / "hooks" / "pre-push"
    assert not hook.exists()
    assert repo.run("install-hook") == 0
    assert hook.read_text().startswith("#!/bin/sh\n") and os.access(str(hook), os.X_OK)


@pytest.mark.parametrize("lfs_rc", [0, 2, 1])
def test_the_hook_keeps_the_lfs_exit_status(hook_repo, lfs_rc):
    hook_repo.run("install-hook")
    assert run_hook(hook_repo, lfs_rc) == lfs_rc


def test_the_hook_starts_the_snapshot_only_after_a_good_push_and_never_fails_it(
    hook_repo,
):
    hook_repo.run("install-hook")
    assert run_hook(hook_repo, 2) == 2
    assert not hook_repo.ran.exists()
    assert run_hook(hook_repo, 0) == 0
    assert hook_repo.ran.read_text().split()[:2] == ["run", "--log"]
    # a snapshot command that fails or is missing does not change the push result
    hook_repo.ran.unlink()
    (hook_repo.root / "scripts/commands/gbx-local-snapshot.sh").unlink()
    assert run_hook(hook_repo, 0) == 0


def test_status_reports_the_last_logged_run_and_the_hook(hook_repo, capsys):
    r = hook_repo
    log = write(
        r.root,
        "test-logs/snapshot/20261108T010101Z.log",
        "wrote x\nRESULT ok x.zip 1.0 MB 3s\n",
    )
    r.run("install-hook")
    assert r.run("status") == 0
    out = capsys.readouterr().out
    assert (
        "RESULT ok x.zip" in out
        and "pre-push hook: installed" in out
        and log.name in out
    )


# --- the command -----------------------------------------------------------------------------------
@pytest.fixture
def command_repo(repo):
    """The repo with copies of the command, common.sh, and the container-settings loader."""
    for rel in (
        "scripts/commands/common.sh",
        "scripts/commands/gbx-local-snapshot.sh",
        "scripts/snapshot/snapshot.py",
        "scripts/snapshot/drive_upload.py",
    ):
        dst = repo.root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(str(REPO_ROOT / rel), str(dst))
    shutil.copytree(
        str(REPO_ROOT / "scripts" / "docker"),
        str(repo.root / "scripts" / "docker"),
        ignore=shutil.ignore_patterns("m2", "dcs", "extras"),
    )
    return repo


def run_cmd(r, *args, env=None):
    return sh(
        r.root,
        "bash",
        "scripts/commands/gbx-local-snapshot.sh",
        *args,
        check=False,
        env=dict(env or {}, GBX_SNAPSHOT_DIR=str(r.dest)),
    )


def test_the_command_accepts_the_snapshot_settings_in_the_local_env_file(command_repo):
    r = command_repo
    (r.root / ".gbx-local.env").write_text(
        "GBX_SNAPSHOT_KEEP=2\nGBX_SNAPSHOT_MAX_FILE_MB=5\n"
    )
    res = run_cmd(r, "--dry-run")
    assert res.returncode == 0, res.stdout + res.stderr
    assert "keep newest 2" in res.stdout and "unknown key" not in res.stderr


def test_the_command_runs_end_to_end_and_logs(command_repo):
    r = command_repo
    res = run_cmd(r, "run", "--log", "snapshot/t.log")
    assert res.returncode == 0, res.stdout + res.stderr
    assert snap.snapshots(r.dest)
    assert "RESULT ok" in (r.root / "test-logs" / "snapshot" / "t.log").read_text()
    assert "RESULT ok" in run_cmd(r, "status").stdout
    assert run_cmd(r, "list").stdout.strip().endswith("B")
    assert run_cmd(r, "--help").returncode == 0


def test_the_command_carries_a_md_and_the_example_env_lists_the_keys():
    md = (REPO_ROOT / "scripts/commands/gbx-local-snapshot.md").read_text()
    example = (REPO_ROOT / "scripts/docker/gbx-local.env.example").read_text()
    keys = (REPO_ROOT / "scripts/docker/gbx-container.sh").read_text()
    for key in snap.CONFIG_KEYS:
        assert key in md and key in example and key in keys


# --- cluster config: the derived non-secret copy and the rebuild on restore -------------------------
REAL_CFG = "examples/tests/databricks_cluster_config.env"
DERIVED_CFG = "examples/tests/databricks_cluster_config.nonsecret.env"
EXAMPLE_CFG = "examples/tests/databricks_cluster_config.example.env"
CFG_SECRET = fake("dapi", "0123456789abcdef" * 2)
CFG_JWT = (
    fake("eyJ", "hbGciOiJIUzI1NiJ9") + ".eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcDEF123456_-xyz"
)
REAL_CFG_TEXT = (
    "# docs: DATABRICKS_TOKEN=dapi...\n"
    "#DATABRICKS_TOKEN=" + CFG_SECRET + "\n"
    "# note " + CFG_JWT + "\n"
    "DATABRICKS_TOKEN=" + CFG_SECRET + "\n"
    "MY_CLIENT_SECRET=hunter2\n"
    "DATABRICKS_CONFIG_PROFILE=oauth-fe\n"
    "CLUSTER_ID=0101-abcdef-xyz\n"
    "GBX_UC_WAREHOUSE_ID=abc123def456\n"
)
EXAMPLE_CFG_TEXT = (
    "# example\nDATABRICKS_CONFIG_PROFILE=default\nCLUSTER_ID=\n"
    "GBX_BUNDLE_VOLUME_CATALOG=main\n# GBX_UC_WAREHOUSE_ID=\n"
)


def test_derived_config_drops_secrets_and_secret_shaped_comments_and_keeps_workspace_values():
    body, names, anonymous = snap.derive_nonsecret_config(REAL_CFG_TEXT)
    assert names == ["DATABRICKS_TOKEN", "MY_CLIENT_SECRET"] and anonymous == 1
    for gone in (CFG_SECRET, CFG_JWT, "hunter2"):
        assert gone not in body
    for kept in (
        "DATABRICKS_CONFIG_PROFILE=oauth-fe",
        "CLUSTER_ID=0101-abcdef-xyz",
        "GBX_UC_WAREHOUSE_ID=abc123def456",
        "# docs: DATABRICKS_TOKEN=dapi...",  # a placeholder, not a secret
        "# Secret keys dropped: DATABRICKS_TOKEN, MY_CLIENT_SECRET",
    ):
        assert kept in body
    assert not snap._secret_shaped(body)


def test_run_includes_the_derived_config_leaves_the_real_one_out_and_the_scan_passes(
    repo,
):
    write(repo.root, ".gitignore", ".superpowers/\n/" + REAL_CFG + "\n")
    write(repo.root, REAL_CFG, REAL_CFG_TEXT)
    assert repo.run("run") == 0
    names = repo.names()
    assert "worktree/" + DERIVED_CFG in names and "worktree/" + REAL_CFG not in names
    with zipfile.ZipFile(str(repo.newest_zip())) as zf:
        text = zf.read("worktree/" + DERIVED_CFG).decode()
    assert CFG_SECRET not in text and "CLUSTER_ID=0101-abcdef-xyz" in text


def _snapshot_with_config(repo):
    write(repo.root, EXAMPLE_CFG, EXAMPLE_CFG_TEXT)
    write(repo.root, REAL_CFG, REAL_CFG_TEXT)
    assert repo.run("run") == 0
    return repo.dest.parent / "restored"


def test_restore_rebuilds_the_config_from_example_defaults_and_snapshot_values(
    repo, capsys
):
    out = _snapshot_with_config(repo)
    assert repo.run("restore", str(repo.newest_zip()), str(out)) == 0
    said = capsys.readouterr().out
    rebuilt = (out / "worktree" / REAL_CFG).read_text()
    assert "DATABRICKS_CONFIG_PROFILE=oauth-fe" in rebuilt  # the snapshot's value wins
    assert "CLUSTER_ID=0101-abcdef-xyz" in rebuilt
    assert "GBX_BUNDLE_VOLUME_CATALOG=main" in rebuilt  # the example's default stays
    assert (
        "GBX_UC_WAREHOUSE_ID=abc123def456" in rebuilt
    )  # appended: only a comment in the example
    assert CFG_SECRET not in rebuilt and "hunter2" not in rebuilt
    assert "DATABRICKS_TOKEN, MY_CLIENT_SECRET" in said and "~/.databrickscfg" in said
    assert CFG_SECRET not in said


def test_restore_never_overwrites_an_existing_real_config(repo, tmp_path):
    _snapshot_with_config(repo)
    tree = tmp_path / "tree"
    write(tree, DERIVED_CFG, "CLUSTER_ID=from-snapshot\n")
    write(tree, REAL_CFG, "CLUSTER_ID=mine\n")
    snap.restore_cluster_config(tree)
    assert (tree / REAL_CFG).read_text() == "CLUSTER_ID=mine\n"


# --- Drive upload and prune (a fake Drive: no network, no gcloud) ------------------------------------
import hashlib  # noqa: E402
import json  # noqa: E402
import re  # noqa: E402
import urllib.parse  # noqa: E402

drive = snap.drive_module()
FOLDER = "fakeFolderId0001"
PRIVATE_PERM = {"type": "user", "role": "owner", "emailAddress": "me@example.com"}
TEAM_PERMS = (
    {"type": "domain", "role": "reader", "domain": "example.com"},
    {"type": "user", "role": "writer", "emailAddress": "colleague@example.com"},
)
ZIP_NAME = "geobrix-snapshot-20261108T120000Z-main-abc1234.zip"


@pytest.fixture(autouse=True)
def _no_network_no_gcloud(monkeypatch):
    """Every test here must use the fake: a real request or a gcloud call fails the test."""

    def boom(*args, **kwargs):
        raise AssertionError("a test reached the network or gcloud")

    monkeypatch.setattr(drive.urllib.request, "build_opener", boom)
    monkeypatch.setattr(drive.GcloudToken, "get", boom)


class FakeToken:
    """Hands out its tokens in order; a refresh moves to the next one."""

    def __init__(self, *tokens):
        self.tokens, self.i, self.refreshes = list(tokens) or ["tok-new"], 0, 0

    def get(self, refresh=False):
        if refresh:
            self.refreshes += 1
            self.i = min(self.i + 1, len(self.tokens) - 1)
        return self.tokens[self.i]


class FakeDrive:
    """An in-memory Drive v3 for the calls drive_upload makes. Used as the client's transport."""

    def __init__(self):
        self.files, self.sessions, self.calls, self.seq = {}, {}, [], 0
        self.valid = {"tok-new"}
        self.fail_chunks = set()  # 0-based chunk PUTs that answer 503 and store nothing
        self.corrupt_md5 = (
            False  # Drive reports a checksum that is not the bytes' checksum
        )
        self.list_status = 200
        self.chunk_puts = 0
        self.perms = [
            PRIVATE_PERM
        ]  # permissions.list on the folder: private unless a test shares it
        self.perms_status = 200
        self.folder_shared = False
        self.files[FOLDER] = self._record(
            FOLDER, "Snapshots", b"", True, drive.FOLDER_MIME, []
        )

    @staticmethod
    def _record(fid, name, data, owned, mime, parents, created="2026-01-01T00:00:00Z"):
        return {
            "id": fid,
            "name": name,
            "data": data,
            "owned": owned,
            "mimeType": mime,
            "parents": parents,
            "created": created,
            "trashed": False,
        }

    def add(
        self,
        name,
        data=b"zip",
        owned=True,
        created="2026-01-01T00:00:00Z",
        mime="application/zip",
        parents=None,
    ):
        self.seq += 1
        fid = "fakeFile%06d" % self.seq
        self.files[fid] = self._record(
            fid, name, data, owned, mime, parents or [FOLDER], created
        )
        return fid

    def share(self, *extra):
        """Open the folder to others (the owner stays in the list)."""
        self.perms = [PRIVATE_PERM] + list(extra or TEAM_PERMS)
        self.folder_shared = True

    def live(self):
        return {
            f["name"]
            for f in self.files.values()
            if f["id"] != FOLDER and not f["trashed"]
        }

    def methods(self, *names):
        return [c for c in self.calls if c[0] in names]

    def _meta(self, f):
        md5 = "0" * 32 if self.corrupt_md5 else hashlib.md5(f["data"]).hexdigest()
        return {
            "id": f["id"],
            "name": f["name"],
            "size": str(len(f["data"])),
            "md5Checksum": md5,
            "createdTime": f["created"],
            "mimeType": f["mimeType"],
            "ownedByMe": f["owned"],
        }

    @staticmethod
    def _json(status, obj):
        return drive.Response(status, {}, json.dumps(obj).encode())

    def __call__(self, method, url, headers, body):
        u = urllib.parse.urlparse(url)
        q = dict(urllib.parse.parse_qsl(u.query))
        self.calls.append((method, u.path, q, dict(headers)))
        if "upload_id" in q:  # a resumable session URL carries its own authority
            return self._chunk(headers, body, q["upload_id"])
        if headers.get("Authorization") not in ["Bearer " + t for t in self.valid]:
            return self._json(401, {"error": {"message": "invalid credentials"}})
        if method == "POST":
            sid = "s%d" % len(self.sessions)
            self.sessions[sid] = {
                "meta": json.loads(body),
                "data": bytearray(),
                "size": int(headers["X-Upload-Content-Length"]),
            }
            loc = (
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id="
                + sid
            )
            return drive.Response(200, {"location": loc}, b"{}")
        if method == "GET" and u.path.endswith("/drive/v3/files"):
            return self._list(q)
        if method == "GET" and u.path.endswith("/permissions"):
            if self.perms_status != 200:
                return self._json(self.perms_status, {"error": {"message": "no"}})
            assert u.path.split("/")[-2] == FOLDER, "permissions of something else"
            assert (
                q["fields"]
                == "nextPageToken,permissions(type,role,emailAddress,domain)"
            )
            return self._json(200, {"permissions": self.perms})
        f = self.files.get(u.path.rsplit("/", 1)[1])
        if f is None:
            return self._json(404, {"error": {"message": "not found"}})
        if method == "GET" and q.get("alt") == "media":
            a, b = re.match(r"bytes=(\d+)-(\d+)", headers["Range"]).groups()
            return drive.Response(206, {}, f["data"][int(a) : int(b) + 1])
        if method == "GET":
            out = self._meta(f)
            if f["id"] == FOLDER:
                out["capabilities"] = {"canAddChildren": True}
                out["shared"] = self.folder_shared
            return self._json(200, out)
        if method == "PATCH":
            assert json.loads(body) == {"trashed": True}
            f["trashed"] = True
            return self._json(200, {"id": f["id"], "trashed": True})
        if method == "DELETE":
            del self.files[f["id"]]
            return drive.Response(204, {}, b"")
        raise AssertionError("unexpected request %s %s" % (method, u.path))

    def _list(self, q):
        if self.list_status != 200:
            return self._json(self.list_status, {"error": {"message": "boom"}})
        parent = re.match(r"'([^']+)' in parents", q["q"]).group(1)
        items = sorted(
            (
                f
                for f in self.files.values()
                if parent in f["parents"] and not f["trashed"]
            ),
            key=lambda f: f["id"],
        )
        start = int(q.get("pageToken", "0"))
        out = {
            "files": [self._meta(f) for f in items[start : start + 2]]
        }  # two per page
        if start + 2 < len(items):
            out["nextPageToken"] = str(start + 2)
        return self._json(200, out)

    def _chunk(self, headers, body, sid):
        s = self.sessions[sid]
        crange = headers["Content-Range"]
        if not crange.startswith("bytes */"):  # a real chunk, not a status query
            n, self.chunk_puts = self.chunk_puts, self.chunk_puts + 1
            if n in self.fail_chunks:
                return self._json(503, {"error": {"message": "try later"}})
            assert int(re.match(r"bytes (\d+)-", crange).group(1)) == len(
                s["data"]
            ), "out of order"
            s["data"].extend(body)
        got = len(s["data"])
        if got == s["size"]:
            if "id" not in s:
                s["id"] = self.add(
                    s["meta"]["name"], bytes(s["data"]), parents=s["meta"]["parents"]
                )
            return self._json(200, self._meta(self.files[s["id"]]))
        return drive.Response(
            308, {"range": "bytes=0-%d" % (got - 1)} if got else {}, b""
        )


def make_client(drv, token=None, say=None, **kw):
    kw.setdefault("sleep", lambda s: None)
    kw.setdefault(
        "chunk_bytes", 256 * 1024
    )  # small, so a 600 KB zip takes three chunks
    return drive.DriveClient(
        token or FakeToken("tok-new"), transport=drv, say=say or (lambda m: None), **kw
    )


def make_produced(tmp_path, size=600_000, name=ZIP_NAME):
    p = tmp_path / name
    p.write_bytes((b"snapshot-bytes-" * (size // 15 + 1))[:size])
    return drive.Produced.after_scan(p, [], [], [], True, snap.SNAPSHOT_RE)


def use_fake_drive(repo, drv, token=None):
    repo.mp.setattr(
        snap,
        "make_drive_client",
        lambda cfg: make_client(
            drv, token=token, say=snap.say, allow_shared=cfg.allow_shared
        ),
    )
    repo.mp.setenv("GBX_SNAPSHOT_DRIVE_FOLDER_ID", FOLDER)


def clean_gitleaks(repo, tmp_path):
    stub = tmp_path / "gitleaks-clean"
    stub.write_text("#!/bin/sh\nexit 0\n")
    stub.chmod(0o755)
    repo.mp.setenv("GBX_SNAPSHOT_GITLEAKS", str(stub))


# -- upload --
def test_upload_sends_the_zip_in_chunks_and_checks_drives_checksum(tmp_path):
    drv, lines = FakeDrive(), []
    client = make_client(drv, say=lines.append, quota_project="quota-test-proj")
    produced = make_produced(tmp_path)
    client.upload(produced, FOLDER, snap.SNAPSHOT_RE)
    (stored,) = [f for f in drv.files.values() if f["name"] == ZIP_NAME]
    assert stored["data"] == produced.path.read_bytes() and stored["parents"] == [
        FOLDER
    ]
    chunks = [
        c
        for c in drv.methods("PUT")
        if not c[3]["Content-Range"].startswith("bytes */")
    ]
    assert len(chunks) == 3
    # the session URL is the credential for chunks: no bearer token, no quota project on them
    assert all(
        "Authorization" not in c[3] and "x-goog-user-project" not in c[3]
        for c in chunks
    )
    start = drv.methods("POST")[0]
    assert start[3]["x-goog-user-project"] == "quota-test-proj"
    assert any("md5 and size match" in m for m in lines)


def test_a_checksum_mismatch_fails_the_upload_and_removes_the_bad_copy(tmp_path):
    drv = FakeDrive()
    drv.corrupt_md5 = True
    with pytest.raises(drive.VerifyError):
        make_client(drv).upload(make_produced(tmp_path), FOLDER, snap.SNAPSHOT_RE)
    assert drv.live() == set() and len(drv.methods("DELETE")) == 1


def test_a_401_refreshes_the_token_once_and_the_token_is_never_printed(tmp_path):
    drv, lines = FakeDrive(), []
    token = FakeToken("tok-old", "tok-new")
    make_client(drv, token=token, say=lines.append).upload(
        make_produced(tmp_path), FOLDER, snap.SNAPSHOT_RE
    )
    assert token.refreshes == 1 and ZIP_NAME in drv.live()
    assert not any("tok-" in m for m in lines)


def test_a_token_that_stays_invalid_stops_after_one_refresh():
    drv = FakeDrive()
    drv.valid = {"never"}
    token = FakeToken("a", "b")
    with pytest.raises(drive.DriveError, match="401"):
        make_client(drv, token=token).list_folder(FOLDER)
    assert token.refreshes == 1


def test_a_failed_chunk_resumes_from_what_drive_holds(tmp_path):
    drv = FakeDrive()
    drv.fail_chunks = {1}
    produced = make_produced(tmp_path)
    make_client(drv).upload(produced, FOLDER, snap.SNAPSHOT_RE)
    (stored,) = [f for f in drv.files.values() if f["name"] == ZIP_NAME]
    assert stored["data"] == produced.path.read_bytes()
    queries = [
        c for c in drv.methods("PUT") if c[3]["Content-Range"].startswith("bytes */")
    ]
    assert len(queries) == 1


def test_the_time_cap_stops_a_stuck_request(monkeypatch):
    monkeypatch.setattr(drive.random, "random", lambda: 0.0)
    drv, now = FakeDrive(), [0.0]
    drv.list_status = 503
    client = make_client(
        drv,
        clock=lambda: now[0],
        sleep=lambda s: now.__setitem__(0, now[0] + s),
        total_seconds=20,
    )
    with pytest.raises(drive.DriveTimeout):
        client.list_folder(FOLDER)


def test_nothing_but_a_clean_scan_can_make_an_uploadable_zip(tmp_path):
    p = tmp_path / ZIP_NAME
    p.write_bytes(b"z")
    ok = dict(
        findings=[],
        zip_findings=[],
        allowed_paths=[],
        gitleaks_ran=True,
        name_re=snap.SNAPSHOT_RE,
    )
    assert drive.Produced.after_scan(p, **ok).md5
    for bad in (
        dict(findings=[object()]),
        dict(zip_findings=[object()]),
        dict(allowed_paths=["x/y"]),
        dict(gitleaks_ran=False),
    ):
        with pytest.raises(drive.UploadRefused):
            drive.Produced.after_scan(p, **dict(ok, **bad))
    with pytest.raises(drive.UploadRefused):
        drive.Produced(p, 1, "0" * 32)  # no building one by hand
    other = tmp_path / "notes.zip"
    other.write_bytes(b"z")
    with pytest.raises(drive.UploadRefused):
        drive.Produced.after_scan(other, **ok)  # not named like a snapshot


# -- prune --
def test_prune_keeps_the_newest_n_by_name_and_touches_nothing_else():
    drv = FakeDrive()
    for i in range(
        1, 7
    ):  # Drive's own dates run backwards on purpose: the name decides
        drv.add(
            "geobrix-snapshot-2026110%dT120000Z-main-abc1234.zip" % i,
            created="2026-12-%02dT00:00:00Z" % (10 - i),
        )
    others = [
        "notes.zip",
        "geobrix-snapshot-notes.zip",
        "geobrix-snapshot-20261101T120000Z-main.zip",
        "geobrix-snapshot-20261102T120000Z-main-abc1234.zip.bak",
        "geobrix-snapshot-20260101T000000Z-main-abc1234.zip",  # matches, but not yours
    ]
    for name in others:
        drv.add(name, owned=not name.startswith("geobrix-snapshot-2026010"))
    drv.add(
        "geobrix-snapshot-20261105T000000Z-x-abc1234.zip", mime=drive.FOLDER_MIME
    )  # a folder
    res = make_client(drv).prune(FOLDER, 3, snap.SNAPSHOT_RE)
    assert {f["name"] for f in res.trashed} == {
        "geobrix-snapshot-2026110%dT120000Z-main-abc1234.zip" % i for i in (1, 2, 3)
    }
    assert set(others) <= drv.live() and len(res.foreign) == 1
    assert drv.methods("DELETE") == [] and len(drv.methods("PATCH")) == 3


def test_prune_dry_run_changes_nothing():
    drv, lines = FakeDrive(), []
    for i in range(1, 6):
        drv.add("geobrix-snapshot-2026110%dT120000Z-main-abc1234.zip" % i)
    res = make_client(drv, say=lines.append).prune(
        FOLDER, 2, snap.SNAPSHOT_RE, dry_run=True
    )
    assert len(res.trashed) == 3 and len(drv.live()) == 5
    assert (
        drv.methods("PATCH", "DELETE") == []
        and sum("would trash" in m for m in lines) == 3
    )


def test_prune_refuses_a_folder_with_no_snapshot_in_it():
    drv = FakeDrive()
    drv.add("report.pdf")
    drv.add("geobrix-snapshot-notes.zip")
    with pytest.raises(drive.PruneRefused):
        make_client(drv).prune(FOLDER, 3, snap.SNAPSHOT_RE)
    assert drv.methods("PATCH", "DELETE") == []


def test_prune_removes_nothing_when_the_listing_fails():
    drv = FakeDrive()
    for i in range(1, 6):
        drv.add("geobrix-snapshot-2026110%dT120000Z-main-abc1234.zip" % i)
    drv.list_status = 500
    with pytest.raises(drive.DriveError):
        make_client(drv).prune(FOLDER, 2, snap.SNAPSHOT_RE)
    assert drv.methods("PATCH", "DELETE") == [] and len(drv.live()) == 5


def test_prune_of_an_empty_folder_is_a_no_op_and_the_new_upload_must_be_kept():
    assert make_client(FakeDrive()).prune(FOLDER, 3, snap.SNAPSHOT_RE).trashed == []
    listing = [
        {
            "id": "fakeFile000001",
            "name": "geobrix-snapshot-20261101T120000Z-main-abc1234.zip",
            "ownedByMe": True,
        },
        {
            "id": "fakeFile000002",
            "name": "geobrix-snapshot-20261102T120000Z-main-abc1234.zip",
            "ownedByMe": True,
        },
    ]
    with pytest.raises(
        drive.PruneRefused
    ):  # the id to protect is not among the newest 1
        drive.plan_prune(listing, 1, snap.SNAPSHOT_RE, protect_ids=["fakeFile000001"])


def test_a_prune_trashes_at_most_the_cap_oldest_first():
    drv = FakeDrive()
    for d in range(1, 31):
        drv.add("geobrix-snapshot-202611%02dT120000Z-main-abc1234.zip" % d)
    res = make_client(drv).prune(FOLDER, 3, snap.SNAPSHOT_RE)
    assert (
        len(res.trashed) == drive.MAX_TRASHED_PER_RUN == 25 and len(res.deferred) == 2
    )
    assert "geobrix-snapshot-20261101T120000Z-main-abc1234.zip" not in drv.live()
    assert "geobrix-snapshot-20261127T120000Z-main-abc1234.zip" in drv.live()


# -- download --
def test_download_is_ranged_and_checked_against_drives_checksum(tmp_path):
    drv = FakeDrive()
    data = bytes(range(256)) * 3000  # 768000 bytes: three ranges
    fid = drv.add(ZIP_NAME, data)
    meta = drv._meta(drv.files[fid])
    make_client(drv).download(meta, tmp_path / ZIP_NAME)
    assert (tmp_path / ZIP_NAME).read_bytes() == data
    drv.corrupt_md5 = True
    with pytest.raises(drive.VerifyError):
        make_client(drv).download(drv._meta(drv.files[fid]), tmp_path / "again.zip")
    assert not (tmp_path / "again.zip").exists() and not list(tmp_path.glob("*.part"))


# -- the command, with the fake Drive standing in for the real one --
def test_run_uploads_the_zip_it_just_wrote_then_prunes_drive(repo, tmp_path, capsys):
    clean_gitleaks(repo, tmp_path)
    drv = FakeDrive()
    for d in range(1, 5):
        drv.add("geobrix-snapshot-2026010%dT000000Z-main-abc1234.zip" % d)
    use_fake_drive(repo, drv)
    assert repo.run("run", "--upload") == 0
    local, out = repo.newest_zip(), capsys.readouterr().out
    assert (
        len(drv.live()) == 3 and local.name in drv.live()
    )  # four old + one new, keep 3
    (stored,) = [f for f in drv.files.values() if f["name"] == local.name]
    assert stored["data"] == local.read_bytes()
    assert "RESULT ok" in out and "drive=uploaded,trashed=2" in out
    assert FOLDER not in out and "tok-" not in out


def test_the_upload_is_off_unless_asked_for(repo, tmp_path):
    clean_gitleaks(repo, tmp_path)
    drv = FakeDrive()
    use_fake_drive(repo, drv)
    assert repo.run("run") == 0 and drv.calls == []
    repo.mp.setenv(
        "GBX_SNAPSHOT_UPLOAD", "1"
    )  # the setting turns it on without the flag
    assert repo.run("run") == 0 and len(drv.methods("POST")) == 1


def test_a_secret_stops_the_run_before_any_drive_call(repo, tmp_path):
    clean_gitleaks(repo, tmp_path)
    drv = FakeDrive()
    use_fake_drive(repo, drv)
    write(repo.root, ".superpowers/notes.md", fake("ghp_", "a1B2" * 9))
    assert repo.run("run", "--upload") == snap.EXIT_REFUSED
    assert drv.calls == [] and snap.snapshots(repo.dest) == []


def test_no_clean_gitleaks_pass_means_no_upload_but_the_local_zip_stays(repo, capsys):
    drv = FakeDrive()  # the fixture points GBX_SNAPSHOT_GITLEAKS at nothing
    use_fake_drive(repo, drv)
    assert repo.run("run", "--upload") == snap.EXIT_DRIVE
    out = capsys.readouterr().out
    assert drv.calls == [] and len(snap.snapshots(repo.dest)) == 1
    assert "gitleaks did not run cleanly" in out and "RESULT drive-failed" in out


def test_a_waived_scan_finding_blocks_the_upload_not_the_local_zip(repo, tmp_path):
    clean_gitleaks(repo, tmp_path)
    drv = FakeDrive()
    use_fake_drive(repo, drv)
    write(repo.root, "examples/tests/cluster.env", fake("ghp_", "a1B2" * 9))
    repo.mp.setenv("GBX_SNAPSHOT_SCAN_ALLOW", "examples/tests/cluster.env")
    assert repo.run("run", "--upload") == snap.EXIT_DRIVE
    assert drv.calls == [] and len(snap.snapshots(repo.dest)) == 1


def test_a_failed_checksum_exits_drive_failed_and_never_prunes(repo, tmp_path, capsys):
    clean_gitleaks(repo, tmp_path)
    drv = FakeDrive()
    drv.corrupt_md5 = True
    for d in range(1, 5):
        drv.add("geobrix-snapshot-2026010%dT000000Z-main-abc1234.zip" % d)
    use_fake_drive(repo, drv)
    assert repo.run("run", "--upload") == snap.EXIT_DRIVE
    assert (
        drv.methods("PATCH") == [] and len(drv.live()) == 4
    )  # the bad upload is gone, nothing else
    assert "RESULT drive-failed" in capsys.readouterr().out


def test_upload_dry_run_only_reads_and_says_what_a_prune_would_do(repo, capsys):
    drv = FakeDrive()
    for d in range(1, 5):
        drv.add("geobrix-snapshot-2026010%dT000000Z-main-abc1234.zip" % d)
    use_fake_drive(repo, drv)
    assert repo.run("run", "--dry-run", "--upload") == 0
    out = capsys.readouterr().out
    assert {c[0] for c in drv.calls} == {"GET"} and snap.snapshots(repo.dest) == []
    assert out.count("\n    would trash ") == 2
    assert "shared with: no one (private)" in out and "guard: ok" in out
    assert "throttle: no upload recorded" in out
    assert "'Snapshots'" in out and FOLDER not in out


# -- the folder must be private --
def test_a_shared_folder_is_refused_before_any_upload_and_the_message_names_the_sharing(
    repo, tmp_path, capsys
):
    clean_gitleaks(repo, tmp_path)
    drv = FakeDrive()
    drv.share()
    use_fake_drive(repo, drv)
    assert repo.run("run", "--upload") == snap.EXIT_DRIVE
    out = capsys.readouterr().out
    assert (
        drv.methods("POST", "PUT", "PATCH", "DELETE") == []
    )  # nothing left, nothing removed
    assert len(snap.snapshots(repo.dest)) == 1 and "RESULT drive-failed" in out
    assert "everyone at domain example.com (reader)" in out
    assert "user c***@example.com (writer)" in out and "colleague" not in out
    assert (
        "Fix: make the folder private" in out
        and "GBX_SNAPSHOT_DRIVE_ALLOW_SHARED=1" in out
    )
    assert FOLDER not in out
    assert not (repo.dest / snap.UPLOAD_STAMP).exists()  # a refusal is not an upload


def test_prune_refuses_a_shared_folder_too_even_as_a_dry_run(repo, capsys):
    drv = FakeDrive()
    drv.share()
    for d in range(1, 5):
        drv.add("geobrix-snapshot-2026010%dT000000Z-main-abc1234.zip" % d)
    use_fake_drive(repo, drv)
    for args in (
        ["prune", "--drive", "--keep", "1"],
        ["prune", "--drive", "--dry-run"],
    ):
        assert repo.run(*args) == snap.EXIT_DRIVE
    assert "not private" in capsys.readouterr().out
    assert drv.methods("PATCH", "DELETE") == [] and len(drv.live()) == 4


def test_what_counts_as_other_access():
    owned = {"ownedByMe": True}
    assert drive.other_access(owned, [PRIVATE_PERM]) == []
    assert drive.other_access(owned, []) == []
    anyone = {"type": "anyone", "role": "reader"}
    assert drive.other_access(owned, [PRIVATE_PERM, anyone]) == [
        "ANYONE WITH THE LINK (reader)"
    ]
    group = {"type": "group", "role": "writer", "emailAddress": "team@example.com"}
    assert drive.other_access(owned, [group]) == ["group t***@example.com (writer)"]
    assert drive.other_access(
        {"ownedByMe": False}, [PRIVATE_PERM]
    )  # someone else's folder
    assert drive.other_access({}, [PRIVATE_PERM])  # ownership unknown: not private
    assert drive.other_access(
        dict(owned, shared=True), [PRIVATE_PERM]
    )  # flag says shared
    assert drive.mask_email("colleague@example.com") == "c***@example.com"


def test_an_unreadable_permission_list_is_a_refusal_unless_sharing_is_accepted(
    tmp_path,
):
    drv = FakeDrive()
    drv.perms_status = 403
    with pytest.raises(drive.SharedFolderRefused, match="cannot be confirmed private"):
        make_client(drv).upload(make_produced(tmp_path), FOLDER, snap.SNAPSHOT_RE)
    assert drv.methods("POST") == []
    make_client(drv, allow_shared=True).upload(
        make_produced(tmp_path), FOLDER, snap.SNAPSHOT_RE
    )
    assert ZIP_NAME in drv.live()


def test_allow_shared_accepts_a_shared_folder_and_says_so(repo, tmp_path, capsys):
    clean_gitleaks(repo, tmp_path)
    drv = FakeDrive()
    drv.share()
    use_fake_drive(repo, drv)
    repo.mp.setenv("GBX_SNAPSHOT_DRIVE_ALLOW_SHARED", "1")
    assert repo.run("run", "--upload") == 0
    out = capsys.readouterr().out
    assert repo.newest_zip().name in drv.live() and "drive=uploaded" in out
    assert (
        "accepted by GBX_SNAPSHOT_DRIVE_ALLOW_SHARED" in out and "colleague" not in out
    )


def test_dry_run_reports_the_sharing_and_whether_the_guard_would_refuse(repo, capsys):
    drv = FakeDrive()
    drv.share()
    use_fake_drive(repo, drv)
    assert repo.run("run", "--dry-run", "--upload") == snap.EXIT_DRIVE
    out = capsys.readouterr().out
    assert (
        "everyone at domain example.com (reader)" in out and "c***@example.com" in out
    )
    assert (
        "guard: WOULD REFUSE (exit 4)" in out and "Fix: make the folder private" in out
    )
    assert {c[0] for c in drv.calls} == {"GET"}  # reads only, even when it would refuse
    repo.mp.setenv("GBX_SNAPSHOT_DRIVE_ALLOW_SHARED", "1")
    assert repo.run("run", "--dry-run", "--upload") == 0
    assert "guard: the folder is shared; accepted" in capsys.readouterr().out


# -- the upload throttle --
def set_last_upload(repo, minutes_ago):
    import time

    repo.dest.mkdir(parents=True, exist_ok=True)
    (repo.dest / snap.UPLOAD_STAMP).write_text(
        "%.0f\n" % (time.time() - minutes_ago * 60)
    )


def test_a_hook_started_upload_inside_the_interval_is_skipped_but_the_snapshot_runs(
    repo, tmp_path, capsys
):
    clean_gitleaks(repo, tmp_path)
    drv = FakeDrive()
    use_fake_drive(repo, drv)
    repo.mp.setenv("GBX_SNAPSHOT_UPLOAD", "1")  # the setting, as the push hook uses it
    # nothing recorded yet: this one uploads and records the time
    assert repo.run("run") == 0
    assert len(drv.methods("POST")) == 1 and (repo.dest / snap.UPLOAD_STAMP).is_file()
    capsys.readouterr()
    set_last_upload(repo, 10)
    calls = len(drv.calls)
    assert repo.run("run") == 0
    out = capsys.readouterr().out
    assert len(drv.calls) == calls  # the Drive step was skipped: no request at all
    assert out.count("drive: skipping the upload") == 1 and "10 min ago" in out
    assert "RESULT ok" in out and "drive=throttled" in out
    assert "wrote geobrix-snapshot-" in out  # the local snapshot still ran
    set_last_upload(repo, 61)  # past the interval: uploads again
    assert repo.run("run") == 0 and len(drv.methods("POST")) == 2


def test_the_interval_is_a_setting_and_zero_turns_the_throttle_off(repo, tmp_path):
    clean_gitleaks(repo, tmp_path)
    drv = FakeDrive()
    use_fake_drive(repo, drv)
    repo.mp.setenv("GBX_SNAPSHOT_UPLOAD", "1")
    set_last_upload(repo, 10)
    repo.mp.setenv("GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN", "5")
    assert repo.run("run") == 0 and len(drv.methods("POST")) == 1  # 10 min > 5 min
    set_last_upload(repo, 1)
    repo.mp.setenv("GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN", "0")
    assert repo.run("run") == 0 and len(drv.methods("POST")) == 2


def test_a_manual_upload_ignores_the_throttle_unless_asked_to_respect_it(
    repo, tmp_path, capsys
):
    clean_gitleaks(repo, tmp_path)
    drv = FakeDrive()
    use_fake_drive(repo, drv)
    set_last_upload(repo, 1)
    assert repo.run("run", "--upload", "--respect-throttle") == 0
    assert drv.methods("POST") == [] and "drive=throttled" in capsys.readouterr().out
    assert repo.run("run", "--upload") == 0  # explicit: uploads anyway
    assert len(drv.methods("POST")) == 1


def test_a_failed_or_refused_upload_does_not_start_the_throttle(repo, tmp_path):
    clean_gitleaks(repo, tmp_path)
    drv = FakeDrive()
    drv.corrupt_md5 = True
    use_fake_drive(repo, drv)
    assert repo.run("run", "--upload") == snap.EXIT_DRIVE
    assert not (repo.dest / snap.UPLOAD_STAMP).exists()


def test_an_unreadable_or_future_stamp_does_not_block_an_upload(repo):
    repo.dest.mkdir(parents=True)
    cfg = snap.Config(repo.root)
    (repo.dest / snap.UPLOAD_STAMP).write_text("not a number")
    assert snap.upload_throttled(cfg) is None
    set_last_upload(repo, -120)  # a clock that ran backwards
    assert snap.upload_throttled(cfg) is None
    set_last_upload(repo, 3)
    assert snap.upload_throttled(cfg) is not None


def test_missing_drive_settings_fail_cleanly_without_a_request(repo, capsys):
    drv = FakeDrive()
    repo.mp.setattr(snap, "make_drive_client", lambda cfg: make_client(drv))
    assert repo.run("run", "--upload") == snap.EXIT_DRIVE
    assert drv.calls == [] and len(snap.snapshots(repo.dest)) == 1
    assert "GBX_SNAPSHOT_DRIVE_FOLDER_ID is not set" in capsys.readouterr().out


def test_list_prune_and_restore_work_from_drive(repo, tmp_path, capsys):
    assert repo.run("run") == 0
    local = repo.newest_zip()
    drv = FakeDrive()
    drv.add(local.name, local.read_bytes())
    drv.add("geobrix-snapshot-20260101T000000Z-main-abc1234.zip", owned=False)
    use_fake_drive(repo, drv)
    assert repo.run("list", "--drive") == 0
    out = capsys.readouterr().out
    assert local.name in out and "not yours" in out
    target = tmp_path / "restored"
    assert repo.run("restore", "--from-drive", local.name, str(target)) == 0
    assert (target / "worktree" / "README.md").read_text() == "hi"
    foreign = "geobrix-snapshot-20260101T000000Z-main-abc1234.zip"
    assert repo.run("restore", "--from-drive", foreign, str(tmp_path / "other")) == 2
    assert repo.run("prune", "--drive", "--keep", "1", "--dry-run") == 0
    assert drv.methods("PATCH", "DELETE") == []


def test_the_command_passes_drive_options_through_and_stops_without_a_folder(
    command_repo,
):
    for args in (
        ["--upload", "--dry-run"],
        ["prune", "--drive", "--dry-run"],
        ["list", "--drive"],
    ):
        res = run_cmd(command_repo, *args)
        assert res.returncode == snap.EXIT_DRIVE, res.stdout + res.stderr
        assert "GBX_SNAPSHOT_DRIVE_FOLDER_ID is not set" in res.stdout


# -- settings and hygiene --
def test_drive_settings_default_off_and_are_validated(tmp_path):
    cfg = snap.Config(tmp_path, env={})
    assert cfg.upload is False and cfg.drive_keep == 3 and cfg.drive_folder_id == ""
    assert cfg.allow_shared is False and cfg.upload_min_interval_min == 60
    on = snap.Config(
        tmp_path,
        env={
            "GBX_SNAPSHOT_UPLOAD": "1",
            "GBX_SNAPSHOT_DRIVE_KEEP": "5",
            "GBX_SNAPSHOT_DRIVE_FOLDER_ID": FOLDER,
            "GBX_SNAPSHOT_DRIVE_ALLOW_SHARED": "1",
            "GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN": "0",
        },
    )
    assert on.upload and on.drive_keep == 5 and on.drive_folder_id == FOLDER
    assert on.allow_shared and on.upload_min_interval_min == 0
    # the real client is built from the settings (building it makes no request)
    assert snap.make_drive_client(on).allow_shared is True
    assert snap.make_drive_client(cfg).allow_shared is False
    with pytest.raises(SystemExit):
        snap.Config(tmp_path, env={"GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN": "-1"})
    assert drive.config_problems("short", "") and drive.config_problems(
        FOLDER, "Bad Project!"
    )
    assert (
        drive.config_problems(FOLDER, "my-quota-project1") == []
        and drive.config_problems(FOLDER, "") == []
    )


def test_errors_never_carry_a_token_the_folder_id_or_the_quota_project():
    text = "Bearer abc.def ya29.abcDEF-123 in %s under quota-test-proj" % FOLDER
    out = drive.redact(text, [FOLDER, "quota-test-proj"])
    assert not any(s in out for s in ("abc.def", "ya29", FOLDER, "quota-test-proj"))


def test_the_drive_module_carries_no_ids_or_internal_names():
    src = (REPO_ROOT / "scripts" / "snapshot" / "drive_upload.py").read_text()
    assert "databricks" not in src.lower() and "gcp-dev" not in src
    assert not re.search(
        r"[\"'](?=[A-Za-z_-]*\d)[A-Za-z0-9_-]{25,}[\"']", src
    )  # no id-shaped literal
    top_level = (REPO_ROOT / "scripts" / "snapshot" / "snapshot.py").read_text()
    assert not re.search(
        r"^(import|from)\s+drive_upload", top_level, re.M
    )  # loaded lazily only
