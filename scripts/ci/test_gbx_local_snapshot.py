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
        '{\\"File\\":\\"$src/notes.md\\",\\"StartLine\\":2,\\"RuleID\\":\\"aws-access-token\\",\\"Secret\\":\\"AKIAIOSFODNN7EXAMPLE\\"}]" > "$out"\n'
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
