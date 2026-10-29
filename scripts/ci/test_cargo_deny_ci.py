"""The rust-deny gate reads the crates through the JFrog mirror, and says which host it cannot reach.

The first CI run of .github/workflows/rust-deny.yml died in `cargo metadata` with "download of config.json
failed ... SSL connect error": the hardened runner group does not reach index.crates.io. The crates now come from
the JFrog mirror (.github/actions/jfrog-cargo writes cargo's config), and scripts/native/cargo-deny-ci.sh keeps
every check, probes the hosts before cargo runs, and names the ones that do not answer.

Nothing here touches the network: the composite action's script runs in a scratch directory, and the gate script
runs in a scratch repository with fake `curl`, `cargo` and `uname` first on PATH (the real cargo-deny, pinned
and checksum-checked, is exercised by the workflow itself).
"""

import os
import shutil
import stat
import subprocess
import textwrap
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "native" / "cargo-deny-ci.sh"
COMPOSITE = REPO_ROOT / ".github" / "actions" / "jfrog-cargo" / "action.yml"
WORKFLOWS = ("rust-deny.yml", "build-gdal-native.yml")
TOKEN = "tok-3f9a1c-not-a-real-token"
MIRROR_HOST = "databricks.jfrog.io"
CHECKS = ["-D", "index-failure", "advisories", "bans", "licenses", "sources"]


def _workflow_job(name):
    doc = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / name).read_text())
    (job,) = doc["jobs"].values()
    return job


@pytest.mark.parametrize("name", WORKFLOWS)
def test_a_workflow_that_resolves_crates_logs_in_to_jfrog_first(name):
    """OIDC permission, the login, then the cargo config fed by that login's token, then the build."""
    job = _workflow_job(name)
    assert job["permissions"].get("id-token") == "write"
    assert job["permissions"].get("contents") == "read"
    assert job.get("environment") == "runtime"  # as every workflow that uses jfrog-auth

    steps = job["steps"]
    uses = [s.get("uses", "") for s in steps]
    login = uses.index("./.github/actions/jfrog-auth")
    cargo = uses.index("./.github/actions/jfrog-cargo")
    assert login < cargo
    login_id = steps[login]["id"]
    assert (
        steps[cargo]["with"]["jfrog-access-token"]
        == "${{ steps.%s.outputs.jfrog-access-token }}" % login_id
    )
    # the gate / build runs once the config exists
    assert any("run" in s for s in steps[cargo + 1 :])


def _run_composite(tmp_path):
    """Run the composite action's script the way a runner does; return (result, cargo home, GITHUB_ENV text)."""
    (step,) = yaml.safe_load(COMPOSITE.read_text())["runs"]["steps"]
    runner_temp = tmp_path / "runner-temp"
    runner_temp.mkdir()
    github_env = tmp_path / "github-env"
    github_env.write_text("")
    env = {
        "PATH": os.environ["PATH"],
        "RUNNER_TEMP": str(runner_temp),
        "GITHUB_ENV": str(github_env),
        "JFROG_ACCESS_TOKEN": TOKEN,
    }
    result = subprocess.run(
        ["bash", "-c", step["run"]], env=env, capture_output=True, text=True
    )
    return result, runner_temp / "cargo-home", github_env.read_text()


def test_the_cargo_config_replaces_crates_io_and_keeps_the_token_out_of_the_log(
    tmp_path,
):
    tomllib = pytest.importorskip("tomllib")
    result, home, github_env = _run_composite(tmp_path)
    assert result.returncode == 0, result.stderr

    config = tomllib.loads((home / "config.toml").read_text())
    assert config["source"]["crates-io"]["replace-with"] == "jfrog"
    registry = config["source"]["jfrog"]["registry"]
    assert registry.startswith("sparse+https://%s/" % MIRROR_HOST)
    assert config["registries"]["jfrog"]["index"] == registry
    assert config["registries"]["jfrog"]["credential-provider"] == ["cargo:token"]

    credentials = home / "credentials.toml"
    assert (
        tomllib.loads(credentials.read_text())["registries"]["jfrog"]["token"]
        == "Bearer " + TOKEN
    )
    assert stat.S_IMODE(credentials.stat().st_mode) == 0o600

    # the gate's CARGO_HOME is where cargo looks for both files: no reliance on ~/.cargo being found
    assert "GBX_HOST_CARGO_HOME=%s\n" % home in github_env
    assert TOKEN not in result.stdout + result.stderr


def _fake_bin(tmp_path):
    """Fake uname (a Linux x86_64 host), curl (fails for the hosts in FAIL_HOSTS) and cargo (records `cargo deny`)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    def tool(name, body):
        path = bin_dir / name
        path.write_text("#!/bin/bash\n" + textwrap.dedent(body))
        path.chmod(0o755)

    tool(
        "uname",
        """
        case "$1" in -s) echo Linux ;; -m) echo x86_64 ;; *) echo Linux ;; esac
        """,
    )
    tool(
        "curl",
        """
        url="${@: -1}"
        host="${url#*://}"; host="${host%%/*}"
        for h in ${FAIL_HOSTS:-}; do
            if [ "$h" = "$host" ]; then
                echo "curl: (35) SSL connect error (TLS connect error: unexpected eof while reading)" >&2
                exit 35
            fi
        done
        echo "$host" >> "$PROBE_LOG"
        """,
    )
    tool(
        "cargo",
        """
        if [ "$1" = deny ] && [ "$2" = --version ]; then echo "cargo-deny 0.20.2"; exit 0; fi
        if [ "$1" = deny ]; then
            { echo "CARGO_HOME=$CARGO_HOME"; echo "config=$([ -f "$CARGO_HOME/config.toml" ] && echo present || echo missing)";
              echo "args=$*"; } >> "$DENY_LOG"
            exit 0
        fi
        echo "unexpected cargo $*" >&2; exit 1
        """,
    )
    return bin_dir


def _gate_tree(tmp_path):
    """A scratch repository holding the real gate script, one Rust crate with a lock, and stubs for what it downloads."""
    root = tmp_path / "repo"
    native = root / "scripts" / "native"
    native.mkdir(parents=True)
    shutil.copy(SCRIPT, native / "cargo-deny-ci.sh")
    (native / "fetch-lib.sh").write_text(
        textwrap.dedent(
            """
            GBX_NATIVE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
            GBX_NATIVE_ROOT="$(cd "$GBX_NATIVE_DIR/../.." && pwd)"
            GBX_NATIVE_BUILD="$GBX_NATIVE_ROOT/build/native"
            pin() { if [ "$1" = CARGO_DENY_VERSION ]; then echo 0.20.2; else echo stub; fi; }
            pin_url() { echo "https://example.invalid/$2"; }
            fetch_verified() {   # a tarball holding a cargo-deny file, as the real one downloads
                mkdir -p "$(dirname "$1")" "$GBX_NATIVE_BUILD/stub"
                echo '#!/bin/sh' > "$GBX_NATIVE_BUILD/stub/cargo-deny"
                tar -czf "$1" -C "$GBX_NATIVE_BUILD/stub" cargo-deny
            }
            """
        )
    )
    # the real one creates $CARGO_HOME/bin
    installer = native / "install-host-rust.sh"
    installer.write_text('#!/bin/bash\nmkdir -p "${GBX_HOST_CARGO_HOME}/bin"\n')
    crate = root / "rust" / "crate"
    crate.mkdir(parents=True)
    (crate / "Cargo.toml").write_text('[package]\nname = "crate"\nversion = "0.0.0"\n')
    (crate / "Cargo.lock").write_text("version = 4\n")
    (root / "deny.toml").write_text("")
    git = {"cwd": root, "check": True, "capture_output": True}
    subprocess.run(["git", "init", "-q", "."], **git)
    subprocess.run(["git", "add", "-A"], **git)
    return root


def _run_gate(tmp_path, *args, fail_hosts="", with_mirror=True):
    root = _gate_tree(tmp_path)
    cargo_home = tmp_path / "cargo-home"
    cargo_home.mkdir()
    if with_mirror:
        result, home, _ = _run_composite(tmp_path / "composite")
        assert result.returncode == 0, result.stderr
        shutil.copytree(home, cargo_home, dirs_exist_ok=True)
    env = {
        "PATH": "%s:%s" % (_fake_bin(tmp_path), os.environ["PATH"]),
        "HOME": str(tmp_path / "home"),
        "GBX_HOST_CARGO_HOME": str(cargo_home),
        "GBX_HOST_RUSTUP_HOME": str(tmp_path / "rustup"),
        "FAIL_HOSTS": fail_hosts,
        "PROBE_LOG": str(tmp_path / "probe.log"),
        "DENY_LOG": str(tmp_path / "deny.log"),
    }
    result = subprocess.run(
        ["bash", str(root / "scripts" / "native" / "cargo-deny-ci.sh"), *args],
        env=env,
        capture_output=True,
        text=True,
    )
    probed = _read(tmp_path / "probe.log").split()
    return result, probed, _read(tmp_path / "deny.log"), cargo_home


def _read(path):
    return path.read_text() if path.exists() else ""


@pytest.fixture(autouse=True)
def _scratch_dirs(tmp_path):
    (tmp_path / "composite").mkdir()
    (tmp_path / "home").mkdir()


def test_the_gate_keeps_every_check_and_reads_the_crates_through_the_mirror(tmp_path):
    result, probed, deny, cargo_home = _run_gate(tmp_path)
    assert result.returncode == 0, result.stderr
    # the hosts it needs: the mirror (index and crate files) and the advisory database; crates.io itself is not used
    assert sorted(probed) == ["databricks.jfrog.io", "github.com"]
    assert "CARGO_HOME=%s\n" % cargo_home in deny
    # cargo reads the mirror's config.toml from the home the gate uses
    assert "config=present" in deny
    args = [line for line in deny.splitlines() if line.startswith("args=")]
    assert len(args) == 1
    assert "--locked" in args[0]
    # index-failure stays denied: no silent yank skip
    assert args[0].endswith("check " + " ".join(CHECKS))


def test_an_unreachable_mirror_is_named_and_stops_the_gate_before_cargo(tmp_path):
    result, _, deny, _ = _run_gate(tmp_path, fail_hosts=MIRROR_HOST)
    assert result.returncode == 1
    assert "%s UNREACHABLE" % MIRROR_HOST in result.stderr
    assert "github.com UNREACHABLE" not in result.stderr
    assert deny == ""  # cargo deny never ran


def test_without_a_mirror_config_the_gate_probes_the_crates_io_hosts(tmp_path):
    result, probed, _, _ = _run_gate(
        tmp_path, "--probe", fail_hosts="index.crates.io", with_mirror=False
    )
    assert result.returncode == 1
    assert "index.crates.io UNREACHABLE" in result.stderr
    assert sorted(probed) == ["github.com", "static.crates.io"]


def test_probe_mode_needs_no_toolchain_and_names_nothing_when_all_answer(tmp_path):
    result, probed, deny, _ = _run_gate(tmp_path, "--probe")
    assert result.returncode == 0, result.stderr
    assert "UNREACHABLE" not in result.stdout + result.stderr
    assert sorted(probed) == ["databricks.jfrog.io", "github.com"]
    assert deny == ""
