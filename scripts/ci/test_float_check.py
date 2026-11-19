"""gbx:build:wheel --float-check's pure parts (scripts/native/float_check.py): reading a pip report.

- a package the runtime preinstalls at another version is a move, with the requirement chain that asks for it;
- a package the runtime lacks is new; the requested wheel itself is neither;
- an extra's requirement counts only for that extra; a marker for another platform does not count;
- the stub dist-infos make pip see the runtime's packages installed, pip itself left real;
- the installs are the core, each extra alone and all extras together; an extra's moves are judged as install_moves'
  'full' install;
- report() prints a row per (runtime, install) and exits 1 on an undocumented move or a failed resolution;
- host() stops on a missing wheel, image or index before any container runs, and hands the index to the container
  through the environment only.
"""

import importlib.util
import json
import subprocess
import sys
from argparse import Namespace
from pathlib import Path

import pytest

NATIVE = Path(__file__).resolve().parents[1] / "native"
sys.path.insert(0, str(NATIVE))
_spec = importlib.util.spec_from_file_location("float_check", NATIVE / "float_check.py")
fc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fc)


def _item(name, version, requires=(), requested=False):
    return {"requested": requested, "metadata": {"name": name, "version": version, "requires_dist": list(requires)}}


REPORT = {
    "install": [
        _item("geobrix", "0.6.0+env6", ['httpx2>=2.12', 'contextily; extra == "vizx"', 'urllib3<3'], requested=True),
        _item("httpx2", "2.16.0", ["idna>=3.18", "truststore", 'pywin32; sys_platform == "win32"']),
        _item("idna", "3.20"),
        _item("truststore", "0.10.5"),
        _item("contextily", "1.7.0", ["xyzservices", "requests"]),
        _item("xyzservices", "2026.9.1"),
    ]
}
RUNTIME = {"idna": "3.11", "urllib3": "2.5.0", "requests": "2.32.5", "pillow": "12.0.0"}


def test_classify_names_the_move_and_its_chain():
    moves, new = fc.classify(REPORT, RUNTIME, ())
    assert moves == [("idna", "3.11", "3.20", "geobrix -> httpx2 -> idna")]
    assert ("truststore", "0.10.5", "geobrix -> httpx2 -> truststore") in new
    assert all(n != "geobrix" for n, _v, _c in new)  # the requested wheel is neither


def test_an_extras_requirement_counts_only_for_that_extra():
    assert "contextily" not in fc.chains(REPORT, ())
    chains = fc.chains(REPORT, ("vizx",))
    assert chains["xyzservices"] == "geobrix[vizx] -> contextily -> xyzservices"
    assert "pywin32" not in chains  # a marker for another platform


def test_no_move_when_the_report_keeps_the_runtimes_versions():
    report = {"install": [_item("geobrix", "0.6.0+env6", ["pillow>=12.0.0,<14"], requested=True)]}
    assert fc.classify(report, RUNTIME, ()) == ([], [])  # pillow 12.0.0 satisfies: pip lists nothing


def test_a_downgrade_is_a_move_too():
    report = {"install": [_item("geobrix", "0.6.0+env6", ["urllib3<2.6"], requested=True), _item("urllib3", "2.5.0")]}
    moves, _new = fc.classify(report, {"urllib3": "2.6.3"}, ())
    assert moves == [("urllib3", "2.6.3", "2.5.0", "geobrix -> urllib3")]


def test_installed_stubs_are_what_pip_sees(tmp_path):
    n = fc.make_installed(tmp_path, {"pip": "25.0", "pillow": "12.0.0", "typing-extensions": "4.15.0"})
    assert n == 2 and not list(tmp_path.glob("pip-*"))
    out = subprocess.run(
        [sys.executable, "-c", "import importlib.metadata as m, sys; d = {x.metadata['Name']: x.version for x in "
         "m.distributions(path=[sys.argv[1]])}; print(sorted(d.items()))", str(tmp_path)],
        capture_output=True, text=True, check=True,
    ).stdout  # fmt: skip
    assert "('pillow', '12.0.0')" in out and "('typing-extensions', '4.15.0')" in out


def test_installs_and_their_allowed_moves():
    labels = [label for label, _x in fc.installs(6)]
    assert labels[0] == "core" and labels[-1] == "full"
    assert set(labels[1:-1]) == set(fc.extras_of(6)) and "zarr" in labels  # one per extra-<name>.txt
    assert "requests" not in fc.allowed_for(5, "core") and "requests" in fc.allowed_for(5, "stac")
    assert "idna" in fc.allowed_for(6, "core") and "anyio" not in fc.allowed_for(6, "full")


# --- report(): the host's verdict over the container's rows -----------------------------------------------------------


def _row(install="core", moves=(), new=(), **extra):
    row = {"runtime": "dbr19", "install": install, "installed_before": 3, "moves": list(moves), "new": list(new)}
    return {**row, **extra}


def _move(name, documented):
    return {"name": name, "runtime": "3.11", "to": "3.20", "chain": f"geobrix -> {name}", "documented": documented}


def test_report_passes_a_documented_move_and_lists_new_packages(capsys):
    new = [{"name": "truststore", "version": "0.10.5", "chain": "geobrix -> t"}]
    rows = [_row(moves=[_move("idna", True)], new=new)]
    assert fc.report(6, "x86_64", Path("/w/geobrix.whl"), rows) == 0
    out = capsys.readouterr().out
    assert "status=ok moves=1 undocumented=0 new=1" in out
    assert "documented move: idna 3.11 -> 3.20 (geobrix -> idna)" in out
    assert "new: truststore==0.10.5 (geobrix -> t)" in out
    assert "FLOAT_CHECK env=6 arch=x86_64 wheel=geobrix.whl rows=1 moves=1 undocumented=0" in out


def test_report_fails_an_undocumented_move(capsys):
    rows = [_row(moves=[_move("idna", True), _move("numpy", False)]), _row("full")]
    assert fc.report(6, "aarch64", Path("geobrix.whl"), rows) == 1
    out = capsys.readouterr().out
    assert "install=core status=FAILED moves=2 undocumented=1" in out
    assert "UNDOCUMENTED MOVE: numpy" in out
    assert "install=full status=ok" in out  # the verdict is per row
    assert "rows=2 moves=2 undocumented=1" in out


def test_report_counts_a_failed_resolution_as_undocumented(capsys):
    rows = [_row(error="No matching distribution found for foo"), _row("full")]
    assert fc.report(5, "x86_64", Path("g.whl"), rows) == 1
    out = capsys.readouterr().out
    assert "install=core status=FAILED" in out and "no resolution: No matching distribution" in out
    assert "rows=2 moves=0 undocumented=1" in out


def test_report_with_no_moves_passes(capsys):
    assert fc.report(6, "x86_64", Path("g.whl"), [_row(), _row("full")]) == 0
    assert "rows=2 moves=0 undocumented=0" in capsys.readouterr().out


# --- host(): wheel, image and index are checked before docker runs ----------------------------------------------------


class _Docker:
    """subprocess.run stand-in: `docker image inspect` answers ``image_ok``; `docker run` records the call and, when
    it succeeds, writes the rows the container would."""

    def __init__(self, image_ok=True, run_rc=0, rows=(), stderr=""):
        self.image_ok, self.run_rc, self.rows, self.stderr, self.calls = image_ok, run_rc, list(rows), stderr, []

    def __call__(self, cmd, **kw):
        self.calls.append((cmd, kw))
        if cmd[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(cmd, 0 if self.image_ok else 1, "", "")
        assert cmd[:2] == ["docker", "run"], cmd
        if self.run_rc == 0:
            out = Path(cmd[cmd.index("--json") + 1].replace("/out", str(self.out), 1))
            out.write_text(json.dumps(self.rows))
        return subprocess.CompletedProcess(cmd, self.run_rc, "", self.stderr)

    @property
    def runs(self):
        return [c for c, _kw in self.calls if c[:2] == ["docker", "run"]]


@pytest.fixture
def hostenv(tmp_path, monkeypatch):
    """A fake repo with an env6 wheel in dist/, a known image tag and a configured index."""
    import pins

    (tmp_path / "dist" / "env6").mkdir(parents=True)
    wheel = tmp_path / "dist" / "env6" / "geobrix-0.6.0-py3-none-any.whl"
    wheel.write_text("")
    monkeypatch.setattr(fc, "REPO", tmp_path)
    monkeypatch.setattr(pins, "tag_hash", lambda root, arch: "abc123def456")
    monkeypatch.setattr(fc, "index_url", lambda: "https://user:secret@pypi.example.internal/simple")
    docker = _Docker()
    docker.out = tmp_path / "out"
    monkeypatch.setattr(fc.subprocess, "run", docker)
    args = Namespace(env=6, arch="x86_64", wheel=None, runtime=None, out=str(docker.out))
    return Namespace(args=args, docker=docker, wheel=wheel, tmp=tmp_path)


def test_host_without_a_wheel_stops_before_docker(hostenv, capsys):
    hostenv.wheel.unlink()
    assert fc.host(hostenv.args) == 1
    assert "PRECONDITION MISSING: no env6 wheel" in capsys.readouterr().out
    assert hostenv.docker.calls == []


def test_host_without_the_toolchain_image_does_not_run_it(hostenv, capsys):
    hostenv.docker.image_ok = False
    assert fc.host(hostenv.args) == 1
    out = capsys.readouterr().out
    assert "PRECONDITION MISSING: geobrix-native-build:toolchain-abc123def456-x86_64" in out
    assert hostenv.docker.runs == []


def test_host_without_an_index_does_not_run_docker_run(hostenv, monkeypatch, capsys):
    monkeypatch.setattr(fc, "index_url", lambda: "")
    assert fc.host(hostenv.args) == 1
    assert "PRECONDITION MISSING: no package index" in capsys.readouterr().out
    assert hostenv.docker.runs == []


def test_host_runs_the_container_and_reports_its_rows(hostenv, capsys):
    hostenv.docker.rows = [_row(moves=[_move("idna", True)]), _row("full")]
    assert fc.host(hostenv.args) == 0
    (cmd,) = hostenv.docker.runs
    assert cmd[cmd.index("--platform") + 1] == "linux/amd64"
    assert cmd[cmd.index("--arch") + 1] == "x86_64" and cmd[cmd.index("--wheel") + 1] == f"/wheel/{hostenv.wheel.name}"
    runtimes = [cmd[i + 1] for i, a in enumerate(cmd) if a == "--runtime"]
    assert runtimes == list(fc.RUNTIMES[6])  # default: the env's family
    env = [kw for c, kw in hostenv.docker.calls if c[:2] == ["docker", "run"]][0]["env"]
    assert env["PIP_INDEX_URL"] == "https://user:secret@pypi.example.internal/simple"
    assert "secret" not in " ".join(cmd) and "pypi.example" not in " ".join(cmd)  # `-e PIP_INDEX_URL`, no value
    assert "FLOAT_CHECK env=6 arch=x86_64" in capsys.readouterr().out


def test_host_returns_one_for_an_undocumented_move_and_honours_runtime(hostenv):
    hostenv.docker.rows = [_row(moves=[_move("numpy", False)])]
    hostenv.args.runtime = ["env6"]
    assert fc.host(hostenv.args) == 1
    (cmd,) = hostenv.docker.runs
    assert [cmd[i + 1] for i, a in enumerate(cmd) if a == "--runtime"] == ["env6"]


def test_host_redacts_a_failed_container_run(hostenv, capsys):
    hostenv.docker.run_rc = 2
    hostenv.docker.stderr = "ERROR: Could not connect to https://pypi.example.internal/simple/foo/"
    assert fc.host(hostenv.args) == 1
    out = capsys.readouterr().out
    assert "pypi.example" not in out and "Could not connect to" in out  # the error stays, the index does not
    assert "rows=0 moves=0 undocumented=1" in out
