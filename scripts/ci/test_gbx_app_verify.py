"""gbx:app:verify (examples/tests/verify_app.py): the RUNNING wait and the answer check, on a fake workspace."""

import importlib
import subprocess
from types import SimpleNamespace as NS

import pytest
from conftest import REPO_ROOT

SH = REPO_ROOT / "scripts" / "commands" / "gbx-app-verify.sh"


@pytest.fixture
def va(monkeypatch):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    return importlib.import_module("verify_app")


def _app(app="RUNNING", compute="ACTIVE", dep="SUCCEEDED", url="https://genie-map-1.aws.databricksapps.com"):
    return NS(app_status=NS(state=app, message="m"), compute_status=NS(state=compute, message=""),
              active_deployment=NS(status=NS(state=dep, message="")), url=url)


class Clock:
    t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def _w(*apps):
    seq = list(apps)
    return NS(apps=NS(get=lambda name: seq.pop(0) if len(seq) > 1 else seq[0]),
              config=NS(authenticate=lambda: {"Authorization": "Bearer t"}))


def test_help_and_an_unknown_option():
    res = subprocess.run(["bash", str(SH), "--help"], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr
    for opt in ("--profile", "--app", "--deploy", "--local-checks", "--path", "--wait-min", "--log"):
        assert opt in res.stdout
    assert subprocess.run(["bash", str(SH), "--nope"], capture_output=True, text=True).returncode == 2


def test_the_app_name_comes_from_the_bundle(va):
    assert va.bundle_app_name() == "genie-map"


def test_the_wait_ends_when_the_app_runs(va):
    c = Clock()
    app, problem = va.wait_running(_w(_app("DEPLOYING", "STARTING"), _app()), "genie-map", 600, say=lambda s: None,
                                   now=c.now, sleep=c.sleep)
    assert problem is None and app.url.startswith("https://")


@pytest.mark.parametrize("bad", [_app("CRASHED"), _app(compute="ERROR"), _app("DEPLOYING", dep="FAILED")])
def test_a_crashed_or_failed_app_fails_at_once(va, bad):
    _, problem = va.wait_running(_w(bad), "genie-map", 600, say=lambda s: None, now=Clock().now, sleep=Clock().sleep)
    assert problem and "genie-map" in problem


def test_the_wait_is_bounded(va):
    c = Clock()
    _, problem = va.wait_running(_w(_app("DEPLOYING", "STARTING")), "genie-map", 60, poll_s=15, say=lambda s: None,
                                 now=c.now, sleep=c.sleep)
    assert "not RUNNING within" in problem and c.t <= 90


def test_the_profile_is_required(va):
    with pytest.raises(SystemExit) as exc:
        va.main([])
    assert exc.value.code == 2


@pytest.mark.parametrize("status,code", [(200, 0), (302, 1), (401, 1)])
def test_the_root_must_answer_2xx(va, monkeypatch, status, code):
    seen = []
    monkeypatch.setattr(va, "probe", lambda url, headers: seen.append((url, headers)) or (status, 10, ""))
    assert va.main(["--profile", "p", "--poll-secs", "0"], workspace_client=lambda profile: _w(_app())) == code
    assert seen == [("https://genie-map-1.aws.databricksapps.com/", {"Authorization": "Bearer t"})]
