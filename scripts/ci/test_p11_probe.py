"""scripts/native/p11_probe.py: the probe counts a pytest run the way pytest does.

Pure Python, no Docker and no network: a synthetic log (one JSON line per phase, as the probe's pytest plugin writes
it) stands in for a run.
"""

import importlib.util
import inspect
import json

from conftest import REPO_ROOT

spec = importlib.util.spec_from_file_location(
    "p11_probe", REPO_ROOT / "scripts" / "native" / "p11_probe.py"
)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def rec(i, when, outcome, xfail=False, tb=None):
    r = {"id": i, "when": when, "outcome": outcome}
    if xfail:
        r["xfail"] = True
    if tb:
        r["tb"] = tb
    return r


def test_the_log_is_counted_as_pytest_counts_it(tmp_path):
    ok = [rec("t_pass", s, "passed") for s in ("setup", "call", "teardown")]
    rows = [{"start": "t_pass"}, *ok]
    rows += [
        rec("t_skip_setup", "setup", "skipped"),
        rec("t_skip_setup", "teardown", "passed"),
    ]
    rows += [
        rec("t_skip_call", "setup", "passed"),
        rec("t_skip_call", "call", "skipped"),
        rec("t_skip_call", "teardown", "passed"),
    ]
    rows += [
        rec("t_xfail", "setup", "passed"),
        rec("t_xfail", "call", "skipped", xfail=True),
        rec("t_xfail", "teardown", "passed"),
    ]
    rows += [
        rec("t_xpass", "setup", "passed"),
        rec("t_xpass", "call", "passed", xfail=True),
        rec("t_xpass", "teardown", "passed"),
    ]
    rows += [
        rec("t_fail", "setup", "passed"),
        rec("t_fail", "call", "failed", tb="boom"),
        rec("t_fail", "teardown", "passed"),
    ]
    rows += [rec("t_error", "setup", "failed", tb="fixture")]
    rows += [
        rec("t_late", "setup", "passed"),
        rec("t_late", "call", "passed"),
        rec("t_late", "teardown", "failed", tb="cleanup"),
    ]
    rows += [
        rec("mod_a.py", "collect", "failed", tb="import"),
        rec("mod_b.py", "collect", "skipped"),
    ]
    rows += [
        {"start": "t_sub"},
        {**rec("t_sub", "call", "passed"), "subtest": True},
        rec("t_sub", "call", "passed"),
    ]
    rows += [{"start": "t_crash"}, rec("t_crash", "setup", "passed")]
    rows += ok  # a node id can repeat (pytest allows it): both reports count
    log = tmp_path / "x.jsonl"
    log.write_text("\n".join(json.dumps(r) for r in rows) + "\n")

    assert probe.tally(log) == {
        "passed": 4,
        "failed": 1,
        "errors": 3,
        "skipped": 3,
        "xfailed": 1,
        "xpassed": 1,
        "subtests_passed": 1,
    }  # t_crash: setup only
    outcome, tb, crashed = probe.read_log(log)
    assert [outcome[i] for i in ("t_skip_call", "t_xfail", "t_xpass", "t_fail", "t_error", "t_late")] == [
        "skipped", "xfailed", "xpassed", "failed", "failed", "failed",
    ]  # fmt: skip
    assert crashed == ["t_crash"] and tb["t_fail"] == "boom"

    out = tmp_path / "x.out"
    out.write_text(
        "....\n= 5 failed, 1 error, 90 passed, 2 skipped, 3 xfailed, 1 xpassed, 4 warnings, 7 subtests passed in 3.20s =\n"
    )
    assert probe.summary_counts(out) == {
        "passed": 90,
        "failed": 5,
        "errors": 1,
        "skipped": 2,
        "xfailed": 3,
        "xpassed": 1,
        "subtests_passed": 7,
    }

    # the module is loaded with `-p p11_probe`: a function named pytest_* that is not a hook fails the whole run
    hooks = {
        "pytest_runtest_logstart",
        "pytest_collectreport",
        "pytest_runtest_logreport",
    }
    assert {n for n in dir(probe) if n.startswith("pytest_")} == hooks

    # the log path is taken when the plugin loads: a test that replaces os.environ (rasterio's test_session) must not lose its records
    assert "os.environ" not in inspect.getsource(probe._emit)
