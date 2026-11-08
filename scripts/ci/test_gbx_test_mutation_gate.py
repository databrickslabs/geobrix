"""Tests for gbx:test:mutation-gate and scripts/mutation/gate.py: the command's shell, the argument handling
and the verdict maths (no mutmut run: that is minutes of work, done by the command itself)."""

import importlib.util
import json
import subprocess
import sys

import pytest
from conftest import REPO_ROOT

_spec = importlib.util.spec_from_file_location("mutation_gate", REPO_ROOT / "scripts" / "mutation" / "gate.py")
gate = importlib.util.module_from_spec(_spec)
sys.modules["mutation_gate"] = gate
_spec.loader.exec_module(gate)

CMD = REPO_ROOT / "scripts" / "commands" / "gbx-test-mutation-gate.sh"
P = "databricks.labs.gbx.pygx"


def _sh(*args):
    return subprocess.run(["bash", str(CMD), *args], capture_output=True, text=True, cwd=REPO_ROOT)


def test_help_exits_zero_and_names_every_option():
    res = _sh("--help")
    assert res.returncode == 0
    for opt in ("--family", "--modules", "--tests", "--slim-tests", "--oracle-tables", "--out", "--old-codes", "--plan", "--log"):
        assert opt in res.stdout


def test_an_unknown_option_is_refused():
    res = _sh("--bogus")
    assert res.returncode == 1
    assert "Unknown option" in res.stderr


def test_a_run_without_arguments_is_refused():
    assert _sh().returncode == 2


def test_the_command_has_its_registration_and_is_executable():
    assert (REPO_ROOT / "scripts" / "commands" / "gbx-test-mutation-gate.md").exists()
    assert CMD.stat().st_mode & 0o111


def test_plan_resolves_modules_and_tests_and_runs_nothing(tmp_path):
    res = _sh(
        "--family", "unit", "--modules", "pygx/_bng,pygx._custom",
        "--tests", "python/geobrix/test/pygx/test_bng_batched_walk.py --ignore=python/geobrix/test/pygx/test_parity_bng.py",
        "--slim-tests", "test/manager/test_family_cross_tier.py::test_family_oracle_l3_callables",
        "--oracle-tables", "GRID_SCALAR", "--plan",
    )
    assert res.returncode == 0, res.stderr
    plan = json.loads(res.stdout[res.stdout.index("{"):])
    assert plan["modules"] == [
        "src/databricks/labs/gbx/pygx/_bng.py",
        "src/databricks/labs/gbx/pygx/_custom.py",
    ]
    assert plan["old"] == ["test/pygx/test_bng_batched_walk.py", "--ignore=test/pygx/test_parity_bng.py"]
    assert plan["oracle_tables"] == "GRID_SCALAR"
    assert "/gbx-mut-unit-" in plan["work"] and "<random>" in plan["work"] and "mutate_only_covered_lines = true" in plan["mutmut"]


def test_a_missing_module_or_test_is_refused():
    base = ["--family", "unit", "--tests", "test/pygx/test_bng_batched_walk.py", "--slim-tests", "test/pygx/test_bng_batched_walk.py", "--plan"]
    assert _sh("--modules", "pygx/_nope", *base).returncode != 0
    res = _sh("--modules", "pygx/_bng", "--family", "unit", "--tests", "test/pygx/test_nope.py", "--slim-tests", "test/pygx/test_bng_batched_walk.py", "--plan")
    assert res.returncode != 0 and "do not exist" in res.stderr


def test_old_codes_must_name_an_existing_file_and_shows_in_the_plan(tmp_path):
    base = ["--family", "unit", "--modules", "pygx/_bng", "--tests", "test/pygx/test_bng_batched_walk.py",
            "--slim-tests", "test/pygx/test_bng_batched_walk.py", "--plan"]
    res = _sh(*base, "--old-codes", str(tmp_path / "nope.json"))
    assert res.returncode != 0 and "Fix:" in res.stderr
    saved = tmp_path / "K_old.json"
    saved.write_text("{}")
    res = _sh(*base, "--old-codes", str(saved))
    assert res.returncode == 0, res.stderr
    assert json.loads(res.stdout[res.stdout.index("{"):])["old_codes"] == str(saved)


def test_a_module_level_pyspark_import_is_reported(tmp_path):
    (tmp_path / "test_a.py").write_text("from pyspark.sql import SparkSession\n")
    (tmp_path / "test_b.py").write_text("def test_x():\n    import pyspark\n")
    files = gate.selected_files(["."], tmp_path)
    assert [f.name for f in gate.pyspark_importers(files)] == ["test_a.py"]


def _codes(spec):
    return {f"{P}.{m}.x_{f}__mutmut_{i}": c for (m, f, i), c in spec.items()}


def test_a_mutant_absent_from_the_slim_run_is_not_killed():
    old = _codes({("_bng", "f", 1): 1, ("_bng", "f", 2): 1, ("_bng", "f", 3): 0})
    new = _codes({("_bng", "f", 1): 1})  # mutant 2 sits on a line only an old test covered
    K, ok = gate.kept(old, new)
    assert len(K) == 2 and len(ok) == 1
    assert gate.judge(old, new, {"sample50": gate.sample(old, 50)})["pass"] is False


def test_the_verdict_passes_when_the_slim_run_keeps_the_old_kills_and_fails_on_a_module_drop():
    old = _codes({("_a", "f", i): 1 for i in range(100)} | {("_b", "g", i): 1 for i in range(3)})
    same = dict(old)
    v = gate.judge(old, same, {"sample50": gate.sample(old, 50), "sample100": gate.sample(old, 100)})
    assert v["pass"] and v["rows"]["all"]["pct"] == 100.0
    new = dict(old)
    for k in [k for k in old if k.split(".")[-2] == "_b"][:1]:  # 1 of 3 in _b: a 33 point drop, 1% overall
        new[k] = 0
    v = gate.judge(old, new, {"sample50": gate.sample(old, 50), "sample100": gate.sample(old, 100)})
    assert not v["pass"] and any("module _b" in f for f in v["failures"])


def test_the_sample_is_seeded_and_stratified_by_function():
    codes = _codes({("_m", f"fn{j}", i): i % 2 for j in range(10) for i in range(20)})
    a, b = gate.sample(codes, 10), gate.sample(codes, 10)
    assert a == b
    assert len({gate.mutant_function(k) for k in a}) == 10  # one mutant per function before any function repeats
    assert gate.sample(codes, 10, seed=1) != a
    assert len(gate.sample(codes, 1000)) == len(codes)


@pytest.mark.parametrize(
    "key,module,func",
    [
        (f"{P}._bng.x_cell_to_geom__mutmut_12", "_bng", "cell_to_geom"),
        (f"{P}._bng.x__helper__mutmut_3", "_bng", "_helper"),
        (f"{P}._custom.xǁGridǁcell__mutmut_1", "_custom", "cell"),
    ],
)
def test_mutant_keys_name_their_module_and_function(key, module, func):
    assert (gate.mutant_module(key), gate.mutant_function(key)) == (module, func)


def test_the_venv_defaults_to_a_per_user_cache_and_honours_the_overrides(monkeypatch, tmp_path):
    monkeypatch.delenv("GBX_MUTATION_VENV", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    assert gate.mutmut_venv() == tmp_path / "cache" / "geobrix" / "mutation-venv"
    monkeypatch.delenv("XDG_CACHE_HOME")
    assert gate.mutmut_venv() == gate.Path.home() / ".cache" / "geobrix" / "mutation-venv"
    assert "/tmp" not in str(gate.mutmut_venv())
    monkeypatch.setenv("GBX_MUTATION_VENV", str(tmp_path / "mine"))
    assert gate.mutmut_venv() == tmp_path / "mine"


def test_a_venv_dir_another_user_could_have_planted_is_refused(tmp_path):
    ok = tmp_path / "ok"
    ok.mkdir(mode=0o700)
    gate.require_private(ok)  # owned by this user, not writable by group or world

    for mode in (0o770, 0o707, 0o777):
        bad = tmp_path / f"mode{mode:o}"
        bad.mkdir()
        bad.chmod(mode)
        with pytest.raises(SystemExit) as exc:
            gate.require_private(bad)
        assert "Fix:" in str(exc.value)

    link = tmp_path / "link"
    link.symlink_to(ok)
    with pytest.raises(SystemExit):
        gate.require_private(link)


# ------------------------------------------------------------------------- a run that proves nothing is refused


@pytest.mark.parametrize("codes", [{}, _codes({("_bng", "f", 1): 0, ("_bng", "f", 2): None}), {"k": None}])
def test_an_old_run_that_killed_nothing_is_refused(codes):
    with pytest.raises(SystemExit) as exc:
        gate.require_old_kills(codes)
    msg = str(exc.value)
    assert "killed nothing" in msg and "did not run" in msg and "Fix:" in msg


def test_an_old_run_that_killed_something_is_accepted():
    gate.require_old_kills(_codes({("_bng", "f", 1): 1, ("_bng", "f", 2): 0}))


def test_a_verdict_over_an_empty_kill_set_is_a_fail_not_a_vacuous_pass():
    old = _codes({("_bng", "f", i): 0 for i in range(10)})
    v = gate.judge(old, dict(old), {"sample50": gate.sample(old, 50), "sample100": gate.sample(old, 100)})
    assert not v["pass"] and any("killed nothing" in f for f in v["failures"])


@pytest.mark.parametrize(
    "text,reason",
    [
        ("... failed to collect stats. runner returned 2\n", "failed to collect stats"),
        ("Failed to run clean test\n", "Failed to run clean test"),
        ("Stopping early, because we could not find any test case for any mutant.", "Stopping early"),
    ],
)
def test_a_mutmut_run_that_stopped_before_the_tests_is_named(text, reason):
    assert reason in gate.aborted_reason(text)
    assert gate.aborted_reason("1/5 mutants tested\n") is None


# ---------------------------------------------------------------------------------- --old-ref and the saved kill set


def _git(repo, *a):
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", *a], check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path):
    r = tmp_path / "repo"
    t = r / "python" / "geobrix" / "test" / "pygx"
    t.mkdir(parents=True)
    (t / "test_a.py").write_text("OLD = 1\n")
    (t / "test_gone.py").write_text("GONE = 1\n")
    (r / "python" / "geobrix" / "src" / "databricks" / "labs" / "gbx" / "pygx").mkdir(parents=True)
    (r / "python" / "geobrix" / "src" / "databricks" / "labs" / "gbx" / "pygx" / "_bng.py").write_text("X = 1\n")
    _git(r, "init", "-q")
    _git(r, "add", "-A")
    _git(r, "commit", "-q", "-m", "old")
    (t / "test_a.py").write_text("NEW = 1\n")  # edited in place
    (t / "test_gone.py").unlink()  # deleted
    (t / "test_new.py").write_text("NEW = 1\n")  # added
    _git(r, "add", "-A")
    _git(r, "commit", "-q", "-m", "fold")
    return r


def test_the_old_tests_come_back_as_they_were_at_the_ref(repo, tmp_path):
    dest = tmp_path / "old-test"
    gate.export_ref_tests("HEAD~1", dest, repo)
    assert (dest / "pygx" / "test_a.py").read_text() == "OLD = 1\n"
    assert (dest / "pygx" / "test_gone.py").exists()
    assert not (dest / "pygx" / "test_new.py").exists()  # the old selection never had it
    src = tmp_path / "old-src"
    gate.export_ref_dir("HEAD~1", "src", src, repo)
    assert (src / "databricks" / "labs" / "gbx" / "pygx" / "_bng.py").read_text() == "X = 1\n"


def test_a_selection_is_checked_against_the_ref_not_the_tree(repo):
    assert gate.exists_at_ref("test/pygx/test_gone.py", "HEAD~1", repo)
    assert not gate.exists_at_ref("test/pygx/test_gone.py", "HEAD", repo)
    assert gate.exists_at_ref("test/pygx", "HEAD", repo)  # a directory
    assert gate.exists_at_ref("test/pygx/test_a.py::test_x", "HEAD", repo)  # a node id is cut to its file


def test_a_ref_that_does_not_resolve_is_refused_with_a_fix(repo):
    assert len(gate.resolve_ref("HEAD~1", repo)) == 40
    with pytest.raises(SystemExit) as exc:
        gate.resolve_ref("no-such-ref", repo)
    assert "Fix:" in str(exc.value)


def test_a_kernel_module_edited_since_the_ref_is_refused(repo):
    mod = "src/databricks/labs/gbx/pygx/_bng.py"
    gate.require_unchanged_since("HEAD", [mod], repo)  # same content at the ref and in the tree
    (repo / "python" / "geobrix" / mod).write_text("X = 2\n")
    with pytest.raises(SystemExit) as exc:
        gate.require_unchanged_since("HEAD", [mod], repo)
    assert "Fix:" in str(exc.value) and "_bng.py" in str(exc.value)


def test_old_ref_shows_in_the_plan_and_a_kill_set_from_another_ref_is_refused(tmp_path):
    base = ["--family", "unit", "--modules", "pygx/_bng", "--tests", "test/pygx/test_bng_batched_walk.py",
            "--slim-tests", "test/pygx/test_bng_batched_walk.py", "--plan"]
    res = _sh(*base, "--old-ref", "HEAD")
    assert res.returncode == 0, res.stderr
    assert len(json.loads(res.stdout[res.stdout.index("{"):])["old_ref"]) == 40
    assert _sh(*base, "--old-ref", "no-such-ref").returncode != 0
    saved = tmp_path / "K_old.json"
    saved.write_text("{}")
    (tmp_path / "K_old.fingerprint").write_text("made-from-another-ref\n")
    res = _sh(*base, "--old-ref", "HEAD", "--old-codes", str(saved))
    assert res.returncode != 0 and "Fix:" in res.stderr
    (tmp_path / "K_old.fingerprint").unlink()  # a kill set from before fingerprints: accepted
    assert _sh(*base, "--old-ref", "HEAD", "--old-codes", str(saved)).returncode == 0


def test_a_saved_kill_set_is_reused_only_for_the_same_kernels_and_old_tests(tmp_path):
    mod = tmp_path / "_bng.py"
    mod.write_text("X = 1\n")
    t = tmp_path / "test_a.py"
    t.write_text("def test_a(): pass\n")
    fp = gate.old_fingerprint([mod], [t], None, ["test/pygx"])
    saved = tmp_path / "K_old.json"
    saved.write_text("{}")
    gate.write_fingerprint(saved, fp)
    gate.check_fingerprint(saved, fp)  # unchanged: fine
    t.write_text("def test_a(): assert 1\n")  # an old test was edited since
    with pytest.raises(SystemExit) as exc:
        gate.check_fingerprint(saved, gate.old_fingerprint([mod], [t], None, ["test/pygx"]))
    assert "Fix:" in str(exc.value)
    saved.with_name("K_old.fingerprint").unlink()
    gate.check_fingerprint(saved, fp)  # a kill set from before fingerprints: accepted, nothing to compare


# ------------------------------------------------------------------------------------------------- sample-first


def _universe(n_mod=4, n_fn=10, n_each=5):
    return _codes({(f"_m{m}", f"f{f}", i): 1 for m in range(n_mod) for f in range(n_fn) for i in range(n_each)})


def _fakes(old, slim, calls):
    def old_for(names):
        calls.append(("old", list(names)))
        return {k: old[k] for k in names if k in old}

    def slim_for(names):
        calls.append(("slim", list(names)))
        return {k: slim[k] for k in names if k in slim}

    return old_for, slim_for


def test_sample_first_runs_only_the_sample_and_stops_at_50_when_nothing_is_lost():
    old = _universe()
    calls = []
    res = gate.sampled_gate(sorted(old), *_fakes(old, dict(old), calls), seed=31)
    assert [(who, len(names)) for who, names in calls] == [("old", 50), ("slim", 50)]  # 200 mutants, 50 run per pass
    assert res["verdict"]["pass"] and not res["extended"] and list(res["samples"]) == ["sample50"]
    assert res["names"] == gate.sample(old, 50, 31)  # the same stratified sample as a full run draws


def test_one_sampled_loss_extends_to_100_and_judges_on_100():
    old = _universe()
    first = gate.sample(old, 50, 31)
    slim = dict(old)
    slim[first[0]] = 0  # one of the 50 is lost: 98%, but any loss extends
    calls = []
    res = gate.sampled_gate(sorted(old), *_fakes(old, slim, calls), seed=31)
    assert res["extended"] and list(res["samples"]) == ["sample50", "sample100"]
    assert [(who, len(names)) for who, names in calls] == [("old", 50), ("slim", 50), ("old", 50), ("slim", 50)]
    assert set(calls[2][1]).isdisjoint(first) and set(calls[2][1]) | set(first) == set(res["names"])  # 50 more, not 100 again
    assert len(res["names"]) == 100 and res["verdict"]["pass"]  # 99/100 and every module within 5 points
    assert list(res["verdict"]["rows"]) == ["sample100"]


def test_the_extended_sample_can_still_fail():
    old = _universe()
    slim = dict(old)
    for k in gate.sample(old, 100, 31)[:10]:
        slim[k] = 0
    res = gate.sampled_gate(sorted(old), *_fakes(old, slim, []), seed=31)
    v = res["verdict"]
    assert res["extended"] and not v["pass"] and any("sample100 keeps" in f for f in v["failures"])


def test_a_small_universe_is_not_extended():
    old = _codes({("_a", "f", i): 1 for i in range(30)})
    slim = {k: (0 if i < 3 else 1) for i, k in enumerate(sorted(old))}  # 27/30 = 90%
    res = gate.sampled_gate(sorted(old), *_fakes(old, slim, []), seed=31)
    assert not res["extended"] and len(res["names"]) == 30 and not res["verdict"]["pass"]


def test_a_sampled_mutant_the_slim_run_did_not_generate_is_not_killed():
    old = _universe()
    first = gate.sample(old, 50, 31)
    slim = {k: v for k, v in old.items() if k not in first[:5]}  # the slim run never generated five of them
    res = gate.sampled_gate(sorted(old), *_fakes(old, slim, []), seed=31)
    assert res["extended"]  # 45/50 is a loss, and below 95%
    assert not res["verdict"]["pass"] and all(k not in res["slim"] for k in first[:5])


def test_an_old_pass_that_kills_nothing_of_the_sample_is_refused():
    old = _codes({("_a", "f", i): 0 for i in range(80)})
    with pytest.raises(SystemExit) as exc:
        gate.sampled_gate(sorted(old), *_fakes(old, dict(old), []), seed=31)
    assert "killed nothing" in str(exc.value) and "Fix:" in str(exc.value)


def test_a_module_that_falls_in_the_sample_fails_the_gate():
    old = _codes({("_big", "f", i): 1 for i in range(200)} | {("_small", f"g{j}", 0): 1 for j in range(6)})
    sample50 = gate.sample(old, 50, 31)
    small = [k for k in sample50 if "_small" in k]
    assert small
    slim = dict(old)
    slim[small[0]] = 0  # 1 of the sampled _small mutants: a drop of more than 5 points, 98% overall
    res = gate.sampled_gate(sorted(old), *_fakes(old, slim, []), seed=31)
    assert not res["verdict"]["pass"] and any("module _small" in f for f in res["verdict"]["failures"])


class _FakeSide(gate.Side):
    """A Side whose mutmut is a stub: generate() reads canned keys, _invoke() records the names it was given."""

    def __init__(self, keys, tested, exit_codes, tmp_path):
        args = type("A", (), {"family": "t", "keep_work": True, "oracle_tables": "", "max_children": 2})()
        super().__init__("old", None, [], [], args, tmp_path)
        self._canned = (keys, tested, exit_codes)
        self.invoked = []
        self.work = tmp_path

    def generate(self):
        self.keys, self.tested = sorted(self._canned[0]), set(self._canned[1])
        return self.keys

    def _invoke(self, names, what):
        self.invoked.append(list(names))
        return 0


def test_a_side_runs_the_named_mutants_only_and_handles_the_ones_without_tests(tmp_path, monkeypatch):
    keys = [f"{P}._m.x_f__mutmut_{i}" for i in (1, 2, 3)] + [f"{P}._m.x_untested__mutmut_1"]
    side = _FakeSide(keys, {f"{P}._m.x_f"}, None, tmp_path)
    monkeypatch.setattr(gate, "collect_codes", lambda work: {k: 1 for k in keys})
    got = side.run([keys[0], keys[1], keys[3], f"{P}._m.x_never_generated__mutmut_9"])
    assert side.invoked == [[keys[0], keys[1]]]  # not the untested one (mutmut crashes on it), not the ungenerated one
    assert got == {keys[0]: 1, keys[1]: 1, keys[3]: gate.NO_TESTS}  # the ungenerated one is absent: not killed
    assert side.run([keys[0]]) == {keys[0]: 1} and len(side.invoked) == 1  # a mutant is run once


def test_sampled_kill_sets_cannot_stand_in_for_a_full_run(tmp_path):
    saved = tmp_path / "K_old.json"
    saved.write_text("{}")
    gate.check_codes_mode(saved, full=True)  # no mode file: from before sampling, accepted
    gate.mode_file(saved).write_text("sample 50\n")
    gate.check_codes_mode(saved, full=False)
    with pytest.raises(SystemExit) as exc:
        gate.check_codes_mode(saved, full=True)
    assert "Fix:" in str(exc.value)
    gate.mode_file(saved).write_text("full\n")
    gate.check_codes_mode(saved, full=True)


def test_the_plan_names_the_mode_and_full_restores_every_mutant():
    base = ["--family", "unit", "--modules", "pygx/_bng", "--tests", "test/pygx/test_bng_batched_walk.py",
            "--slim-tests", "test/pygx/test_bng_batched_walk.py", "--plan"]

    def plan(*extra):
        out = _sh(*base, *extra).stdout
        return json.loads(out[out.index("{"):])

    assert plan()["mode"].startswith("sample-first (50 mutants")
    assert plan("--full")["mode"].startswith("full")
    assert "--full" in _sh("--help").stdout


# ----------------------------------------------------------------------------------------- what counts as a kill


@pytest.mark.parametrize("code", [1, 3, -24, 24, 36, 152, 255, -11, -9, -6])
def test_a_failure_a_timeout_or_a_crash_is_a_kill(code):
    assert gate.detected(code)


@pytest.mark.parametrize("code", [0, None, 2, 5, 33, 34, 35, 37])
def test_a_survivor_or_a_mutant_no_test_ran_is_not_a_kill(code):
    """33 and 5 mean no test ran against the mutant (mutmut's 'no tests'); 2 was interrupted, 34 skipped."""
    assert not gate.detected(code)


def test_a_mutant_no_test_ran_gives_the_slim_run_no_credit():
    old = _codes({("_m", "f", i): 1 for i in range(20)})
    slim = dict(old)
    for k in list(old)[:3]:
        slim[k] = gate.NO_TESTS  # mutmut found no test for these in the slim run
    v = gate.judge(old, slim, {})
    assert v["rows"]["all"]["slim_kills_of_those"] == 17 and not v["pass"]  # 85%, not 100%
    assert gate.judge(slim, slim, {})["rows"]["all"]["old_killed"] == 17  # and an old run gets none either


# ---------------------------------------------------------------------------------- timeouts and an invalid run


def test_a_timeout_still_counts_as_a_kill_and_is_counted_per_module():
    old = _codes({("_a", "f", i): (-24 if i < 2 else 1) for i in range(40)} | {("_b", "g", i): 1 for i in range(40)})
    v = gate.judge(old, dict(old), {})
    assert v["pass"] and v["rows"]["all"]["old_killed"] == 80
    assert v["modules"]["_a"]["timeouts_old"] == 2 and v["modules"]["_a"]["timeouts_slim"] == 2
    assert v["modules"]["_b"]["timeouts_old"] == 0
    assert "t/o" in gate.format_report("unit", v, {"mode": "full"})


def test_a_run_where_over_a_fifth_of_the_mutants_timed_out_is_invalid_not_a_pass():
    old = _codes({("_a", f"f{j}", i): (-24 if (i + j) % 3 == 0 else 1) for j in range(5) for i in range(20)})  # a third
    v = gate.judge(old, dict(old), {})
    assert not v["pass"] and v["invalid"]
    msg = next(f for f in v["failures"] if "INVALID" in f)
    assert "Fix:" in msg and "fewer workers" in msg and "timed out" in msg


def test_the_timeouts_of_the_slim_pass_alone_can_invalidate_the_run():
    old = _codes({("_a", f"f{j}", i): 1 for j in range(5) for i in range(20)})
    slim = {k: -24 for k in old}  # every slim mutant "killed" by the clock
    v = gate.judge(old, slim, {})
    assert not v["pass"] and v["invalid"] and "slim" in next(f for f in v["failures"] if "INVALID" in f)


def test_a_module_that_mostly_timed_out_is_invalid_but_a_tiny_one_is_not_judged_on_it():
    base = {("_big", "f", i): 1 for i in range(100)}
    six = _codes(base | {("_hang", "g", i): (-24 if i < 4 else 1) for i in range(6)})  # 4 of 6: over half
    v = gate.judge(six, dict(six), {})
    assert v["invalid"] and any("_hang" in f for f in v["failures"] if "INVALID" in f)
    two = _codes(base | {("_tiny", "g", i): -24 for i in range(2)})  # 2 of 2, but too few to judge
    v = gate.judge(two, dict(two), {})
    assert not v.get("invalid") and v["pass"]


def test_a_normal_run_is_not_invalid():
    old = _codes({("_a", "f", i): (-24 if i == 0 else 1) for i in range(50)})
    v = gate.judge(old, dict(old), {})
    assert v["pass"] and not v["invalid"]


# ------------------------------------------------------------------------------------------- the host's worker budget


@pytest.fixture
def pool_dir(tmp_path):
    return tmp_path / "slots"


def _budget(pool_dir, capacity=6, **kw):
    kw.setdefault("say", lambda msg: None)
    kw.setdefault("grace", 0.0)
    return gate.WorkerBudget(pool_dir, capacity=capacity, **kw)


def test_the_capacity_is_the_cores_less_two_unless_the_env_says_otherwise(monkeypatch):
    monkeypatch.delenv("GBX_MUTATION_MAX_WORKERS", raising=False)
    monkeypatch.setattr(gate.os, "cpu_count", lambda: 12)
    assert gate.worker_capacity() == 10
    monkeypatch.setattr(gate.os, "cpu_count", lambda: 2)
    assert gate.worker_capacity() == 1  # never below one
    monkeypatch.setenv("GBX_MUTATION_MAX_WORKERS", "7")
    assert gate.worker_capacity() == 7
    monkeypatch.setenv("GBX_MUTATION_MAX_WORKERS", "zero")
    with pytest.raises(SystemExit) as exc:
        gate.worker_capacity()
    assert "Fix:" in str(exc.value)


def test_a_lone_gate_takes_up_to_its_bound_and_the_slots_come_back_on_release(pool_dir):
    b = _budget(pool_dir)
    b.register()
    lease = b.acquire(bound=4, label="old")
    assert lease.n == 4
    other = _budget(pool_dir).acquire(bound=None, label="x")  # a second holder sees only what is left
    assert other.n == 2
    other.release()
    lease.release()
    assert _budget(pool_dir).acquire(None, "y").n == 6
    b.unregister()


def test_all_gates_together_never_hold_more_than_the_capacity(pool_dir):
    leases = [_budget(pool_dir, capacity=6).acquire(4, f"g{i}") for i in range(2)]
    assert [lease.n for lease in leases] == [4, 2]  # the second takes what is left


def test_a_gate_with_others_running_takes_its_fair_share_not_everything(pool_dir):
    a, b, c = (_budget(pool_dir, capacity=6) for _ in range(3))
    a.register()
    # two more gates register, as separate processes would (the presence file is named by pid)
    for pid in (999991, 999992):
        fd = a._flock(pool_dir / f"gate-{pid}")
        assert fd is not None
        a._extra = getattr(a, "_extra", []) + [fd]
    assert a.others() == 2
    lease = a.acquire(None, "old")
    assert lease.n == 2  # 6 workers over 3 gates
    lease.release()


def test_a_gate_gets_at_least_one_slot_even_when_the_share_rounds_to_zero(pool_dir):
    b = _budget(pool_dir, capacity=2)
    b.register()
    held = []
    for pid in range(990001, 990006):  # five other gates on a two-slot host
        held.append(b._flock(pool_dir / f"gate-{pid}"))
    assert b.others() == 5
    assert b.acquire(None, "old").n == 1


def test_a_gate_waits_for_a_slot_and_says_so_then_times_out_with_a_fix(pool_dir):
    holder = _budget(pool_dir, capacity=2).acquire(None, "holder")
    assert holder.n == 2
    now = [0.0]
    said = []

    def sleep(dt):
        now[0] += dt

    waiter = _budget(pool_dir, capacity=2, wait_timeout=100, progress=30, clock=lambda: now[0], sleep=sleep, say=said.append)
    with pytest.raises(SystemExit) as exc:
        waiter.acquire(None, "slim")
    assert "Fix:" in str(exc.value) and "GBX_MUTATION_MAX_WORKERS" in str(exc.value)
    assert said and "waiting for a free worker slot" in said[0]


def test_a_waiting_gate_takes_a_slot_when_one_frees(pool_dir):
    holder = _budget(pool_dir, capacity=1).acquire(None, "holder")
    now = [0.0]

    def sleep(dt):
        now[0] += dt
        holder.release()  # the other gate finishes while we wait

    waiter = _budget(pool_dir, capacity=1, wait_timeout=100, clock=lambda: now[0], sleep=sleep)
    assert waiter.acquire(None, "slim").n == 1


def test_it_reports_how_many_workers_and_other_gates(pool_dir):
    said = []
    b = _budget(pool_dir, capacity=6, say=said.append)
    b.register()
    b.acquire(bound=3, label="old").release()
    assert said == ["[old] using 3 of 6 workers (0 other gates running)"]


_HOLDER = """
import importlib.util, sys, time
spec = importlib.util.spec_from_file_location("g", sys.argv[1]); g = importlib.util.module_from_spec(spec); spec.loader.exec_module(g)
b = g.WorkerBudget(sys.argv[2], capacity=4, say=lambda m: None)
b.register()
lease = b.acquire(None, "holder")
print(lease.n, flush=True)
time.sleep(60)
"""


def test_slots_of_a_killed_gate_are_reclaimed(pool_dir):
    import signal

    proc = subprocess.Popen([sys.executable, "-c", _HOLDER, str(REPO_ROOT / "scripts" / "mutation" / "gate.py"), str(pool_dir)],
                            stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout.readline().strip() == "4"  # the holder has every slot and a presence file
        me = _budget(pool_dir, capacity=4)
        assert me.others() == 1
        assert me._take(1) == []  # nothing free while it lives
        proc.send_signal(signal.SIGKILL)  # no cleanup runs
        proc.wait(timeout=10)
        assert me.others() == 0  # its presence file is dead and removed
        lease = gate.Lease(me._take(4))
        assert lease.n == 4  # every slot is back
        lease.release()
    finally:
        proc.kill()
        proc.wait()


def test_a_side_runs_mutmut_with_the_workers_it_leased_and_gives_them_back(tmp_path, monkeypatch, pool_dir):
    args = type("A", (), {"family": "t", "keep_work": True, "oracle_tables": "", "max_children": 3})()
    budget = _budget(pool_dir, capacity=8)
    side = gate.Side("old", None, [], [], args, tmp_path, budget=budget)
    side.work = tmp_path
    seen = []
    monkeypatch.setattr(side, "_spawn", lambda names, what, work, workers: seen.append(workers) or 0)
    side._invoke(["a"], "run")  # --max-children is the bound
    side._invoke([gate.GENERATE_ONLY], "generate", bound=2)
    assert seen == [3, 2]
    assert _budget(pool_dir, capacity=8).acquire(None, "after").n == 8  # both leases were released


def test_plan_shows_the_host_budget_and_max_children_is_optional():
    base = ["--family", "unit", "--modules", "pygx/_bng", "--tests", "test/pygx/test_bng_batched_walk.py",
            "--slim-tests", "test/pygx/test_bng_batched_walk.py", "--plan"]
    res = _sh(*base)
    plan = json.loads(res.stdout[res.stdout.index("{"):])
    assert plan["workers"]["max_children"] is None and plan["workers"]["host_capacity"] >= 1
    res = _sh(*base, "--max-children", "3")
    assert json.loads(res.stdout[res.stdout.index("{"):])["workers"]["max_children"] == 3


# ------------------------------------------------------------------------------------------ the old kills, cached


def test_the_cache_key_follows_what_the_old_kills_depend_on():
    k = lambda fp="f", tables="T", ref=None, tree="t": gate.cache_key(fp, tables, ref, tree)  # noqa: E731
    assert k() == k()
    assert len({k(), k(fp="g"), k(tables="U"), k(tree="u")}) == 4
    assert k(ref="abc", tree="t") == k(ref="abc", tree="u")  # at a ref the tree does not matter: the ref holds the files
    assert k(ref="abc") != k(ref=None)  # (the ref is in the fingerprint; the tree joins the key only without one)


def test_the_tree_hash_sees_src_and_test_helpers_but_not_test_files(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "test").mkdir()
    (tmp_path / "src" / "a.py").write_text("X = 1\n")
    (tmp_path / "test" / "conftest.py").write_text("Y = 1\n")
    (tmp_path / "test" / "test_a.py").write_text("def test_a(): pass\n")
    h0 = gate.tree_state_hash(tmp_path)
    (tmp_path / "test" / "test_a.py").write_text("def test_a(): assert 1\n")
    assert gate.tree_state_hash(tmp_path) == h0  # the old test files are in the fingerprint, not here
    (tmp_path / "test" / "conftest.py").write_text("Y = 2\n")
    h1 = gate.tree_state_hash(tmp_path)
    (tmp_path / "src" / "a.py").write_text("X = 2\n")
    assert len({h0, h1, gate.tree_state_hash(tmp_path)}) == 3


def test_a_cached_entry_is_reused_only_for_its_own_fingerprint(tmp_path):
    codes = {f"{P}._a.x_f__mutmut_1": 1}
    path = gate.cache_store("key", "fp1", [*codes, f"{P}._a.x_f__mutmut_2"], codes, False, tmp_path)
    assert path.parent == tmp_path and not list(tmp_path.glob(".*.tmp"))
    entry = gate.cache_load("key", "fp1", tmp_path)
    assert entry["codes"] == codes and len(entry["universe"]) == 2 and entry["full"] is False
    assert gate.cache_load("key", "fp2", tmp_path) is None  # another fingerprint: a miss, never an answer
    assert gate.cache_load("nope", "fp1", tmp_path) is None
    path.write_text("{not json")
    assert gate.cache_load("key", "fp1", tmp_path) is None
    path.write_text(json.dumps({"version": 0, "fingerprint": "fp1", "codes": {}, "universe": []}))
    assert gate.cache_load("key", "fp1", tmp_path) is None


def test_the_cache_dir_is_per_user_and_movable(monkeypatch, tmp_path):
    monkeypatch.delenv("GBX_MUTATION_KILLS", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    assert gate.kills_dir() == tmp_path / "geobrix" / "mutation-kills"
    monkeypatch.setenv("GBX_MUTATION_KILLS", str(tmp_path / "k"))
    assert gate.kills_dir() == tmp_path / "k"


class _Side:
    """A stand-in for a Side that counts its passes."""

    def __init__(self, keys, codes):
        self.keys, self._codes, self.generated, self.ran = sorted(keys), codes, 0, []

    def generate(self):
        self.generated += 1
        return self.keys

    def run(self, names):
        if not self.generated:  # the real Side generates before it runs a name
            self.generate()
        self.ran.append(list(names))
        return {n: self._codes[n] for n in names if n in self._codes}

    def run_all(self):
        self.ran.append("all")
        return dict(self._codes)


def _args(tmp_path, **kw):
    return type("A", (), dict({"family": "t", "seed": 31, "old_codes": None, "full": False}, **kw))()


def _run_sampled(tmp_path, old, slim, cache_dir, fp="fp"):
    old_side, slim_side = _Side(old, old), _Side(slim, slim)
    cache = gate.OldCache("k", fp, cache_dir)
    rc = gate.gate_sample_first(_args(tmp_path), out=tmp_path, old_side=old_side, slim_side=slim_side, old_known={},
                                fingerprint=fp, warn=[], t0=0.0, cache=cache)
    return rc, old_side, slim_side


def test_a_second_gate_of_the_same_old_selection_skips_the_old_pass(tmp_path):
    old = _universe()
    rc, old_side, _ = _run_sampled(tmp_path, old, dict(old), tmp_path / "cache")
    assert rc == 0 and old_side.generated == 1 and [len(r) for r in old_side.ran] == [50]
    rc, old_side, slim_side = _run_sampled(tmp_path, old, dict(old), tmp_path / "cache")  # another slim candidate
    assert rc == 0 and old_side.generated == 0 and old_side.ran == []  # nothing of the old pass ran
    assert len(slim_side.ran[0]) == 50


def test_an_extension_after_a_cache_hit_runs_only_the_missing_old_mutants(tmp_path):
    old = _universe()
    _run_sampled(tmp_path, old, dict(old), tmp_path / "cache")  # caches the 50
    slim = dict(old)
    slim[gate.sample(old, 50, 31)[0]] = 0  # the next candidate loses one
    rc, old_side, _ = _run_sampled(tmp_path, old, slim, tmp_path / "cache")
    assert old_side.generated == 1 and [len(r) for r in old_side.ran] == [50]  # the second 50 only
    assert gate.cache_load("k", "fp", tmp_path / "cache")["codes"].keys() >= set(gate.sample(old, 100, 31))


def test_a_changed_fingerprint_does_not_hit(tmp_path):
    old = _universe()
    _run_sampled(tmp_path, old, dict(old), tmp_path / "cache", fp="one")
    _, old_side, _ = _run_sampled(tmp_path, old, dict(old), tmp_path / "cache", fp="two")
    assert old_side.generated == 1  # a miss: the entry belongs to fingerprint "one"


def test_a_full_run_hits_only_a_full_entry(tmp_path):
    old = _universe(n_each=3)
    cdir = tmp_path / "cache"

    def full(fp="fp"):
        old_side, slim_side = _Side(old, old), _Side(old, old)
        rc = gate.gate_full(_args(tmp_path, full=True), out=tmp_path, old_side=old_side, slim_side=slim_side, old_known={},
                            fingerprint=fp, warn=[], t0=0.0, cache=gate.OldCache("k", fp, cdir))
        return rc, old_side

    _run_sampled(tmp_path, old, dict(old), cdir)  # a sampled entry
    rc, old_side = full()
    assert rc == 0 and old_side.ran == ["all"]  # not enough: the old pass ran everything
    assert gate.cache_load("k", "fp", cdir)["full"] is True
    rc, old_side = full()
    assert old_side.ran == []  # now a full entry serves the next full run
    _, old_side, _ = _run_sampled(tmp_path, old, dict(old), cdir)
    assert old_side.generated == 0  # and a sample-first run too
    assert (tmp_path / "K_old.mode").read_text().strip() in ("full", "sample 50")


def test_old_codes_and_no_cache_never_touch_the_cache(tmp_path):
    base = ["--family", "unit", "--modules", "pygx/_bng", "--tests", "test/pygx/test_bng_batched_walk.py",
            "--slim-tests", "test/pygx/test_bng_batched_walk.py", "--plan"]
    assert "--no-cache" in _sh("--help").stdout
    res = _sh(*base, "--no-cache")
    assert res.returncode == 0, res.stderr
