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
