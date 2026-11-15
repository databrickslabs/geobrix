"""Host tests for the batched warehouse runner of the UC gate: examples/tests/uc_sql.py and the `-- batch` helpers.

The warehouse pays a fixed cost per statement, so value cases share one statement (one row, one column per case).
Nothing here calls a workspace: the two functions that talk to the Statement API are replaced by a fake warehouse
that answers every statement from the oracle's own expected values.
"""

import re
import tempfile
import threading

import pytest

from conftest import REPO_ROOT

TYPES = {"DOUBLE": float, "BIGINT": int, "STRING": str}


@pytest.fixture(scope="module")
def uc_sql():
    import importlib
    import sys

    sys.path.insert(0, str(REPO_ROOT / "examples" / "tests"))
    try:
        return importlib.import_module("uc_sql")
    finally:
        sys.path.remove(str(REPO_ROOT / "examples" / "tests"))


@pytest.fixture(scope="module")
def oracles():
    from databricks.labs.gbx.bench import oracles

    return oracles


@pytest.fixture(scope="module")
def core_cases():
    from databricks.labs.gbx.bench import probes

    return [c for c in probes.uc_slice_cases(probes.UC_CORE_TABLES) if "uc" in c.surfaces]


@pytest.fixture(scope="module")
def core_pairs(oracles, core_cases):
    with tempfile.TemporaryDirectory() as d:
        return oracles.uc_sql(core_cases, "cat", "sch", d)


# ---- which cases may share a statement ----------------------------------------------------------------------------


def test_refusals_and_recipes_never_batch_and_every_value_case_does(oracles, core_cases, core_pairs):
    batchable = oracles.uc_batchable(core_cases)
    refusals = {c.cid for c in core_cases if c.kind in ("raises", "uc_refused")}
    recipes = {c.cid for c in core_cases if c.sql is not None}
    assert refusals and not (refusals & batchable) and not (recipes & batchable)
    assert batchable == {c.cid for c in core_cases} - refusals - recipes
    # every batchable case has the one-value form: none silently stays a statement of its own
    stmt = dict(core_pairs)
    assert [cid for cid in batchable if oracles.uc_batch_item(stmt[cid]) is None] == []


def test_batch_item_forms_and_statement(oracles, core_cases, core_pairs):
    assert oracles.uc_batch_item("SELECT `c`.`s`.f(1, 'x') AS out") == "`c`.`s`.f(1, 'x')"
    table = "SELECT to_json(collect_list(named_struct('a', t.a))) AS out FROM `c`.`s`.g(1) AS t"
    assert oracles.uc_batch_item(table) == f"({table})"
    assert oracles.uc_batch_item("WITH x AS (SELECT 1) SELECT x AS out FROM x") is None
    assert oracles.uc_batch_statement(["f(1)", "(SELECT 2)"]) == "SELECT f(1) AS c0, (SELECT 2) AS c1"
    generators = [c.cid for c in core_cases if c.kind in ("cell_rows", "no_rows")]
    assert any(oracles.uc_batch_item(dict(core_pairs)[g]).startswith("(SELECT to_json") for g in generators)


def test_the_batch_marker_round_trips_and_does_not_change_the_statements(oracles):
    pairs = [("a", "SELECT 1 AS out"), ("b", "SELECT 2 AS out"), ("c", "SELECT 3 AS out")]
    text = oracles.uc_sql_text(pairs, {"a", "c"})
    assert oracles.parse_uc_batchable(text) == {"a", "c"}
    assert oracles.parse_uc_sql(text) == pairs
    assert oracles.parse_uc_batchable(oracles.uc_sql_text(pairs)) == set()
    assert oracles.uc_sql_text(pairs) == oracles.uc_sql_text(pairs, None)


# ---- the plan -------------------------------------------------------------------------------------------------------


def test_the_plan_covers_every_case_once_keeps_refusals_single_and_cuts_the_statement_count(uc_sql, oracles, core_cases, core_pairs):
    batchable = oracles.uc_batchable(core_cases)
    units = uc_sql.plan_units(core_pairs, batchable, 25)
    cids = [cid for u in units for cid, _ in u.members]
    assert sorted(cids) == sorted(cid for cid, _ in core_pairs)  # the same case set as the unbatched plan
    assert all(len(u.members) <= 25 for u in units)
    singles = {u.members[0][0] for u in units if not u.is_batch}
    assert {c.cid for c in core_cases if c.kind in ("raises", "uc_refused")} <= singles
    assert all(cid in batchable for u in units if u.is_batch for cid, _ in u.members)
    assert all(len(u.members) == len(u.items) for u in units if u.is_batch)
    assert len(units) < len(core_pairs) * 0.5


def test_a_size_below_two_plans_every_statement_alone(uc_sql, oracles, core_cases, core_pairs):
    for size in (0, 1):
        units = uc_sql.plan_units(core_pairs, oracles.uc_batchable(core_cases), size)
        assert [u.members for u in units] == [[p] for p in core_pairs]
    assert all(not u.is_batch for u in uc_sql.plan_units(core_pairs, (), 25))


def test_a_lone_batchable_statement_is_not_a_batch(uc_sql):
    pairs = [("a", "SELECT f(1) AS out"), ("r", "SELECT bad() AS out"), ("b", "SELECT f(2) AS out")]
    units = uc_sql.plan_units(pairs, {"a", "b"}, 25)
    assert [(u.is_batch, [c for c, _ in u.members]) for u in units] == [(False, ["r"]), (True, ["a", "b"])]
    assert not uc_sql.plan_units(pairs[:1], {"a"}, 25)[0].is_batch


# ---- a fake warehouse -------------------------------------------------------------------------------------------------


class FakeWarehouse:
    """Answers statements the way the oracle expects, except for the cids in `wrong` (a different value) and `broken`."""

    def __init__(self, cases, pairs, man, wrong=(), broken=()):
        self.cases = {c.cid: c for c in cases}
        self.stmt_cid = {}
        for cid, stmt in pairs:
            self.stmt_cid.setdefault(stmt, cid)
        self.item_cid = {}
        self.man = man
        self.wrong, self.broken = set(wrong), set(broken)
        self.calls = []
        self.lock = threading.Lock()

    def answer(self, cid):
        """(error, cell, type) for a case."""
        case = self.cases[cid]
        if cid in self.broken:
            return "UNRESOLVED_ROUTINE: broken", None, None
        if case.kind == "raises":
            wanted = case.expect if isinstance(case.expect, tuple) else (case.expect,)
            return f"boom {wanted[0]}", None, None
        if case.kind == "uc_refused":
            return "refused. Fix: do it differently", None, None
        t = self.man.functions[case.fn].returns_uc
        if case.kind == "exact" and t in TYPES:
            value = case.expect + 1 if cid in self.wrong else case.expect
            return None, str(value), t
        return None, None, "STRING"  # anything else: a NULL cell (the null cases pass, the rest fail alike on both paths)

    def run_once(self, w, statement, warehouse_id, catalog, schema, timeout_s=0, info=None):
        cid = self.stmt_cid[statement]
        with self.lock:
            self.calls.append(("single", cid))
        error, cell, t = self.answer(cid)
        return None, error, cell, t

    def run_batch_once(self, w, statement, warehouse_id, catalog, schema, timeout_s=0):
        rest, items, i = statement[len("SELECT ") :], [], 0
        while rest:
            m = re.search(rf" AS c{i}(, |$)", rest)
            items.append(rest[: m.start()])
            rest = rest[m.end() :]
            i += 1
        cids = [self.item_cid[item] for item in items]
        with self.lock:
            self.calls.append(("batch", tuple(cids)))
        answers = [self.answer(cid) for cid in cids]
        bad = [cid for cid, a in zip(cids, answers) if a[0]]
        if bad:
            return answers[cids.index(bad[0])][0], []
        return None, [(cell, t) for _, cell, t in answers]


@pytest.fixture
def world(uc_sql, oracles, core_cases, core_pairs, monkeypatch):
    """A fake warehouse over the value cases of the core slice plus a few refusals, and a factory for runner args."""
    from argparse import Namespace

    from databricks.labs.gbx import manifest

    man = manifest.load()
    values = [c for c in core_cases if c.kind == "exact" and c.sql is None and man.functions[c.fn].returns_uc in TYPES][:12]
    refusals = [c for c in core_cases if c.kind in ("raises", "uc_refused")][:4]
    nulls = [c for c in core_cases if c.kind == "null"][:6]
    cases = values + refusals + nulls
    ids = {c.cid for c in cases}
    pairs = [p for p in core_pairs if p[0] in ids]
    assert len(values) >= 8 and len({man.functions[c.fn].returns_uc for c in values}) >= 2

    def make(**kw):
        fake = FakeWarehouse(cases, pairs, man, **kw)
        for cid, stmt in pairs:
            item = oracles.uc_batch_item(stmt)
            if item is not None:
                fake.item_cid.setdefault(item, cid)
        monkeypatch.setattr(uc_sql, "run_once", fake.run_once)
        monkeypatch.setattr(uc_sql, "run_batch_once", fake.run_batch_once)
        return fake

    def args(**kw):
        base = dict(warehouse_id="w", catalog=None, schema=None, repeat=1, timeout_s=5.0, decode_binary=True, out=None,
                    batch=25, concurrency=1, batchable=oracles.uc_batchable(cases))
        return Namespace(**{**base, **kw})

    def judge(results):
        with tempfile.TemporaryDirectory() as d:
            return oracles.check_uc_results(cases, results, d)

    return Namespace(cases=cases, pairs=pairs, make=make, args=args, judge=judge)


# ---- the runner -----------------------------------------------------------------------------------------------------------


def test_batch_rows_map_back_to_their_cases_and_are_judged_individually(uc_sql, world):
    fake = world.make()
    results = uc_sql.run_statements(None, world.pairs, world.args())
    assert list(results) == [cid for cid, _ in world.pairs]
    batches = [c for kind, c in fake.calls if kind == "batch"]
    assert batches and sum(len(b) for b in batches) >= 8
    assert world.judge(results) == []
    # the same values as the unbatched run, case for case
    fake2 = world.make()
    alone = uc_sql.run_statements(None, world.pairs, world.args(batch=0))
    assert all(k == "single" for k, _ in fake2.calls)
    assert {c: r.get("value", r.get("error")) for c, r in results.items()} == {c: r.get("value", r.get("error")) for c, r in alone.items()}


def test_fail_on_revert_a_wrong_value_in_a_batch_names_its_case_not_a_neighbour(uc_sql, world):
    victim = next(c.cid for c in world.cases if c.kind == "exact")
    world.make(wrong={victim})
    results = uc_sql.run_statements(None, world.pairs, world.args())
    failures = world.judge(results)
    assert [f.split(":")[0] for f in failures] == [victim]
    # a mapping that shifted the columns (or reversed them) would fail other cases, or none: pin the position
    unit = next(u for u in uc_sql.plan_units(world.pairs, world.args().batchable, 25) if victim in [c for c, _ in u.members])
    cells = [(str(i), "BIGINT") for i in range(len(unit.members))]
    entries = uc_sql.batch_entries(unit, cells, 1.0, True)
    assert [entries[c]["value"] for c, _ in unit.members] == list(range(len(unit.members)))


def test_a_failing_batch_falls_back_to_single_runs_and_names_the_broken_case(uc_sql, world):
    victim = next(c.cid for c in world.cases if c.kind == "exact")
    fake = world.make(broken={victim})
    results = uc_sql.run_statements(None, world.pairs, world.args())
    assert set(results) == {cid for cid, _ in world.pairs}  # no case dropped
    assert "error" in results[victim] and all("error" not in r for c, r in results.items() if c != victim and c not in {x.cid for x in world.cases if x.kind in ("raises", "uc_refused")})
    assert [f.split(":")[0] for f in world.judge(results)] == [victim]
    singles = [c for k, c in fake.calls if k == "single"]
    assert victim in singles  # the members of the failed batch ran one by one


def test_a_batch_timeout_also_falls_back(uc_sql, world, monkeypatch):
    fake = world.make()

    def hung(*a, **k):
        raise uc_sql.StatementTimeout("still RUNNING")

    monkeypatch.setattr(uc_sql, "run_batch_once", hung)
    results = uc_sql.run_statements(None, world.pairs, world.args())
    assert world.judge(results) == [] and {k for k, _ in fake.calls} == {"single"}


def test_concurrent_runs_return_every_case_in_plan_order(uc_sql, world, tmp_path, oracles):
    world.make()
    out = tmp_path / "r.json"
    results = uc_sql.run_statements(None, world.pairs, world.args(concurrency=8, batch=3, out=str(out)))
    assert list(results) == [cid for cid, _ in world.pairs]
    assert world.judge(results) == []
    assert set(oracles.load_uc_results(str(out))) == set(results)  # the file holds every case too


def test_batch_off_by_default_in_the_command_line(uc_sql):
    a = uc_sql.parse_args(["--file", "x.sql"])
    assert a.batch == 0 and a.concurrency == 1
    with pytest.raises(SystemExit):
        uc_sql.parse_args(["--file", "x.sql", "--concurrency", "0"])


def test_example_files_run_side_by_side_after_the_setup_file(uc_sql, monkeypatch, tmp_path):
    import importlib
    import sys
    import types

    sys.path.insert(0, str(REPO_ROOT / "examples" / "tests"))
    try:
        ex = importlib.import_module("uc_examples_sql")
    finally:
        sys.path.remove(str(REPO_ROOT / "examples" / "tests"))
    d = tmp_path / "ex"
    d.mkdir()
    for name in ("00_setup.sql", "01_a.sql", "02_b.sql", "03_c.sql"):
        (d / name).write_text(f"-- cid: {name[:2]}\nSELECT 1;\n")
    started, finished, lock, running, peak = [], [], threading.Lock(), [0], [0]

    def fake_run(w, pairs, args):
        import time

        with lock:
            started.append(pairs[0][0])
            running[0] += 1
            peak[0] = max(peak[0], running[0])
        time.sleep(0.05)
        with lock:
            running[0] -= 1
            finished.append(pairs[0][0])
        return {cid: {"value": 1, "wall_s": [0.1], "rows": 1, "first_row": [1]} for cid, _ in pairs}

    monkeypatch.setattr(uc_sql, "run_statements", fake_run)
    fake_sdk = types.SimpleNamespace(WorkspaceClient=lambda profile=None: types.SimpleNamespace(
            warehouses=types.SimpleNamespace(get=lambda wid: types.SimpleNamespace(state="RUNNING"))
        )
    )
    monkeypatch.setitem(sys.modules, "databricks.sdk", fake_sdk)
    argv = ["--profile", "p", "--warehouse-id", "w", "--dir", str(d), "--out-dir", str(tmp_path / "o")]
    assert ex.main(argv + ["--parallel", "3"]) == 0
    assert started[0] == "00" and finished[0] == "00" and sorted(started[1:]) == ["01", "02", "03"]
    assert peak[0] >= 2  # the three files after the setup overlapped
    started.clear(), finished.clear(), peak.__setitem__(0, 0)
    assert ex.main(argv) == 0 and peak[0] == 1  # default: one file at a time
