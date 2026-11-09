"""Every case a registry entry claims ran and passed in this session: not skipped, not expected to fail, not failed.

The registry (``_tested.py``) lists a driver for a kind when the cases of that driver pass, and ``test_registry.py``
checks the listing against the case data. This test checks the cases themselves, so that a listing never rests on a
test that was skipped or marked an expected failure. It reads the outcomes the other tests recorded (``conftest.py``
puts it last whatever order a plugin gives, and hands over the state of the conftest pytest loaded, whatever the import
mode). In a full run of this directory a registered driver with no passing case FAILS, with the list. It skips only
where the run was a deliberate selection (a test id, ``-k``, ``-m``, ``--deselect`` or ``--lf`` that left a test of the
directory out, or a module not collected) or the GDAL payload is missing (the ``gb`` fixture says so itself).
"""

from test.drivers_extended import _raster as R
from test.drivers_extended import _vector as V
from test.drivers_extended import conftest as C
from test.drivers_extended import test_raster_write as raster_write
from test.drivers_extended import test_registry as reg

import pytest

from databricks.labs.gbx.drivers._tested import TESTED_DRIVERS

READ = "test_raster_read.py::"
RWRITE = "test_raster_write.py::"
SPECIAL = "test_raster_special.py::"
VREAD = "test_vector_read.py::"
VWRITE = "test_vector_write.py::"


def _required(driver: str, kind: str, entry: dict):
    """The node-id suffixes of the cases an entry claims."""
    if driver in reg.SPECIAL and kind == reg.SPECIAL[driver][0]:
        return [f"{SPECIAL}{n}" for n in reg.SPECIAL[driver][2]]
    out = []
    if kind == "raster":
        for c in R.BY_DRIVER[driver]:
            for p in R.read_paths(c):
                name = (
                    "test_the_reader_returns_the_file_as_gdal_reads_it"
                    if p.startswith("reader:")
                    else (
                        "test_rst_fromfile_on_the_path_returns_the_file_as_gdal_reads_it"
                        if p == "fromfile"
                        else "test_rst_fromcontent_follows_the_tile_bytes_flag_of_gbx_drivers"
                    )
                )
                out.append(f"{READ}{name}[{c.id}-{p}]")
        if entry["write"]:
            c = raster_write.write_case(driver)
            out.append(
                f"{RWRITE}test_rst_asformat_encodes_the_driver_as_gdal_does[{c.id}-asformat]"
            )
    else:
        c = V.BY_DRIVER[driver]
        for p in V.read_paths(c):
            out.append(
                f"{VREAD}test_the_reader_returns_the_features_as_pyogrio_reads_them[{c.id}-{p}]"
            )
        if entry["write"]:
            for n in (1, 2):
                out.append(
                    f"{VWRITE}test_the_writer_makes_a_dataset_that_pyogrio_reads_back[{c.id}-write{n}]"
                )
    return out


def judge(outcomes: dict, run: dict, selection: bool, drivers=None):
    """``(skip reason or None, not_passed, missing)`` for the recorded ``outcomes``: the logic of the test below, on data."""
    if run.get("payload_missing"):
        return f"no GDAL payload: {run['payload_missing']}", [], []
    if selection:
        return "a deliberate selection of the driver tests ran", [], []
    not_passed, missing = [], []
    for driver, kinds in (TESTED_DRIVERS if drivers is None else drivers).items():
        for kind, entry in kinds.items():
            for sfx in _required(driver, kind, entry):
                hit = {
                    node: out for node, out in outcomes.items() if node.endswith(sfx)
                }
                if not hit:
                    missing.append(f"{driver} ({kind}): {sfx}")
                elif any(o != "passed" for o in hit.values()):
                    not_passed.append(
                        f"{driver} ({kind}): {sfx} -> {sorted(set(hit.values()))}"
                    )
    return None, not_passed, missing


def test_every_case_a_registry_entry_claims_ran_and_passed(driver_run):
    reason, not_passed, missing = judge(
        C.OUTCOMES, driver_run, C.is_deliberate_selection()
    )
    if reason:
        pytest.skip(reason)
    assert not not_passed, "claimed cases that did not pass:\n" + "\n".join(not_passed)
    assert not missing, (
        f"{len(missing)} claimed cases did not run in a full run of the driver tests (a registered driver needs a "
        "passing case):\n" + "\n".join(missing)
    )


def test_a_full_run_fails_for_a_missing_case_and_a_selection_skips(monkeypatch):
    """The judgement on data: a claimed case that did not run fails a full run; a payload that is missing or a deliberate
    selection skips; a skipped or failed case is not a pass."""
    import types

    drv = {"X": {"vector": {"read": "vector_gbx", "write": None}}}
    monkeypatch.setattr(V, "BY_DRIVER", {"X": types.SimpleNamespace(id="X-c")})
    monkeypatch.setattr(V, "read_paths", lambda c: ["read"])
    want = (
        f"{VREAD}test_the_reader_returns_the_features_as_pyogrio_reads_them[X-c-read]"
    )
    reason, not_passed, missing = judge({}, {}, False, drv)
    assert reason is None and not_passed == [] and missing == [f"X (vector): {want}"]
    assert judge({"a/" + want: "passed"}, {}, False, drv) == (None, [], [])
    skipped = judge({"a/" + want: "skipped"}, {}, False, drv)
    assert skipped[0] is None and len(skipped[1]) == 1
    assert judge({}, {}, True, drv)[0].startswith("a deliberate selection")
    assert judge({}, {"payload_missing": "no osgeo"}, False, drv)[0].startswith(
        "no GDAL payload"
    )


def test_a_selection_is_told_from_a_full_run_by_what_was_left_out(monkeypatch):
    run = {"node_args": False, "deselected": 0, "collected_modules": C.all_modules()}
    monkeypatch.setattr(C, "RUN", run)
    assert C.is_deliberate_selection() is False
    for change in (
        {"node_args": True},
        {"deselected": 3},
        {"collected_modules": C.all_modules() - {"test_registry.py"}},
    ):
        monkeypatch.setattr(C, "RUN", {**run, **change})
        assert C.is_deliberate_selection() is True, change
