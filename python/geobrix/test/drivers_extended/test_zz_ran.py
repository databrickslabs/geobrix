"""Every case a registry entry claims ran and passed in this session: not skipped, not expected to fail, not failed.

The registry (``_tested.py``) lists a driver for a kind when the cases of that driver pass, and ``test_registry.py``
checks the listing against the case data. This test checks the cases themselves, so that a listing never rests on a
test that was skipped (no payload) or marked an expected failure. It runs last (its file name sorts last), reads the
outcomes the other tests recorded, and skips when the session did not run those tests (a selection, or no payload).
"""

from test.drivers_extended import _raster as R
from test.drivers_extended import _vector as V
from test.drivers_extended import test_raster_write as raster_write
from test.drivers_extended import test_registry as reg
from test.drivers_extended.conftest import OUTCOMES

import pytest

from databricks.labs.gbx.drivers._tested import TESTED_DRIVERS

READ = "test_raster_read.py::"
RWRITE = "test_raster_write.py::"
SPECIAL = "test_raster_special.py::"
VREAD = "test_vector_read.py::"
VWRITE = "test_vector_write.py::"


def _find(suffixes):
    """The recorded outcomes of the tests whose node id ends with one of ``suffixes`` (module::name[param])."""
    return {
        node: out
        for node, out in OUTCOMES.items()
        if any(node.endswith(sfx) for sfx in suffixes)
    }


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


def test_every_case_a_registry_entry_claims_ran_and_passed():
    if not OUTCOMES or all(o == "skipped" for o in OUTCOMES.values()):
        pytest.skip("the driver tests did not run in this session (no GDAL payload)")
    not_passed, missing = [], []
    for driver, kinds in TESTED_DRIVERS.items():
        for kind, entry in kinds.items():
            for sfx in _required(driver, kind, entry):
                hit = _find([sfx])
                if not hit:
                    missing.append(f"{driver} ({kind}): {sfx}")
                elif any(o != "passed" for o in hit.values()):
                    not_passed.append(
                        f"{driver} ({kind}): {sfx} -> {sorted(set(hit.values()))}"
                    )
    assert not not_passed, "claimed cases that did not pass:\n" + "\n".join(not_passed)
    if missing:  # a selection (-k, one file) did not run them: nothing to judge
        pytest.skip(f"{len(missing)} claimed cases were not selected in this session")
