"""The registry of tested drivers (``databricks.labs.gbx.drivers._tested``) is the suite's cases and nothing else: a
driver is listed when every GeoBrix path its cases run passes, and a passing case lists its driver. A path that fails
today is a finding (an expected failure with its reason); its driver is not listed for that path.
"""

import json
from test.drivers_extended import _raster as R
from test.drivers_extended import _vector as V
from test.drivers_extended import test_raster_special as special
from test.drivers_extended import test_raster_write as raster_write

from databricks.labs.gbx.drivers import _listing
from databricks.labs.gbx.drivers._tested import TESTED_DRIVERS

# Drivers reached in their own way, with the tests that cover them (test_raster_special.py)
SPECIAL = {
    "VRT": (
        "raster",
        "raster_gbx",
        (
            "test_a_user_written_vrt_reads_with_raster_gbx",
            "test_a_user_written_vrt_reads_with_rst_fromfile",
            "test_the_sql_rst_fromfile_renders_a_vrt_into_a_tile_of_its_own",
            "test_the_bytes_of_a_vrt_are_refused_with_a_reason",
        ),
    ),
}


def _ordered(readers):
    """The named readers first, then the generic ones."""
    generic = ("raster_gbx", "vector_gbx")
    return sorted(dict.fromkeys(readers), key=lambda r: r in generic)


def derived() -> dict:
    """The registry the cases give: for each driver and kind, the GeoBrix entries whose paths all pass. A case with
    ``counts=False`` runs, but its driver is not listed (GeoBrix has its own reader and writer for the format).
    """
    out = {}

    def add(driver, kind, reads, write):
        out.setdefault(driver, {})[kind] = {
            "read": ", ".join(dict.fromkeys(reads)),
            "write": write,
        }

    for driver, cases in R.BY_DRIVER.items():
        if not all(c.counts for c in cases):
            continue
        if all(
            R.RASTER_FINDINGS.get((p, c.id)) is None
            for c in cases
            for p in R.read_paths(c)
        ):
            wrote = driver in raster_write.WRITE and all(
                R.RASTER_FINDINGS.get(("asformat", c.id)) is None
                for c in [raster_write.write_case(driver)]
            )
            add(
                driver,
                "raster",
                _ordered(r for c in cases for r in (c.reader, "raster_gbx")),
                "gbx_rst_asformat" if wrote else None,
            )
    for c in (c for c in V.VECTOR_CASES if c.counts):
        paths = V.read_paths(c) + (["folder"] if c.driver == "MVT" else [])
        if all(V.VECTOR_FINDINGS.get((p, c.id)) is None for p in paths):
            wrote = c.write and all(
                V.VECTOR_FINDINGS.get((p, c.id)) is None for p in ("write1", "write2")
            )
            add(
                c.driver,
                "vector",
                _ordered(["vector_gbx", c.reader] if c.reader else ["vector_gbx"]),
                c.write if wrote else None,
            )
    for driver, (kind, entry, _) in SPECIAL.items():
        add(driver, kind, [entry], None)
    return {d: out[d] for d in sorted(out)}


def test_the_registry_is_the_passing_cases_and_nothing_else():
    got = derived()
    assert set(TESTED_DRIVERS) == set(got), (
        f"listed without a passing case: {sorted(set(TESTED_DRIVERS) - set(got))}; "
        f"passing but not listed: {sorted(set(got) - set(TESTED_DRIVERS))}"
    )
    for driver, want in got.items():
        assert TESTED_DRIVERS[driver] == want, driver


def test_the_special_cases_have_their_tests():
    for driver, (_, _, names) in SPECIAL.items():
        for n in names:
            assert callable(getattr(special, n, None)), f"{driver}: no test {n}"


def test_every_finding_names_a_path_that_runs():
    raster = {(p, c.id) for c in R.RASTER_CASES for p in R.read_paths(c)} | {
        ("asformat", c.id)
        for c in [raster_write.write_case(d) for d in raster_write.WRITE]
    }
    assert set(R.RASTER_FINDINGS) <= raster, sorted(set(R.RASTER_FINDINGS) - raster)
    vector = {(p, c.id) for c in V.VECTOR_CASES for p in V.read_paths(c)}
    vector |= {(f"write{n}", c.id) for c in V.VECTOR_CASES if c.write for n in (1, 2)}
    vector |= {("folder", "MVT")}
    assert set(V.VECTOR_FINDINGS) <= vector, sorted(set(V.VECTOR_FINDINGS) - vector)


def test_each_tested_driver_is_in_the_build_and_in_the_table_of_its_kind():
    snap = {
        r["driver"]: r for r in json.loads(_listing.SNAPSHOT.read_text())["drivers"]
    }
    for driver, e in TESTED_DRIVERS.items():
        assert driver in snap, f"{driver} is not a driver of the env wheel's build"
        for kind, entry in e.items():
            assert snap[driver][kind], f"{driver} is not a {kind} driver"
            assert entry["read"], (driver, kind)
        assert snap[driver]["allowed"] and snap[driver]["available"], driver
