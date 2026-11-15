"""
Tests for the GridX light-tier (pygx) Python examples.

Every example in gridx_functions_python_light.py runs against the autouse setup views and meets its check below.
An example without a check, or without its `*_output` constant, fails.
"""

import pytest

try:
    from . import gridx_functions_python_light as light_examples
except (ModuleNotFoundError, ImportError):
    try:
        import gridx_functions_python_light as light_examples
    except ModuleNotFoundError:
        light_examples = None

SF_Z10 = 5233961839712272383  # the San Francisco cell at quadbin zoom 10
CUSTOM_CELL = 360287970373976640  # POINT(530000 180000) at custom resolution 5


def _bytes(r):
    return isinstance(r, (bytes, bytearray)) and len(r) > 0


def _eq(value):
    return lambda r: r == value


def _cells(n, has=None, hasnot=None, key=None, ints=False):
    """A list of `n` cells (or rows or chips, whose cell is `key`), with `has` in it and `hasnot` out of it."""

    def check(r):
        if not isinstance(r, list) or len(r) != n:
            return False
        ids = [c[key] for c in r] if key else r
        return (
            (has is None or has in ids)
            and (hasnot is None or hasnot not in ids)
            and (not ints or all(isinstance(c, int) for c in ids))
        )

    return check


def _some(ints=False):
    return lambda r: isinstance(r, list) and len(r) > 0 and (
        not ints or all(isinstance(c, int) for c in r)
    )


def _chip(r):
    return r is not None and r["cellid"] == "TQ3080" and r["core"] is True


def _tessellation(r):
    """9 chips; the core chip is TQ3080 and keeps no geometry (keep_core_geom defaults to false)."""
    core = [c for c in r if c["core"] is True]
    return (
        len(r) == 9
        and {c["cellid"] for c in core} >= {"TQ3080"}
        and all(c["chip"] is None for c in core)
    )


def _tessellateexplode(r):
    return (
        len(r) == 9
        and all(hasattr(r[0], f) for f in ("cellid", "core", "chip"))
        and "TQ3080" in {x["cellid"] for x in r if x["core"] is True}
    )


def _fill_payload(n, center):
    """The BINARY a `*_cellfill` aggregate returns, decoded: `n` cells, all 5.0, so the NULL `center` is filled."""

    def check(r):
        from databricks.labs.gbx.pygx import _cellfill  # noqa: PLC0415

        if not _bytes(r):
            return False
        cells = dict(_cellfill.decode(bytes(r)))
        return len(cells) == n and cells.get(center) == 5.0 and set(cells.values()) == {5.0}

    return check


def _filled(n, center=None):
    """A fill result: `n` (cellid, value) structs in ascending id order, all 5.0 (so the NULL cell is filled)."""

    def check(r):
        ids = [c["cellid"] for c in r] if isinstance(r, list) else []
        return (
            len(ids) == n
            and ids == sorted(ids)
            and (center is None or center in ids)
            and all(c["value"] == 5.0 for c in r)
        )

    return check


def _wkb_equals(x0, x1):
    """WKB of the box x0..x1 over the 1 km cell's northing 180000..181000."""

    def check(r):
        from shapely import box
        from shapely.wkb import loads

        return _bytes(r) and loads(bytes(r)).equals(box(x0, 180000, x1, 181000))

    return check


# example name (without `_python_example`) -> what its result must satisfy
CHECKS = {
    # BNG codec and accessors
    "bng_aswkb": _bytes,
    "bng_aswkt": lambda r: isinstance(r, str)
    and r.startswith("POLYGON")
    and "531000" in r,
    "bng_cellarea": _eq(1.0),
    "bng_centroid": _bytes,
    "bng_eastnorthasbng": _eq("TQ3080"),
    "bng_pointascell": _eq("TQ3080"),
    # BNG distance and chips
    "bng_distance": _eq(1),
    "bng_euclideandistance": _eq(1),
    "bng_cellintersection": _chip,
    "bng_cellunion": _chip,
    "bng_cellintersection_agg": _bytes,
    "bng_cellunion_agg": _bytes,
    # BNG neighbourhoods and fills
    "bng_kring": _cells(9, has="TQ3080"),
    "bng_kloop": _cells(8, hasnot="TQ3080"),
    "bng_geomkring": _cells(24),
    "bng_geomkloop": _cells(16),
    "bng_polyfill": _cells(9, has="TQ3080"),
    "bng_tessellate": _tessellation,
    "bng_kringexplode": _cells(9, has="TQ3080", key="cellid"),
    "bng_kloopexplode": _cells(8, hasnot="TQ3080", key="cellid"),
    "bng_geomkringexplode": _cells(24),
    "bng_geomkloopexplode": _cells(16),
    "bng_tessellateexplode": _tessellateexplode,
    "bng_cellfill": _bytes,
    # quadbin
    "quadbin_pointascell": _eq(SF_Z10),
    "quadbin_aswkb": _bytes,
    "quadbin_centroid": _bytes,
    "quadbin_resolution": _eq(10),
    "quadbin_distance": _eq(1),
    "quadbin_kring": _cells(9, has=SF_Z10),
    "quadbin_kloop": _cells(8, hasnot=SF_Z10, ints=True),
    "quadbin_polyfill": _cells(4, has=5211790668774506495),
    "quadbin_tessellate": _cells(4, has=5211790668774506495, key="cell"),
    "quadbin_cellunion": _bytes,
    "quadbin_cellunion_agg": _bytes,
    "quadbin_cellfill": _fill_payload(3, center=5234158540624494591),
    "quadbin_geomkring": _some(ints=True),
    "quadbin_geomkloop": _some(),
    "quadbin_geomkringexplode": _some(),
    "quadbin_geomkloopexplode": _some(),
    # custom grid
    "custom_grid": lambda r: r is not None,
    "custom_pointascell": _eq(CUSTOM_CELL),
    "custom_cellaswkb": _bytes,
    "custom_cellaswkt": lambda r: isinstance(r, str)
    and r.startswith("POLYGON")
    and "530031.25" in r,
    "custom_centroid": _bytes,
    "custom_polyfill": _cells(36, has=72057594038644994, ints=True),
    "custom_kring": _cells(9, has=CUSTOM_CELL),
    "custom_kloop": _cells(8, hasnot=CUSTOM_CELL, ints=True),
    "custom_distance": lambda r: isinstance(r, int) and r == 1,
    "custom_geomkring": _cells(56),
    "custom_geomkloop": _cells(32),
    "custom_geomkringexplode": _cells(56),
    "custom_geomkloopexplode": _cells(32),
    "custom_cellfill": _bytes,
    # H3 (light only)
    "h3_geomkring": _some(ints=True),
    "h3_geomkloop": _some(),
    "h3_geomkringexplode": _some(),
    "h3_geomkloopexplode": _some(),
    "h3_cellfill": _bytes,
    # array forms of the cell aggregates
    "bng_cellfill_array": _filled(5, center="TQ300800"),
    "quadbin_cellfill_array": _filled(3, center=5234158540624494591),
    "h3_cellfill_array": _filled(3, center=612934495919669247),
    "custom_cellfill_array": _filled(3),
    "bng_cellunion_array": _wkb_equals(530000, 531000),
    "bng_cellintersection_array": _wkb_equals(530400, 530600),
}


@pytest.fixture(autouse=True)
def _gridx_light_setup_views(spark):
    """Register pygx + create the ten GridX light-tier Setup views so every
    per-function example can read ``spark.table("bng_cells")`` etc."""
    from databricks.labs.gbx import manager as gbx_manager
    from ._fixtures import create_setup_views_gridx_light  # noqa: PLC0415

    gbx_manager.register_family(
        spark, ("gbx_bng_", "gbx_custom_", "gbx_quadbin_", "gbx_h3_"), level="L3"
    )
    create_setup_views_gridx_light(spark)


def test_gridx_light_setup_executes(spark):
    """gridx_light_setup_example(spark) runs without error, twice: register is idempotent."""
    assert light_examples is not None
    assert hasattr(light_examples, "gridx_light_setup_example_output")
    light_examples.gridx_light_setup_example(spark)
    light_examples.gridx_light_setup_example(spark)


@pytest.mark.parametrize("name", sorted(CHECKS))
def test_python_example_meets_its_check(spark, name):
    assert light_examples is not None
    result = getattr(light_examples, f"{name}_python_example")(spark)
    assert CHECKS[name](result), f"{name}: unexpected result {result!r}"


def test_every_example_has_a_check_and_an_output():
    """The checks are the module's examples, and each example has its non-empty `*_output` constant."""
    assert light_examples is not None
    examples = {
        n[: -len("_python_example")]
        for n in dir(light_examples)
        if n.endswith("_python_example")
    }
    assert examples == set(CHECKS)
    for name in examples:
        out = getattr(light_examples, f"{name}_python_example_output", None)
        assert isinstance(out, str) and out.strip(), f"empty output constant: {name}"
