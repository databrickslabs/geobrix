"""Test geometry set routing for VectorX ST functions in the benchmark spec."""

import pytest

from databricks.labs.gbx.bench import runner


def test_geometry_set_field_added_to_fnspec():
    """Verify that FnSpec has the geometry_set field."""
    from databricks.labs.gbx.bench.spec import _BOTH, FnSpec

    # Create a simple FnSpec with geometry_set
    fs = FnSpec(
        name="test_fn",
        sql_name="gbx_test_fn",
        category="test",
        modes=_BOTH,
        geometry_set="test_set",
    )
    assert fs.geometry_set == "test_set"

    # Verify None is the default
    fs_no_geom = FnSpec(
        name="test_fn",
        sql_name="gbx_test_fn",
        category="test",
        modes=_BOTH,
    )
    assert fs_no_geom.geometry_set is None


def test_geometry_set_override_in_geometry_set_for():
    """Verify _geometry_set_for honors the geometry_set_override parameter."""
    from databricks.labs.gbx.bench.manifest import GeometryCorpus, GeometrySet

    # Create a minimal corpus with two sets
    set1 = GeometrySet(
        srid=4326,
        source_tile="tile1.tif",
        boxes=[(b"\x00", 1.0)],
        points=[],
        zpoints=[],
    )
    set2 = GeometrySet(
        srid=27700,
        source_tile="tile2.tif",
        boxes=[(b"\x01", 2.0)],
        points=[],
        zpoints=[],
    )
    corpus = GeometryCorpus(
        seed=1, srid=4326, source_tile="tile1.tif", sets={"set1": set1, "set2": set2}
    )

    # Create a dummy tile entry
    from databricks.labs.gbx.bench.manifest import TileEntry

    te = TileEntry(
        path="tile1.tif",
        cellid=0,
        srid=4326,
        dtype="float32",
        bands=2,
        tile_px=256,
        nodata_frac=0.0,
    )

    # Test 1: Without override, should match by source_tile
    result = runner._geometry_set_for(corpus, te, None)
    assert result == set1

    # Test 2: With override to set2, should return set2
    result = runner._geometry_set_for(corpus, te, "set2")
    assert result == set2

    # Test 3: With invalid override, should raise ValueError
    with pytest.raises(ValueError, match="geometry_set 'invalid' not found"):
        runner._geometry_set_for(corpus, te, "invalid")


def test_st_functions_have_geometry_set():
    """Verify that all required ST functions have geometry_set configured."""
    from databricks.labs.gbx.bench.spec import REGISTRY

    # Expected geometry_set assignments per the task
    expected_mappings = {
        "st_validity": ["st_makevalid", "st_explainvalidity"],
        "st_antimeridian": ["st_shiftlongitude", "st_wrapx", "st_split"],
        "st_cleaning": [
            "st_simplifypreservetopology",
            "st_removerepeatedpoints",
            "st_reduceprecision",
            "st_node",
            "st_snap",
        ],
        "st_coverage": ["st_coverageisvalid", "st_coverageinvalidedges"],
        # CRS fns operate on a geometry, so they route to a valid box set (srid_4326).
        "srid_4326": ["st_crs", "st_setcrs", "st_transformcrs"],
    }

    for geom_set, fn_names in expected_mappings.items():
        for fn_name in fn_names:
            assert fn_name in REGISTRY, f"Function {fn_name} not found in REGISTRY"
            spec = REGISTRY[fn_name]
            actual_geom_set = getattr(spec, "geometry_set", None)
            assert (
                actual_geom_set == geom_set
            ), f"{fn_name} has geometry_set={actual_geom_set}, expected {geom_set}"


def test_geometry_set_for_with_default_matching():
    """Verify _geometry_set_for default behavior (no override) still works."""
    from databricks.labs.gbx.bench.manifest import (
        GeometryCorpus,
        GeometrySet,
        TileEntry,
    )

    # Create corpus with sets keyed by srid
    set_4326 = GeometrySet(
        srid=4326,
        source_tile="tile_4326.tif",
        boxes=[(b"\x00", 1.0)],
        points=[],
        zpoints=[],
    )
    set_27700 = GeometrySet(
        srid=27700,
        source_tile="tile_27700.tif",
        boxes=[(b"\x01", 2.0)],
        points=[],
        zpoints=[],
    )
    corpus = GeometryCorpus(
        seed=1,
        srid=4326,
        source_tile="tile_4326.tif",
        sets={"by_source": set_4326, "by_srid": set_27700},
    )

    # Tile with matching source_tile
    te1 = TileEntry(
        path="tile_4326.tif",
        cellid=0,
        srid=4326,
        dtype="float32",
        bands=2,
        tile_px=256,
        nodata_frac=0.0,
    )
    result = runner._geometry_set_for(corpus, te1, None)
    assert result == set_4326

    # Tile with no matching source_tile but matching srid (27700)
    te2 = TileEntry(
        path="other_tile.tif",
        cellid=1,
        srid=27700,
        dtype="float32",
        bands=2,
        tile_px=256,
        nodata_frac=0.0,
    )
    result = runner._geometry_set_for(corpus, te2, None)
    assert result == set_27700


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
