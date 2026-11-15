"""Geometry set routing for the VectorX ST functions of the benchmark spec: which set a function reads, and how the
runner picks a set for a tile (explicit override, then the tile's source, then its SRID).
"""

import pytest

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench import runner
from databricks.labs.gbx.bench.manifest import GeometryCorpus, GeometrySet, TileEntry
from databricks.labs.gbx.bench.spec import REGISTRY

# geometry set -> the geometry_scalar functions that read it
_ROUTES = {
    "st_validity": ["st_makevalid", "st_explainvalidity"],
    "st_antimeridian": ["st_shiftlongitude", "st_wrapx", "st_split"],
    "st_cleaning": [
        "st_simplifypreservetopology",
        "st_removerepeatedpoints",
        "st_reduceprecision",
        "st_node",
        "st_snap",
    ],
    # CRS functions operate on a geometry, so they route to a valid box set
    "srid_4326": ["st_crs", "st_setcrs", "st_transformcrs"],
}


def _set(srid, tile, tag):
    return GeometrySet(
        srid=srid, source_tile=tile, boxes=[(tag, 1.0)], points=[], zpoints=[]
    )


def _tile(path, srid):
    return TileEntry(
        path=path,
        cellid=0,
        srid=srid,
        dtype="float32",
        bands=2,
        tile_px=256,
        nodata_frac=0.0,
    )


@pytest.mark.parametrize(
    "geom_set,fn", [(g, f) for g, fns in _ROUTES.items() for f in fns]
)
def test_st_functions_read_their_geometry_set(geom_set, fn):
    spec = REGISTRY[fn]
    assert (spec.input_kind, spec.geometry_set) == ("geometry_scalar", geom_set)


def test_the_coverage_checks_read_the_coverage_set_as_aggregates():
    for agg in ("st_coverageisvalid", "st_coverageinvalidedges"):
        assert REGISTRY[agg].geometry_set == "st_coverage"
        assert REGISTRY[agg].input_kind != "geometry_scalar"


def test_geometry_set_for_picks_override_then_source_then_srid():
    by_source, by_srid = _set(4326, "tile_4326.tif", b"\x00"), _set(
        27700, "x.tif", b"\x01"
    )
    corpus = GeometryCorpus(
        seed=1,
        srid=4326,
        source_tile="tile_4326.tif",
        sets={"by_source": by_source, "by_srid": by_srid},
    )
    assert (
        runner._geometry_set_for(corpus, _tile("tile_4326.tif", 4326), None)
        == by_source
    )
    assert runner._geometry_set_for(corpus, _tile("other.tif", 27700), None) == by_srid
    assert (
        runner._geometry_set_for(corpus, _tile("tile_4326.tif", 4326), "by_srid")
        == by_srid
    )
    with pytest.raises(ValueError, match="geometry_set 'invalid' not found"):
        runner._geometry_set_for(corpus, _tile("tile_4326.tif", 4326), "invalid")


def test_a_generated_corpus_holds_the_named_geometry_sets(tmp_path):
    dg.generate_corpus(
        out_dir=tmp_path,
        seed=42,
        tile_px=[32],
        bands=[2],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=2,
        row_dtype="float32",
    )
    corpus = runner._load_geometry_corpus(tmp_path)
    assert {"st_validity", "srid_4326"} <= set(corpus.sets)
    assert all(
        gset.boxes or gset.points or gset.zpoints for gset in corpus.sets.values()
    )
    tile = _tile("rows/r0.tif", 4326)
    assert runner._geometry_set_for(corpus, tile, "st_validity").boxes
    assert runner._geometry_set_for(corpus, tile, None) is not None
