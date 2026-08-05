"""Task 1 (VectorX-CRS): the CRS resolver lives in the tier-neutral
databricks.labs.gbx.core.crs; pyrx.core.crs is a re-export shim."""

from rasterio.crs import CRS


def test_neutral_module_exports_the_resolver():
    from databricks.labs.gbx.core.crs import (
        crs_to_canonical,
        get_transformer,
        resolve_crs,
        resolve_source_crs,
    )

    assert resolve_crs(54008).to_authority() == ("ESRI", "54008")
    assert crs_to_canonical(resolve_crs(4326)) == "EPSG:4326"
    assert resolve_source_crs(4326, None, None) == CRS.from_epsg(4326)
    assert get_transformer(4326, 3857) is get_transformer("EPSG:4326", "3857")


def test_pyrx_shim_still_works():
    # Existing pyrx.core.crs importers (20 of them) must keep working unchanged.
    from databricks.labs.gbx.pyrx.core import crs as C

    assert C.resolve_crs(4326) == CRS.from_epsg(4326)
    assert C.crs_to_canonical(C.resolve_crs(54008)) == "ESRI:54008"
    # Privates the CRS-100 tests reference through the module must survive the shim.
    assert C._TRANSFORMER_CACHE_SIZE == 128
    assert C._is_intlike(4326) is True
    assert C._transformer_cache() is not None
