"""G2 — standing parity guard: the light-tier v2 tile schema equals the heavy-tier
v2 tile schema, field-for-field.

Prevents the two tiers silently diverging — the class of bug that left the light
tier emitting a legacy 3-field struct while the heavy tier was already on the
v2 struct. The heavy field contract is mirrored from Scala
``RST_ExpressionUtil.v2TileType`` (kept in lock-step; a change on either side that
breaks this is a real cross-tier divergence to reconcile, not a test to relax).
"""

from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

# The heavy-tier v2 tile schema field contract, mirrored from Scala
# src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala
# (val v2TileType). If heavy changes its tile schema, update BOTH sides together.
_HEAVY_V2_FIELDS = [
    "cellid",
    "raster",
    "path",
    "path_mode",
    "window",
    "clip_polygon",
    "clip_crs",
    "crs",
    "metadata",
]


def test_light_v2_schema_matches_heavy_field_contract():
    """Light V2_TILE_SCHEMA field names + order match the heavy v2TileType contract."""
    assert [fld.name for fld in V2_TILE_SCHEMA.fields] == _HEAVY_V2_FIELDS
