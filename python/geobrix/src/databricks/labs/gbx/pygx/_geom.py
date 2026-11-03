"""Shared geometry-input parsing for the pygx light tier.

Every geom-accepting pygx function uses ``parse_geom`` so the accepted encodings
(WKB / EWKB / WKT / EWKT) stay consistent across the grid surface and match the
heavyweight tier (which accepts BINARY|STRING for geometry inputs).

This is a thin re-export of the tier-wide decoder in ``databricks.labs.gbx._geom``
(the single source of truth). ``gbx._geom`` depends only on ``shapely`` — a
pygx dependency — so re-exporting it never drags in another tier's deps and
keeps decoding behavior identical across pyrx / pyvx / pygx.
"""

from databricks.labs.gbx._geom import geom_to_wkb, jvm_rect, parse_geom

__all__ = ["parse_geom", "geom_to_wkb", "jvm_rect"]
