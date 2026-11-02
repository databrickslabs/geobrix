"""X1 (review m-7): gbx_rst_bng_rasterize_agg takes a trailing out_crs on both tiers, and the docs say so.

The Python entry is generated from the manifest row (gen --check holds it to the row), so the old light-vs-heavy
signature comparison is gone with the light def it compared. This file pins the docs line (against the 13
parameters) and the light tier's refusal text; the Scala builder's arity is pinned by the JVM row
test_l1_arity_contract.
"""

import re
from pathlib import Path

from databricks.labs.gbx.pyrx import functions as light

REPO = Path(__file__).resolve().parents[4]
EXPECTED = [
    "cellid",
    "value",
    "out_srid",
    "pixel_size",
    "xmin",
    "ymin",
    "xmax",
    "ymax",
    "width",
    "height",
    "mode",
    "kring_pad",
    "out_crs",
]


def test_the_docs_signature_line_lists_the_same_parameters():
    text = (REPO / "docs/docs/api/raster-functions.mdx").read_text()
    line = re.search(
        r"\| SQL \(session\) \| `gbx_rst_bng_rasterize_agg\(([^)]*)\)`", text
    )
    assert line is not None
    names = [p.strip().strip("[]") for p in line.group(1).split(",")]
    assert names == EXPECTED


def test_the_light_tier_refuses_a_non_null_out_crs_with_the_heavy_text():
    """A non-NULL out_crs is refused on the light tier too, with the heavy tier's message and its Fix:, so the two tiers
    agree. The check runs before any GDAL is touched, so this row needs no payload.
    """
    import pandas as pd
    import pytest

    msg = light.BNG_OUT_CRS_REFUSAL
    assert "out_crs is not supported" in msg and "Fix: pass NULL" in msg
    assert "gbx_rst_transformcrs" in msg
    # the heavy tier's text is the same sentence: the Scala literal is pieces joined by " +"
    scala = (
        REPO
        / "src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_BNG_RasterizeAgg.scala"
    ).read_text()
    block = re.search(r"OutCrsRefusal: String = (.*?)\n\s*\n", scala, re.S).group(1)
    assert "".join(re.findall(r'"([^"]*)"', block)) == msg

    def one(v):
        return pd.Series([v])

    udf = light._rst_bng_rasterize_agg_udf.func  # the pandas UDF's Python function
    with pytest.raises(ValueError, match="out_crs is not supported"):
        udf(
            one("TQ3080"),
            one(1.0),
            one(27700),
            one(None),
            one(None),
            one(None),
            one(None),
            one(None),
            one(None),
            one(None),
            one(None),
            one(None),
            one("EPSG:4326"),
        )
