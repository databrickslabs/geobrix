"""X1 (review m-7): gbx_rst_bng_rasterize_agg takes a trailing out_crs on both tiers, and the docs say so.

The seven-surface rule (.claude/rules/functions-authoring.md): the builder (Scala, 12 or 13 arguments), the wrapper,
the heavy shim, the light binding, the docs Signature line, and the usage metadata must agree on the parameter list.
This row pins the Python surfaces and the docs line; the Scala builder's arity is pinned by the JVM row
test_l1_arity_contract.
"""

import inspect
import re
from pathlib import Path

from databricks.labs.gbx.pyrx import functions as light
from databricks.labs.gbx.rasterx import functions as heavy

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


def test_the_light_and_heavy_shims_take_the_same_thirteen_parameters():
    light_names = list(inspect.signature(light.rst_bng_rasterize_agg).parameters)
    heavy_names = list(inspect.signature(heavy.rst_bng_rasterize_agg).parameters)
    assert light_names == EXPECTED
    assert heavy_names == EXPECTED


def test_the_docs_signature_line_lists_the_same_parameters():
    text = (REPO / "docs/docs/api/raster-functions.mdx").read_text()
    line = re.search(r"\*\*Signature:\*\* `rst_bng_rasterize_agg\(([^)]*)\)", text)
    assert line is not None
    names = [p.strip().split(":")[0] for p in line.group(1).split(",")]
    assert names == EXPECTED
