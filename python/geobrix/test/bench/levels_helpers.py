"""Helpers shared by the levels leg's test files (test_levels.py and test_levels_legs.py)."""

from databricks.labs.gbx.bench import levels

B = "gbx_bng_eastnorthasbng"
RST = "gbx_rst_transformcrs"


def run_s(spark, tmp_path, scales, base_rows=2000, mvp="S", **kw):
    kw = {"only_levels": ("L3",), **kw}
    return levels.run_mvp(
        spark, mvp, "t1", 1, 1, corpus=str(tmp_path), schema=None, scales=scales,
        where="venv", base_rows=base_rows, **kw,
    )  # fmt: skip
