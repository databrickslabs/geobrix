"""VectorX Python-only helpers: DataFrame functions with no SQL name.

The implementations stay in ``databricks.labs.gbx.pyvx``; this module is the public API.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

__all__ = ["coverage_simplify"]


def coverage_simplify(
    df,
    group_col,
    geom_col,
    tolerance,
    simplify_boundary=True,
    out_col="geom_simplified",
):
    """Topology-preserving simplification of a whole coverage (Python-API only).

    Groups df by group_col and simplifies each coverage together (shared edges stay
    shared). N rows in -> N rows out; every input column preserved, EWKB written to
    out_col. Serverless-safe (groupBy().applyInPandas()).
    """
    from pyspark.sql.types import BinaryType, StructField, StructType

    from databricks.labs.gbx.pyvx import _coverage as _cov

    out_schema = StructType(
        list(df.schema.fields) + [StructField(out_col, BinaryType(), True)]
    )

    def _fn(pdf):
        return _cov.coverage_simplify_pdf(
            pdf, geom_col, tolerance, simplify_boundary, out_col
        )

    return df.groupBy(group_col).applyInPandas(_fn, schema=out_schema)
