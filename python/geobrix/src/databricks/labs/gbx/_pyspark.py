"""One clear error when PySpark is missing.

PySpark is not a GeoBrix dependency: every supported Databricks runtime ships its
own (classic DBR, and Serverless / Lakeflow through Spark Connect). Importing this
module checks for it. Each module that imports PySpark at import time imports this
one first (an ``# isort: split`` keeps it ahead of the pyspark imports), so a
missing PySpark fails there with this message instead of a bare
``ModuleNotFoundError``. Packages and modules that import PySpark lazily do not
import this one, so they stay importable without it.
"""

MESSAGE = (
    "No module named 'pyspark'. GeoBrix uses the PySpark that ships with your "
    "Databricks runtime; outside Databricks install `pyspark>=4.0.0`."
)


def require_pyspark() -> None:
    """Raise ``ModuleNotFoundError`` with :data:`MESSAGE` if PySpark is missing."""
    try:
        import pyspark  # noqa: F401
    except ModuleNotFoundError as e:
        if e.name != "pyspark":
            raise  # PySpark is installed; one of its own dependencies is not
        raise ModuleNotFoundError(MESSAGE, name="pyspark") from e


require_pyspark()
