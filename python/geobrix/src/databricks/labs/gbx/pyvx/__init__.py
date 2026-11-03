"""pyvx: the internal implementation package behind ``databricks.labs.gbx.vectorx``.

Nothing here is public API: the API is ``databricks.labs.gbx.vectorx`` (and ``rasterx``, ``gridx``).
What this package used to export moved there, and an old path raises with a ``Fix:`` that names the new one.
"""

from databricks.labs.gbx import _moved

__getattr__ = _moved.install(
    __name__,
    {m: f"databricks.labs.gbx.vectorx.{m}" for m in ("file_read", "file_write")},
)
