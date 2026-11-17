"""Genie helpers: build or seed a Genie space and ask a Genie Agent in Agent mode.

``databricks-sdk`` (the ``databricks`` extra, preinstalled on Databricks runtimes) is
imported lazily inside the functions, never at module import.
"""

from databricks.labs.gbx.genie.agent import ask
from databricks.labs.gbx.genie.space import apply_space, build_serialized_space

__all__ = ["ask", "apply_space", "build_serialized_space"]
