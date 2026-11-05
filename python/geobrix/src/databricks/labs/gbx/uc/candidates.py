"""SQL bodies that are candidates for a function's UC form: data only, never installed by default.

A candidate is a SQL expression (and its private SQL helpers) that may replace a Scala or Python handler on Unity
Catalog if a measurement shows it faster and the oracle rows agree. It lives here, not on the manifest row, so the
default install does not change before the numbers are in: a row that gained an L4 ``sql`` key would be installed as a
SQL function at once. The M2 probe (``bench.uc_perf``) reads it to build the ``sql`` leg of the row's probe copies;
``bench`` and the host tests judge it against the row's oracle rows. When a candidate wins, its body moves onto the row
(L4 ``sql``) and the entry here is deleted.

Pure data: no pyspark, no manifest import at module level.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Tuple


@dataclass(frozen=True)
class Candidate:
    """``body`` is the text after ``RETURN`` (``{q}`` marks a helper call, as in a manifest ``sql`` key) and ``helpers``
    the private SQL functions it calls (``gbx__<x>``, the manifest's helper shape). ``limits`` lists, in words, where the
    body is known to differ from the Python handler (so a decision to adopt it reads them).
    """

    function: str
    body: str
    helpers: Tuple[Mapping, ...] = ()
    summary: str = ""
    limits: Tuple[str, ...] = ()


# No candidate is waiting: gbx_bng_cellarea and gbx_bng_eastnorthasbng were measured (SQL 1 s, Scala 10 s, Python 33-78 s over
# 10M rows, results equal) and moved onto their rows as L0/L4 `sql`. Add an entry here as ``name: Candidate(...)`` for the next one.
CANDIDATES: Dict[str, Candidate] = {}


def get(name: str) -> Optional[Candidate]:
    """The candidate for the manifest function ``name``, or None."""
    return CANDIDATES.get(name)


def source(name: str) -> Optional[Tuple[str, tuple]]:
    """``(body, helpers)`` of the candidate for ``name`` (the shape ``sqlbody.l4_source`` returns), or None."""
    c = CANDIDATES.get(name)
    return None if c is None else (c.body, tuple(c.helpers))


def with_candidate(fn, cand: Optional[Candidate] = None):
    """``fn`` (a manifest ``Function``) with the candidate's SQL kernel as its L4 ``sql`` key: a copy to build probe
    functions from, never stored. A row that already has a SQL kernel is returned as it is (the row wins).
    """
    from databricks.labs.gbx.uc import sqlbody

    cand = cand or CANDIDATES.get(fn.sql_name)
    if cand is None or sqlbody.l4_source(fn) is not None:
        return fn
    l4 = {**fn.levels["L4"], "sql": cand.body}
    if cand.helpers:
        l4["sql_helpers"] = tuple(cand.helpers)
    return dataclasses.replace(fn, levels={**fn.levels, "L4": l4})
