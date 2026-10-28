"""Registration messages: each reason once per session, the 0.6.0 reminder once per session."""

from __future__ import annotations

import weakref
from typing import Dict, Set

REMINDER = (
    "geobrix 0.6.0: SQL functions register through databricks.labs.gbx.register(spark); "
    "each runtime has its own wheel. See https://databrickslabs.github.io/geobrix/docs/release-notes"
)
_SEEN: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()
_SEEN_BY_ID: Dict[int, Set[str]] = {}


def _seen(spark) -> Set[str]:
    try:
        return _SEEN.setdefault(spark, set())
    except TypeError:  # a session object that cannot be weakly referenced
        return _SEEN_BY_ID.setdefault(id(spark), set())


def say(spark, line: str) -> None:
    seen = _seen(spark)
    if line not in seen:
        seen.add(line)
        print(line)


def reminder(spark) -> None:
    say(spark, REMINDER)
