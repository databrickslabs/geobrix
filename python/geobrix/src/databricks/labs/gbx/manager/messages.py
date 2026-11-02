"""Registration messages: each reason once per session, the 0.6.0 reminder once per session."""

from __future__ import annotations

import weakref
from typing import Dict, Iterable, List, Set, Tuple

REMINDER = (
    "geobrix 0.6.0: one wheel per runtime (+env5 / +env6); SQL functions register through "
    "databricks.labs.gbx.register(spark); the Python API is rasterx / gridx / vectorx. "
    "See https://databrickslabs.github.io/geobrix/docs/migration-0-6"
)
MIGRATION_PATH = "/docs/migration-0-6"  # the docs page the reminder links to
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


def say_grouped(spark, lines: Iterable[Tuple[str, str]]) -> None:
    """(name, line) pairs -> one printed line per distinct reason. A reason shared by several names prints
    once with a count; a reason that names its function prints as it is when only one name has it.
    """
    by_reason: Dict[str, List[str]] = {}
    for name, line in lines:
        by_reason.setdefault(line.replace(name, "{fn}"), []).append(name)
    for template, names in by_reason.items():
        if len(names) == 1:
            say(spark, template.replace("{fn}", names[0]))
        elif "{fn}" in template:
            shown = ", ".join(sorted(names)[:3]) + (", ..." if len(names) > 3 else "")
            say(spark, template.replace("{fn}", f"{len(names)} functions ({shown})"))
        else:
            say(spark, f"{template} ({len(names)} functions)")
