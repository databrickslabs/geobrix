"""Transition only: SQL names not yet in the manifest register through the light registrars, so
databricks.labs.gbx.register(spark) serves every function from the first converted family on.
Deleted with the light registrars once every family is in the manifest."""

from __future__ import annotations

import functools
import importlib
from typing import Dict, Iterable, List, Set, Tuple

_MODULES = (
    "databricks.labs.gbx.pyrx.functions",
    "databricks.labs.gbx.pygx.functions",
    "databricks.labs.gbx.pyvx.functions",
)


@functools.lru_cache(maxsize=1)
def _by_module() -> Dict[str, Set[str]]:
    out = {}
    for m in _MODULES:
        mod = importlib.import_module(m)
        out[m] = set().union(
            *(set(entries) for _guard, entries in mod._registrar_groups())
        )
    return out


def names() -> Set[str]:
    return set().union(*_by_module().values())


def register_unmigrated(
    spark, wanted: Iterable[str]
) -> Tuple[Set[str], List[Tuple[str, str]]]:
    """Register ``wanted`` through their light registrars: (the names registered, a (name, line) pair for each
    name a registrar could not serve here). A registrar that fails skips its names, never the others.
    """
    wanted = set(wanted)
    done: Set[str] = set()
    lines: List[Tuple[str, str]] = []
    for m, have in _by_module().items():
        todo = sorted(have & wanted)
        if not todo:
            continue
        try:
            importlib.import_module(m).register(spark, only=todo)
        except (
            Exception
        ) as exc:  # noqa: BLE001 - a missing light dependency skips these names only
            why = (
                f"{type(exc).__name__}: {str(exc).splitlines()[0][:160]}"
                if str(exc)
                else type(exc).__name__
            )
            lines += [(n, f"geobrix: {n} not registered ({why})") for n in todo]
            continue
        done.update(todo)
    return done, lines
