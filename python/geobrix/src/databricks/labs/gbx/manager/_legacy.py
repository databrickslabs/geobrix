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
def _by_module() -> Dict[str, list]:
    """Each light module's registrar groups, (guard, {name: register}) pairs, as the module defines them."""
    return {m: list(importlib.import_module(m)._registrar_groups()) for m in _MODULES}


def names() -> Set[str]:
    return {
        n for groups in _by_module().values() for _g, entries in groups for n in entries
    }


def register_unmigrated(
    spark, wanted: Iterable[str]
) -> Tuple[Set[str], List[Tuple[str, str]]]:
    """Register ``wanted`` through their light registrars: (the names registered, a (name, line) pair for each
    name a registrar could not serve here). A GROUP is the unit: a group whose guard or registration fails (a
    missing light dependency) skips only its own names; the groups before and after it still register.
    """
    from databricks.labs.gbx import _register

    wanted = set(wanted)
    done: Set[str] = set()
    lines: List[Tuple[str, str]] = []
    for m, groups in _by_module().items():
        for guard, entries in groups:
            todo = sorted(set(entries) & wanted)
            if not todo:
                continue
            try:
                _register.run_groups([(guard, entries)], spark, todo)
            except (
                Exception
            ) as exc:  # noqa: BLE001 - a missing light dependency skips this group's names only
                why = (
                    f"{type(exc).__name__}: {str(exc).splitlines()[0][:160]}"
                    if str(exc)
                    else type(exc).__name__
                )
                lines += [(n, f"geobrix: {n} not registered ({why})") for n in todo]
                continue
            done.update(todo)
            importlib.import_module(m)._gbx_registered = (
                True  # as the module's register() marks it
            )
    return done, lines
