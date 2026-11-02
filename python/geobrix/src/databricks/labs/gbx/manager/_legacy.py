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


# the light modules that could not be imported, with the reason (a missing dependency of one module must not stop the
# manifest's names or the other modules' names from registering)
_FAILED: Dict[str, str] = {}
# the names the bridge registered, by module: a CRS-grid registration re-registers these (and only these) with fresh
# closures, where a module's own register() would also overwrite the manager's wrappers under the same SQL names
_BRIDGED: Dict[str, Set[str]] = {}


@functools.lru_cache(maxsize=1)
def _by_module() -> Dict[str, list]:
    """Each light module's registrar groups, (guard, {name: register}) pairs, as the module defines them. A module
    that fails to import is left out and named in :func:`import_failures`."""
    out: Dict[str, list] = {}
    _FAILED.clear()
    for m in _MODULES:
        try:
            out[m] = list(importlib.import_module(m)._registrar_groups())
        except (
            Exception
        ) as exc:  # noqa: BLE001 - one module's failure is its own names' loss only
            first = str(exc).splitlines()[0][:160] if str(exc) else ""
            _FAILED[m] = (
                f"{type(exc).__name__}: {first}" if first else type(exc).__name__
            )
    return out


def import_failures() -> Dict[str, str]:
    """The light modules that could not be imported (module -> reason); their names are not in :func:`names`."""
    _by_module()
    return dict(_FAILED)


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
            _BRIDGED.setdefault(m, set()).update(todo)
    return done, lines


def reregister_bridged(spark) -> None:
    """Register the bridged names again (fresh closures, such as after the CRS grid directories change), with the
    light registrars as before, and only those names: not the whole module, and not the names the manager serves.
    """
    for names_ in list(_BRIDGED.values()):
        register_unmigrated(spark, sorted(names_))
