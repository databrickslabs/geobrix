"""Shared helpers for selective SQL registration: register(spark, only=[...]).

Used by the lightweight register() functions (pyrx, pygx, pyvx) so each can
register a subset of its gbx_* SQL functions. Names are case-insensitive and
accept either the short form (rst_slope) or the full SQL name (gbx_rst_slope).
"""

from __future__ import annotations

import difflib
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

# A group = (availability-guard thunk, {canonical_sql_name: register_fn(spark)}).
Group = Tuple[Callable[[], None], Dict[str, Callable[[Any], None]]]


def normalize_name(name: str) -> str:
    """Normalize one requested name to its canonical gbx_ SQL name.

    Strips whitespace, lowercases (SQL names are lowercase; Scala classes are
    CamelCase, so users may type RST_Slope / BNG_Polyfill), and prepends gbx_
    if absent. 'rst_slope', 'RST_Slope', 'gbx_rst_slope' -> 'gbx_rst_slope'.
    """
    n = name.strip().lower()
    return n if n.startswith("gbx_") else f"gbx_{n}"


def normalize_datasource_name(name: str) -> str:
    """Normalize one DataSource format name to its canonical form.

    DataSource formats use a `_gbx` suffix (not a `gbx_` prefix). Strips +
    lowercases, then appends `_gbx` if absent. 'raster', 'RASTER',
    'raster_gbx' -> 'raster_gbx'.
    """
    n = name.strip().lower()
    return n if n.endswith("_gbx") else f"{n}_gbx"


def resolve_only(
    only: Iterable[str],
    valid: Iterable[str],
    normalizer: Callable[[str], str] = normalize_name,
) -> Set[str]:
    """Normalize requested names (via `normalizer`) and validate against `valid`.

    Returns the set of canonical names to register. Raises ValueError that lists
    any name not matching a registerable target (after normalization), with up
    to 3 difflib close matches each.
    """
    valid_set = set(valid)
    requested = [(orig, normalizer(orig)) for orig in only]
    unknown = [(orig, norm) for orig, norm in requested if norm not in valid_set]
    if unknown:
        lines = []
        for orig, norm in unknown:
            matches = difflib.get_close_matches(norm, valid_set, n=3)
            hint = f" -> did you mean: {', '.join(matches)}?" if matches else ""
            lines.append(f"  {orig!r}{hint}")
        raise ValueError(
            "register(only=...) got unrecognized name(s):\n"
            + "\n".join(lines)
            + "\nPass a registerable name (or its short form) for this tier."
        )
    return {norm for _, norm in requested}


def run_groups(groups: List[Group], spark: Any, only: Optional[Iterable[str]]) -> None:
    """Register the selected functions across `groups`.

    only=None registers every function in every group (guards all run, in order).
    only=[...] registers exactly the named functions; a group's guard runs only
    when >=1 of its functions is selected. Validation is against the union of all
    group names.
    """
    all_names: Set[str] = set()
    for _guard, entries in groups:
        all_names |= set(entries)
    wanted = None if only is None else resolve_only(only, all_names)
    from databricks.labs.gbx import _gdal_policy

    # every function registered here carries the driver's remote-path switch to the workers
    with _gdal_policy.carrying_registrations(spark):
        for guard, entries in groups:
            selected = [
                fn for name, fn in entries.items() if wanted is None or name in wanted
            ]
            if not selected:
                continue
            guard()
            for fn in selected:
                fn(spark)
