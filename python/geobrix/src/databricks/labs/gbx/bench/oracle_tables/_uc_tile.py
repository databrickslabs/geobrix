"""Helpers for the Unity Catalog rows of the raster tile functions (uc.tier_b): which session rows a UC function
shares, and the two rows every one adds (a virtual tile is refused with a Fix, a corrupt tile is NULL).
"""

import re
from dataclasses import dataclass, replace
from typing import Any, Callable, Optional

from databricks.labs.gbx.bench.oracles import (  # noqa: F401 - the engine rows' UC helpers live beside Case
    VIRTUAL_BUILDS,
    Build,
    Case,
    engine_uc,
    mirror,
)

BOTH = ("session", "uc")


def share(rows, *cids):
    """`rows` with the named rows also run on UC (their value is the session's); a name that is no row raises."""
    wanted, seen = set(cids), set()
    out = []
    for c in rows:
        if c.cid in wanted:
            seen.add(c.cid)
            c = replace(c, surfaces=BOTH)
        out.append(c)
    missing = wanted - seen
    assert not missing, f"no such oracle rows to share with UC: {sorted(missing)}"
    return tuple(out)


def _same(x):
    return x


@dataclass(frozen=True)
class Adapt:
    """How a session row becomes the row of a UC form that has another shape: ``args`` maps one argument row to the UC
    form's (the group's members to one array, an argument dropped or added), ``expect`` maps the session's expected value
    to the UC output (a decoded payload, EWKB bytes), and ``kind`` names the check when it differs on UC.
    """

    args: Callable[[tuple], tuple] = _same
    expect: Callable[[Any], Any] = _same
    kind: Optional[str] = None


def collect_group(project: Optional[Callable[[Any], Any]] = None):
    """An argument adapter for an aggregate's twin: the first argument (the group's members) becomes one array, each
    member through ``project`` when given (the member's one field, the ``struct`` the UC form reads), and the constants
    after it stay."""

    def adapt(args: tuple) -> tuple:
        members = list(args[0]) if isinstance(args[0], (list, tuple)) else [args[0]]
        return ([project(m) for m in members] if project else members, *args[1:])

    return adapt


def twin(rows, uc_fn: str, adapt: Adapt, *cids):
    """The UC rows of a form that is not the session's: each named session row (every session row without ``cids``) copied to
    the UC surface under the function ``uc_fn``, its arguments and expected value adapted. ``share`` is for a UC function
    of the session's own name; this is for another name or shape (an array twin of an aggregate, a partial-state form).

    The result holds only the UC rows (``twin_of`` names the session row each came from); add them to the family table
    beside the session rows. ``uc_fn`` must be a manifest function that has a UC form, so a twin cannot outlive or
    precede its function; a row that is no row, or runs on no session, raises."""
    from databricks.labs.gbx import manifest

    fn = manifest.load().functions.get(uc_fn)
    if fn is None or "L4" not in fn.levels:
        raise ValueError(f"twin: {uc_fn} is no UC function in the manifest")
    # no names: every row that has a session value to adapt (a Python-only row is skipped); a named row must have one
    wanted = set(cids) or {c.cid for c in rows if "session" in c.surfaces}
    found = {c.cid for c in rows}
    assert (
        not wanted - found
    ), f"no such oracle rows to twin on UC: {sorted(wanted - found)}"
    out = []
    for c in rows:
        if c.cid not in wanted:
            continue
        if c.fn == uc_fn:
            raise ValueError(
                f"twin: {c.cid} already runs {uc_fn}; use share() for a UC function of the session's name"
            )
        if "session" not in c.surfaces:
            raise ValueError(
                f"twin: {c.cid} has no session surface, so no session value to adapt"
            )
        out.append(
            replace(
                c,
                cid=f"{c.cid}__{uc_fn[len('gbx_'):]}",
                fn=uc_fn,
                args=adapt.args(c.args),
                expect=adapt.expect(c.expect),
                kind=adapt.kind or c.kind,
                surfaces=("uc",),
                variants=tuple(adapt.args(v) for v in c.variants),
                twin_of=c.cid,
            )
        )
    return tuple(out)


def recipe(
    cid: str,
    fn: str,
    sql: str,
    kind: str,
    expect: Any = None,
    surfaces=("uc",),
    args=(),
    **kw,
) -> Case:
    """A recipe row: the whole SQL statement a user writes (or a procedure embeds), run on the UC surface, with the
    session's value as the expectation. ``sql`` projects one row and names its answer ``out``; ``{q}`` is where a
    function's qualifier goes (the 3-level schema prefix on UC, nothing on a local session), so the statement reads
    ``SELECT {q}gbx_x(c) AS out FROM ...``. ``fn`` is the manifest function it exercises (the function the row is
    filed under; a recipe over several names the one that carries the claim), and ``kind``/``expect`` are the usual
    check on the one value. A ``twin`` row adapts a session row to another function; a recipe replaces the call itself.
    ``args`` are not the statement's arguments (it has none): they are what a reference kind (``ref``) computes its
    expected value from, so a recipe can share the session row's reference and its data.
    """
    from databricks.labs.gbx import manifest

    if fn not in manifest.load().functions:
        raise ValueError(f"recipe {cid}: {fn} is no manifest function")
    if not re.search(r"\bAS\s+`?out`?\b", sql, re.IGNORECASE):
        raise ValueError(f"recipe {cid}: the statement must name its answer `AS out`")
    return Case(cid, fn, tuple(args), kind, expect, tuple(surfaces), sql=sql, **kw)


def edges(_not_null=(), **extra):
    """The UC refusal and corrupt-tile rows of each function: `edges(width=(), bandmetadata=(1,))` takes the function's
    short name and the arguments after the tile. A function named in `_not_null` has no corrupt-tile row here (its own
    answer for a corrupt tile is a value, not NULL)."""
    rows = []
    for short, args in extra.items():
        fn = f"gbx_rst_{short}"
        rows.append(
            Case(
                f"uc_{short}_virtual_refused",
                fn,
                (Build("tile_virtual"), *args),
                "uc_refused",
                None,
                ("uc",),
            )
        )
        if short in _not_null:
            continue
        rows.append(
            Case(
                f"uc_{short}_corrupt_null",
                fn,
                (Build("tile_corrupt"), *args),
                "null",
                None,
                ("uc",),
            )
        )
    return tuple(rows)


def array_edges(**extra):
    """The UC refusal and corrupt-tile rows of each function over an ARRAY of tiles: `array_edges(combineavg=(),
    mapalgebra=("A * 2",))` takes the function's short name and the arguments after the array. Per function: an array
    of one virtual tile and a stack with a virtual member are refused with a Fix, so is a stack whose members and
    result are over the UC cap, and an array of nothing but a corrupt tile is NULL."""
    rows = []
    for short, args in extra.items():
        fn = f"gbx_rst_{short}"
        for cid, build, kind in (
            ("virtual_refused", "aa_virtual_only", "uc_refused"),
            ("virtual_member_refused", "aa_stack_virtual_member", "uc_refused"),
            ("over_cap_sum_refused", "aa_over_cap_sum", "uc_refused"),
            ("corrupt_null", "aa_corrupt_only", "null"),
        ):
            rows.append(
                Case(
                    f"uc_{short}_{cid}", fn, (Build(build), *args), kind, None, ("uc",)
                )
            )
    return tuple(rows)
