"""Helpers for the Unity Catalog rows of the raster tile functions (uc.tier_b): which session rows a UC function
shares, and the two rows every one adds (a virtual tile is refused with a Fix, a corrupt tile is NULL).
"""

from dataclasses import replace

from databricks.labs.gbx.bench.oracles import Build, Case

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
