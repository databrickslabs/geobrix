"""SQL kernel bodies: the text of a manifest `sql` key, its helpers, and inlining.

A body is the text after ``RETURN``. ``{q}`` marks where a helper's qualifier goes: the 3-level schema prefix on Unity
Catalog, nothing in the session. A table function cannot call a SQL function (UC refuses one inside a generator), so a
table function over a SQL kernel, and the session's temporary function, carry the body with its helpers INLINED: each
``{q}gbx__x(args)`` is replaced by the helper's body with its parameters bound to the (parenthesised) arguments.

Pure Python, no pyspark: it runs where the DDL is built.

A body must not use a parameter name as a lambda variable or an alias: inlining renames identifiers outside quotes
(matched case-sensitively, whole words only, never after ``AS``, so ``CAST(x AS STRING)`` survives a parameter named
``string``; a backtick-quoted parameter is renamed too, except after ``AS`` or ``.``). Spark identifiers are
case-insensitive, so :func:`case_mismatches` finds a body that spells a parameter in another case. Helper calls may nest, in a body, in a helper and in an
argument: every path (the UC call, the session's L0 function, a table function) resolves them the same way.
"""

from __future__ import annotations

import re
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from databricks.labs.gbx.manifest import SQL_FROM_L0

Q = "{q}"
_CALL = re.compile(r"\{q\}(gbx__[a-z0-9_]+)\(")
_QUOTED = re.compile(r"'(?:[^'\\]|\\.|'')*'|`[^`]*`|\"(?:[^\"\\]|\\.)*\"")
_MAX_DEPTH = 16


def in_dependency_order(helpers: Sequence[Mapping]) -> tuple:
    """``helpers`` with each one after the helpers its body calls, the declared order kept otherwise.

    Unity Catalog resolves a SQL body when the function is created, so a caller made before its callee is refused.
    """
    by_name = {h["name"]: h for h in helpers}
    out: List[Mapping] = []
    done: set = set()

    def visit(h: Mapping, path: Tuple[str, ...]) -> None:
        if h["name"] in done or h["name"] in path:
            return
        for callee in _CALL.findall(h["sql"]):
            if callee in by_name and callee != h["name"]:
                visit(by_name[callee], path + (h["name"],))
        done.add(h["name"])
        out.append(h)

    for h in helpers:
        visit(h, ())
    return tuple(out)


def l4_source(fn) -> Optional[Tuple[str, tuple]]:
    """``(body, helpers)`` of the UC function's SQL kernel, or None when the row has none."""
    l4 = fn.levels.get("L4", {})
    body = l4.get("sql")
    if body is None:
        return None
    if body == SQL_FROM_L0:
        l0 = fn.levels["L0"]
        return l0["sql"], in_dependency_order(l0.get("sql_helpers", ()))
    return body, in_dependency_order(l4.get("sql_helpers", ()))


def l0_source(fn) -> Optional[Tuple[str, tuple]]:
    """``(body, helpers)`` of the session's SQL kernel, or None when the row has no L0."""
    l0 = fn.levels.get("L0")
    if l0 is None:
        return None
    return l0["sql"], in_dependency_order(l0.get("sql_helpers", ()))


def _segments(text: str) -> List[Tuple[bool, str]]:
    """``text`` as (is_code, piece) runs: quoted strings and backtick names are not code."""
    out, pos = [], 0
    for m in _QUOTED.finditer(text):
        if m.start() > pos:
            out.append((True, text[pos : m.start()]))
        out.append((False, m.group(0)))
        pos = m.end()
    if pos < len(text):
        out.append((True, text[pos:]))
    return out


def render(body: str, prefix: str) -> str:
    """``body`` with every ``{q}`` in code replaced by ``prefix`` (``\\`cat\\`.\\`sch\\`.`` on UC); a ``{q}`` inside a
    string literal or a backtick name is text, as it is for :func:`inlined`."""
    return "".join(
        piece.replace(Q, prefix) if code else piece for code, piece in _segments(body)
    )


_AFTER_AS = re.compile(r"\bas\s+$", re.IGNORECASE)
_WORD = re.compile(r"(?<![\w.$])(\w+)(?![\w(])")


def case_mismatches(body: str, names: Sequence[str]) -> List[Tuple[str, str]]:
    """``(declared, found)`` for each identifier of ``body`` that spells a name of ``names`` in another case.

    Spark identifiers are case-insensitive and :func:`rename` is not, so such a spelling would reach the engine unbound.
    The same places are skipped as in :func:`rename`: quoted text (but a backtick name is read), a field after ``.``, a
    call and what follows ``AS``.
    """
    lower = {n.lower(): n for n in names}
    found: List[Tuple[str, str]] = []
    seen = ""
    for code, piece in _segments(body):
        if code:
            for m in _WORD.finditer(piece):
                if _AFTER_AS.search(piece[: m.start()]):
                    continue
                declared = lower.get(m.group(1).lower())
                if declared is not None and declared != m.group(1):
                    found.append((declared, m.group(1)))
        elif piece.startswith("`") and not (
            _AFTER_AS.search(seen) or seen.endswith(".")
        ):
            declared = lower.get(piece[1:-1].lower())
            if declared is not None and declared != piece[1:-1]:
                found.append((declared, piece[1:-1]))
        seen += piece
    return found


def rename(body: str, mapping: Mapping[str, str]) -> str:
    """``body`` with each identifier in ``mapping`` replaced by its value. The match is case-sensitive and a whole word;
    quoted text is untouched, except a backtick-quoted name that IS a parameter and does not follow ``AS`` or ``.``; an
    identifier after ``.`` (a field access), before ``(`` (a function call) or after ``AS`` (a type or an alias) is not
    the parameter.
    """
    mapping = {k: v for k, v in mapping.items() if k != v}
    if not mapping:
        return body
    pat = re.compile(
        r"(?<![\w.$])(" + "|".join(map(re.escape, mapping)) + r")(?![\w(])"
    )

    def sub_code(piece: str) -> str:
        def one(m):
            if _AFTER_AS.search(piece[: m.start()]):
                return m.group(0)
            return mapping[m.group(1)]

        return pat.sub(one, piece)

    out = []
    for code, piece in _segments(body):
        if code:
            out.append(sub_code(piece))
        elif piece.startswith("`") and piece[1:-1] in mapping:
            before = "".join(out)
            if _AFTER_AS.search(before) or before.endswith("."):
                out.append(
                    piece
                )  # `expr AS `k``, `t.`k``: an alias or a field, not the parameter
            else:
                out.append(mapping[piece[1:-1]])
        else:
            out.append(piece)
    return "".join(out)


def _call_end(text: str, open_idx: int) -> Tuple[List[str], int]:
    """The top-level arguments of the call whose ``(`` is at ``open_idx``, and the index after its ``)``."""
    depth, args, start, i = 0, [], open_idx + 1, open_idx
    quoted = [(m.start(), m.end()) for m in _QUOTED.finditer(text)]
    qi = 0
    while i < len(text):
        while qi < len(quoted) and quoted[qi][1] <= i:
            qi += 1
        if qi < len(quoted) and quoted[qi][0] <= i < quoted[qi][1]:
            i = quoted[qi][1]
            continue
        ch = text[i]
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth == 0:
                args.append(text[start:i].strip())
                return ([] if args == [""] else args), i + 1
        elif ch == "," and depth == 1:
            args.append(text[start:i].strip())
            start = i + 1
        i += 1
    raise ValueError(f"unbalanced call in SQL body: {text[open_idx:open_idx + 60]!r}")


def inline_helpers(body: str, helpers: Sequence[Mapping], _depth: int = 0) -> str:
    """``body`` with every helper call replaced by the helper's body, arguments bound; recursive.

    A helper the body names that is not declared, a wrong argument count and a cycle are ValueErrors.
    """
    if _depth > _MAX_DEPTH:
        raise ValueError("SQL helpers nest too deeply (a cycle?)")
    by_name: Dict[str, Mapping] = {h["name"]: h for h in helpers}
    out, pos = [], 0
    while True:
        m = _CALL.search(body, pos)
        if m is None:
            out.append(body[pos:])
            return "".join(out)
        if not _in_code(body, m.start()):
            out.append(body[pos : m.end()])
            pos = m.end()
            continue
        h = by_name.get(m.group(1))
        if h is None:
            raise ValueError(
                f"SQL body calls {m.group(1)}, which is not a declared helper"
            )
        args, end = _call_end(body, m.end() - 1)
        args = [
            inline_helpers(a, helpers, _depth) for a in args
        ]  # a call nested in an argument
        params = [p[0] for p in h["params"]]
        if len(args) != len(params):
            raise ValueError(
                f"{m.group(1)} takes {len(params)} argument(s), the body passes {len(args)}"
            )
        inner = inline_helpers(h["sql"], helpers, _depth + 1)
        bound = rename(inner, {p: f"({a})" for p, a in zip(params, args)})
        out.append(body[pos : m.start()])
        out.append(f"({bound})")
        pos = end


def _in_code(text: str, idx: int) -> bool:
    return not any(m.start() <= idx < m.end() for m in _QUOTED.finditer(text))


def inlined(body: str, helpers: Sequence[Mapping]) -> str:
    """``body`` with its helpers inlined and no ``{q}`` left in code (a body that holds one is a ValueError)."""
    text = inline_helpers(body, helpers)
    if any(code and Q in piece for code, piece in _segments(text)):
        raise ValueError("SQL body holds a {q} that is not a declared helper call")
    return text
