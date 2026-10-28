"""CREATE FUNCTION text for the UC surface: env 6, 3-level names, DEFAULTs through a SQL wrapper."""

from __future__ import annotations

import json
import re
from typing import Dict, List, Optional

from databricks.labs.gbx.manifest import sql_type

# One rule for every name that reaches a statement: a catalog or schema is checked against Unity
# Catalog's own character rules (so a hyphen is fine) and is ALWAYS emitted backtick-quoted; a string
# literal is always escaped. Function and parameter names come from the manifest ([a-z0-9_]).
_NAME = re.compile(r"[^\s./\\'\"`\x00-\x1f\x7f]+")
_UNSAFE_IN_PATH = re.compile(r"['\"\\\x00-\x1f\x7f]")
_VOLUME_FOLDER = re.compile(r"/Volumes/[^/]+/[^/]+/[^/]+(/.*)?")
_MANIFEST_NAME = re.compile(r"[a-z0-9_]+")


def quote_ident(kind: str, name: str) -> str:
    """`name` as a backtick-quoted identifier; ValueError (with a Fix:) for a name Unity Catalog does not allow."""
    if not isinstance(name, str) or not _NAME.fullmatch(name):
        raise ValueError(
            f"{kind} {name!r} is not a Unity Catalog name: it must be non-empty, without spaces, periods, "
            f"slashes, quotes, backticks or control characters. Fix: pass the {kind} name as Catalog Explorer shows it."
        )
    return f"`{name}`"


def quote_str(text: str) -> str:
    """`text` as a SQL string literal (backslash and quote escaped)."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def check_volume_dir(path: str) -> str:
    """The folder holding the wheel: an absolute /Volumes/<catalog>/<schema>/<volume>/... path, nothing that needs escaping."""
    if (
        not isinstance(path, str)
        or _UNSAFE_IN_PATH.search(path)
        or not _VOLUME_FOLDER.fullmatch(path.rstrip("/"))
    ):
        raise ValueError(
            f"volume_dir {path!r} must be an absolute /Volumes/<catalog>/<schema>/<volume>/... path without "
            "quotes, backslashes or control characters. Fix: pass the Volume folder that holds the wheel, "
            "for example /Volumes/main/geobrix/artifacts/env6."
        )
    return path


def function_ddl(
    fn,
    catalog: str,
    schema: str,
    wheel_uri: str,
    deps: Dict[str, str],
    version: str,
    style: str = "scalar",
    name: Optional[str] = None,
) -> List[str]:
    """One Python function, plus a SQL wrapper that carries the DEFAULTs when a parameter has one.

    A UC Python function cannot declare a DEFAULT; the SQL wrapper takes the public name and calls the
    Python function, which is then named ``gbx__<name>``.

    ``style="batch"`` makes a ``PARAMETER STYLE PANDAS`` function: its handler is the manifest handler's
    ``<attr>_batch`` and it takes an iterator of Series. ``name`` replaces ``fn.sql_name`` (a probe
    function is created beside the product one, never over it).
    """
    if style not in ("scalar", "batch"):
        raise ValueError(f"style must be 'scalar' or 'batch' (got {style!r})")
    sql_name = fn.sql_name if name is None else name
    q = f"{quote_ident('catalog', catalog)}.{quote_ident('schema', schema)}"
    if _UNSAFE_IN_PATH.search(wheel_uri):
        raise ValueError(
            f"wheel_uri {wheel_uri!r} has a quote, backslash or control character. "
            "Fix: stage the wheel under a plain /Volumes path."
        )
    if not _MANIFEST_NAME.fullmatch(sql_name):
        raise ValueError(f"function name {sql_name!r} is not a plain manifest name")
    l4 = fn.levels["L4"]
    mod, _, attr = l4["handler"].partition(":")
    has_default = any(p.default for p in fn.params)
    impl = f"gbx__{sql_name[len('gbx_'):]}" if has_default else sql_name
    params = ", ".join(f"{p.name} {sql_type(p.uc)}" for p in fn.params)
    env = json.dumps(
        [f"geobrix @ {wheel_uri}"] + [f"{n}=={deps[n]}" for n in l4["deps"]]
    )
    comment = quote_str(f"geobrix {version}")
    language = "LANGUAGE PYTHON\n" + (
        "PARAMETER STYLE PANDAS\n" if style == "batch" else ""
    )
    handler = f"{attr}_batch" if style == "batch" else attr
    stmts = [
        f"CREATE OR REPLACE FUNCTION {q}.{impl}({params})\n"
        f"RETURNS {sql_type(fn.returns_uc)}\n"
        f"{language}DETERMINISTIC\nCOMMENT {comment}\n"
        f"ENVIRONMENT (dependencies = {quote_str(env)}, environment_version = '6')\n"
        f"HANDLER 'handler'\nAS $$\nfrom {mod} import {handler} as handler\n$$"
    ]
    if has_default:
        sig = ", ".join(
            f"{p.name} {sql_type(p.uc)}"
            + (f" DEFAULT {p.default}" if p.default else "")
            for p in fn.params
        )
        args = ", ".join(p.name for p in fn.params)
        stmts.append(
            f"CREATE OR REPLACE FUNCTION {q}.{sql_name}({sig})\n"
            f"RETURNS {sql_type(fn.returns_uc)}\n"
            f"LANGUAGE SQL\nCOMMENT {comment}\nRETURN {q}.{impl}({args})"
        )
    return stmts
