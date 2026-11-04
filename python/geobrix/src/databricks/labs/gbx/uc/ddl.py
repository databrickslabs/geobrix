"""CREATE FUNCTION text for the UC surface: env 6, 3-level names, DEFAULTs through a SQL wrapper."""

from __future__ import annotations

import json
import re
from typing import Dict, List, Optional

from databricks.labs.gbx.manifest import sql_type as _manifest_sql_type
from databricks.labs.gbx.uc import sqlbody

# How a UC function declares the manifest's GEOMETRY (measured on Serverless env 6): bare GEOMETRY is not a type
# there (UNSUPPORTED_DATATYPE), GEOMETRY(<srid>) takes that one SRID, and GEOMETRY(ANY) takes every SRID; a
# caller's GEOMETRY(<srid>) value reaches a GEOMETRY(ANY) parameter only through CAST(... AS GEOMETRY(ANY)).
UC_GEOMETRY = "GEOMETRY(ANY)"


def sql_type(t: str) -> str:
    """The SQL type of a manifest UC type: the manifest's text, with GEOMETRY declared as GEOMETRY(ANY)."""
    return UC_GEOMETRY if t.upper() == "GEOMETRY" else _manifest_sql_type(t)


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


def check_jar_path(path: str) -> str:
    """An explicit ``jar=``: an absolute /Volumes/... path ending in ``.jar``, nothing that needs escaping."""
    if (
        not isinstance(path, str)
        or _UNSAFE_IN_PATH.search(path)
        or not path.startswith("/Volumes/")
        or not path.endswith(".jar")
        or not _VOLUME_FOLDER.fullmatch(path.rsplit("/", 1)[0])
    ):
        raise ValueError(
            f"jar {path!r} must be an absolute /Volumes/<catalog>/<schema>/<volume>/.../<name>.jar path without "
            "quotes, backslashes or control characters. Fix: pass the JAR with dependencies staged on a Volume, "
            "for example /Volumes/main/geobrix/artifacts/env6/geobrix-<version>-jar-with-dependencies.jar."
        )
    return path


def comment_text(fn, version: str) -> str:
    """The COMMENT of a UC function: ``geobrix <version>: <summary>``. ``uc.verify`` and ``register`` read the text
    before the first colon as the version of the wheel that created it."""
    return quote_str(f"geobrix {version}: {fn.doc.summary}".rstrip(": "))


def version_of_comment(comment: Optional[str]) -> Optional[str]:
    """The version a COMMENT written by :func:`comment_text` names (None for a comment that is not geobrix's)."""
    head = (comment or "").split(":", 1)[0].strip()
    return head[len("geobrix ") :] if head.startswith("geobrix ") else None


FORMS = ("sql", "scala", "python")


def _params_sig(fn) -> str:
    return ", ".join(
        f"{p.name} {sql_type(p.uc)}" + (f" DEFAULT {p.default}" if p.default else "")
        for p in fn.params
    )


def _env_deps(names, deps: Dict[str, str], shared: bool) -> List[str]:
    """The ``name==version`` pins of a Python function's ENVIRONMENT: its own list, or (``shared``) every pin of the
    manifest's ``uc_dependencies``, so every function declares the same environment."""
    return [f"{n}=={deps[n]}" for n in (deps if shared else names)]


def chosen_form(fn, jar_uri: Optional[str] = None, style: str = "scalar") -> str:
    """The form a UC function is created in: ``sql`` (a SQL kernel) over ``scala`` (with a JAR) over ``python``."""
    l4 = fn.levels["L4"]
    if style == "scalar" and sqlbody.l4_source(fn) is not None:
        return "sql"
    if jar_uri is not None and "scala" in l4 and style == "scalar":
        return "scala"
    return "python"


def needs_jar(fn) -> bool:
    """True when ``fn`` is created as a Scala function if the geobrix JAR is there (it has a Scala handler and no SQL
    kernel, which would be chosen first)."""
    return "scala" in fn.levels["L4"] and sqlbody.l4_source(fn) is None


def helper_ddl(h, fn, catalog: str, schema: str, version: str) -> str:
    """One SQL helper of a SQL kernel (``gbx__<x>``, called as ``{q}gbx__<x>(...)`` in a body)."""
    q = f"{quote_ident('catalog', catalog)}.{quote_ident('schema', schema)}"
    params = ", ".join(f"{n} {sql_type(t)}" for n, t in h["params"])
    summary = h.get("summary") or f"SQL helper of {fn.sql_name}"
    return (
        f"CREATE OR REPLACE FUNCTION {q}.{h['name']}({params})\n"
        f"RETURNS {sql_type(h['returns'])}\n"
        f"LANGUAGE SQL\nDETERMINISTIC\nCOMMENT {quote_str(f'geobrix {version}: {summary}'.rstrip(': '))}\n"
        f"RETURN {sqlbody.render(h['sql'], q + '.')}"
    )


def sql_helper_names(fn) -> List[str]:
    """The names of the helpers the UC form of ``fn`` needs (empty without a SQL kernel)."""
    src = sqlbody.l4_source(fn)
    return [] if src is None else [h["name"] for h in src[1]]


def local_function_ddl(fn, src=None, name: Optional[str] = None) -> str:
    """The UC SQL kernel of ``fn`` as a TEMPORARY function with the UC signature and its helpers inlined, for a Spark
    session that has no Unity Catalog (the host oracle runs the UC rows through it). ``src`` is a ``(body, helpers)``
    pair that replaces the row's own kernel (a candidate body); ``name`` replaces ``fn.sql_name``.
    """
    src = src or sqlbody.l4_source(fn)
    if src is None:
        raise ValueError(f"{fn.sql_name} has no SQL kernel")
    body, hs = src
    return (
        f"CREATE OR REPLACE TEMPORARY FUNCTION {name or fn.sql_name}({_params_sig(fn)}) "
        f"RETURNS {sql_type(fn.returns_uc)} RETURN {sqlbody.inlined(body, hs)}"
    )


def function_ddl(
    fn,
    catalog: str,
    schema: str,
    wheel_uri: str,
    deps: Dict[str, str],
    version: str,
    style: str = "scalar",
    name: Optional[str] = None,
    jar_uri: Optional[str] = None,
    form: Optional[str] = None,
    shared_deps: bool = False,
    helpers: bool = True,
) -> List[str]:
    """One UC function (a SQL kernel, a Scala handler or a Python handler), plus a SQL wrapper that carries the
    DEFAULTs when a Python or Scala parameter has one.

    A UC Python function cannot declare a DEFAULT; the SQL wrapper takes the public name and calls the
    Python function, which is then named ``gbx__<name>``. A SQL function declares its DEFAULTs itself.

    ``style="batch"`` makes a ``PARAMETER STYLE PANDAS`` function: its handler is the manifest handler's
    ``<attr>_batch`` and it takes an iterator of Series. ``name`` replaces ``fn.sql_name`` (a probe
    function is created beside the product one, never over it).

    The form follows the preference ``sql`` (the row's SQL kernel, L4 ``sql``) over ``scala`` (``jar_uri``, the geobrix
    JAR on a Volume, and the row's L4 ``scala`` key) over ``python`` (the row's handler). ``form`` forces one of them
    (a probe times the forms against each other); a form the row cannot be is a ValueError. ``helpers=False`` leaves the
    SQL kernel's helper functions to the caller (``gbx.uc.install`` creates each once). ``shared_deps`` makes the
    ENVIRONMENT of a Python function the manifest's whole dependency list instead of the function's own.
    """
    if style not in ("scalar", "batch"):
        raise ValueError(f"style must be 'scalar' or 'batch' (got {style!r})")
    if form is not None and form not in FORMS:
        raise ValueError(f"form must be one of {FORMS} (got {form!r})")
    sql_name = fn.sql_name if name is None else name
    q = f"{quote_ident('catalog', catalog)}.{quote_ident('schema', schema)}"
    if _UNSAFE_IN_PATH.search(wheel_uri) or (
        jar_uri is not None and _UNSAFE_IN_PATH.search(jar_uri)
    ):
        raise ValueError(
            f"wheel_uri {wheel_uri!r} has a quote, backslash or control character. "
            "Fix: stage the wheel and the JAR under a plain /Volumes path."
        )
    if not _MANIFEST_NAME.fullmatch(sql_name):
        raise ValueError(f"function name {sql_name!r} is not a plain manifest name")
    l4 = fn.levels["L4"]
    if "tvf_over" in l4:
        return [
            _table_function_ddl(fn, q, sql_name, l4["tvf_over"], version, form=form)
        ]
    use = form or chosen_form(fn, jar_uri, style)
    if use == "sql":
        src = sqlbody.l4_source(fn)
        if src is None or style != "scalar":
            raise ValueError(f"{fn.sql_name} has no SQL kernel in a {style} style")
        return _sql_function_ddl(
            fn, q, sql_name, src, catalog, schema, version, helpers
        )
    if use == "scala" and (jar_uri is None or "scala" not in l4 or style != "scalar"):
        raise ValueError(
            f"{fn.sql_name} cannot be a Scala function here (needs a JAR and an L4 scala key)"
        )
    if use == "python" and "handler" not in l4:
        raise ValueError(f"{fn.sql_name} has no Python handler")
    has_default = any(p.default for p in fn.params)
    if not has_default:
        impl = sql_name
    elif sql_name.startswith("gbx_mvp_"):
        impl = f"{sql_name}_impl"  # a bench probe copy: its wrapper's implementation is a probe routine too
    else:
        impl = f"gbx__{sql_name[len('gbx_'):]}"
    params = ", ".join(f"{p.name} {sql_type(p.uc)}" for p in fn.params)
    comment = comment_text(fn, version)
    if use == "scala":
        jars = json.dumps([jar_uri])
        stmts = [
            f"CREATE OR REPLACE FUNCTION {q}.{impl}({params})\n"
            f"RETURNS {sql_type(fn.returns_uc)}\n"
            f"LANGUAGE SCALA\nDETERMINISTIC\nCOMMENT {comment}\n"
            f"ENVIRONMENT (java_dependencies = {quote_str(jars)}, environment_version = '6')\n"
            f"HANDLER {quote_str(l4['scala'])}"
        ]
    else:
        mod, _, attr = l4["handler"].partition(":")
        env = json.dumps(
            [f"geobrix @ {wheel_uri}"] + _env_deps(l4["deps"], deps, shared_deps)
        )
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
        stmts.append(
            f"CREATE OR REPLACE FUNCTION {q}.{sql_name}({_params_sig(fn)})\n"
            f"RETURNS {sql_type(fn.returns_uc)}\n"
            f"LANGUAGE SQL\nCOMMENT {comment}\nRETURN {q}.{impl}({', '.join(p.name for p in fn.params)})"
        )
    return stmts


def _sql_function_ddl(
    fn, q, sql_name, src, catalog, schema, version, helpers
) -> List[str]:
    """A LANGUAGE SQL function from the row's SQL kernel: DETERMINISTIC, its DEFAULTs declared on itself, no sandbox.
    Its helper functions come first when ``helpers``."""
    body, hs = src
    stmts = [helper_ddl(h, fn, catalog, schema, version) for h in hs] if helpers else []
    stmts.append(
        f"CREATE OR REPLACE FUNCTION {q}.{sql_name}({_params_sig(fn)})\n"
        f"RETURNS {sql_type(fn.returns_uc)}\n"
        f"LANGUAGE SQL\nDETERMINISTIC\nCOMMENT {comment_text(fn, version)}\n"
        f"RETURN {sqlbody.render(body, q + '.')}"
    )
    return stmts


def stub_ddl(fn, catalog: str, schema: str, version: str) -> str:
    """A function that exists only to say where it runs: LANGUAGE SQL, the session signature, and a body that raises with
    the row's Fix text. For a session-only SCALAR; an aggregate or a generator has none.
    """
    if fn.kind != "scalar" or fn.stub is None:
        raise ValueError(
            f"{fn.sql_name} has no stub (only a scalar row with a Fix text has one)"
        )
    q = f"{quote_ident('catalog', catalog)}.{quote_ident('schema', schema)}"
    params = ", ".join(
        f"{p.name} {sql_type(p.uc or p.session[0])}"
        + (f" DEFAULT {p.default}" if p.default else "")
        for p in fn.params
    )
    message = f"{fn.sql_name} runs in a notebook or job session. Fix: {fn.stub}"
    return (
        f"CREATE OR REPLACE FUNCTION {q}.{fn.sql_name}({params})\n"
        f"RETURNS {sql_type(fn.returns_uc or fn.returns_session)}\n"
        f"LANGUAGE SQL\nCOMMENT {comment_text(fn, version)}\n"
        f"RETURN raise_error({quote_str(message)})"
    )


def private_ddl(
    pf,
    catalog: str,
    schema: str,
    wheel_uri: str,
    deps: Dict[str, str],
    version: str,
    shared_deps: bool = False,
) -> str:
    """A private routine (``gbx__<x>``, a manifest ``uc_private`` entry): one Python function, no DEFAULTs, called only
    by a table function or a procedure of this package."""
    q = f"{quote_ident('catalog', catalog)}.{quote_ident('schema', schema)}"
    if _UNSAFE_IN_PATH.search(wheel_uri):
        raise ValueError(
            f"wheel_uri {wheel_uri!r} has a quote, backslash or control character. "
            "Fix: stage the wheel under a plain /Volumes path."
        )
    # a bench probe copy of a private routine is named gbx_mvp_perf_<name>, beside the product's and never over it
    if not re.fullmatch(r"gbx__[a-z0-9_]+|gbx_mvp_perf_[a-z0-9_]+", pf.name):
        raise ValueError(f"private routine name {pf.name!r} must be gbx__<name>")
    mod, _, attr = pf.handler.partition(":")
    params = ", ".join(f"{n} {sql_type(t)}" for n, t in pf.params)
    env = json.dumps([f"geobrix @ {wheel_uri}"] + _env_deps(pf.deps, deps, shared_deps))
    comment = quote_str(f"geobrix {version}: {pf.summary}".rstrip(": "))
    return (
        f"CREATE OR REPLACE FUNCTION {q}.{pf.name}({params})\n"
        f"RETURNS {sql_type(pf.returns)}\n"
        f"LANGUAGE PYTHON\nDETERMINISTIC\nCOMMENT {comment}\n"
        f"ENVIRONMENT (dependencies = {quote_str(env)}, environment_version = '6')\n"
        f"HANDLER 'handler'\nAS $$\nfrom {mod} import {attr} as handler\n$$"
    )


def _table_function_ddl(
    fn, q: str, sql_name: str, over: str, version: str, form: Optional[str] = None
) -> str:
    """A generator on UC is a SQL table function over the array function: one row per element.

    It takes the DEFAULTs itself and runs no Python of its own (the function it calls does). It calls the Python
    function itself: when the array function has a DEFAULT its public name is a SQL wrapper, and UC refuses a SQL
    function inside a generator ([UNSUPPORTED_SQL_UDF_USAGE], measured on Serverless env 6).

    The same refusal applies to an array function that is a SQL kernel (L4 ``sql``): the table function carries the
    kernel's expression INLINED (its helpers too, its parameters renamed to the table function's) instead of calling it.
    ``form="python"`` keeps the call.
    """
    from databricks.labs.gbx import manifest

    inner = over
    private = manifest.load().private.get(over)
    src = (
        None
        if private is not None or form == "python"
        else sqlbody.l4_source(manifest.load().functions[over])
    )
    if (
        private is None
        and src is None
        and any(p.default for p in manifest.load().functions[over].params)
    ):
        over = f"gbx__{over[len('gbx_'):]}"
    cols = ", ".join(f"{n} {sql_type(t)}" for n, t in fn.columns)
    sig = _params_sig(fn)
    args = ", ".join(p.name for p in fn.params)
    if src is not None:
        inner_fn = manifest.load().functions[inner]
        if len(inner_fn.params) != len(fn.params):
            raise ValueError(
                f"{fn.sql_name}: {inner} takes {len(inner_fn.params)} argument(s), the table function {len(fn.params)}"
            )
        call = sqlbody.rename(
            sqlbody.inlined(*src),
            {a.name: b.name for a, b in zip(inner_fn.params, fn.params)},
        )
        call = f"({call})"
    else:
        call = f"{q}.{over}({args})"
    # an array of structs is one row per element with the struct's fields as columns (`inline`); an array of single
    # values is one row per element in the table's one column (`explode`)
    returns = (
        private.returns
        if private is not None
        else manifest.load().functions[inner].returns_uc
    )
    rows = (
        f"inline({call})"
        if sql_type(returns).lower().startswith("array<struct")  # ARRAY<TILE> is one
        else f"explode({call}) AS {fn.columns[0][0]}"
    )
    return (
        f"CREATE OR REPLACE FUNCTION {q}.{sql_name}({sig})\n"
        f"RETURNS TABLE ({cols})\n"
        f"COMMENT {comment_text(fn, version)}\n"
        f"RETURN {_tvf_query(fn, rows)}"
    )


def _tvf_query(fn, rows: str) -> str:
    """The query a table function returns: the rows of the function it is over (``rows``, an ``inline`` or ``explode``), or,
    with the row's L4 ``tvf_select``, a projection of them. THE one place that shapes a projected table function.

    The projection form puts a generator over a Python function inside a subquery and projects its fields in the outer
    SELECT, so one call serves every statistic that is a function of the same state. It is waiting for the workspace
    pass: UC refuses a SQL function inside a generator, and whether it takes this shape over a Python one is not proven
    on the host. If it refuses, the fallback is here: one Python array function per statistic under the same names, and
    this function returns ``SELECT {rows}`` over that function.
    """
    select = fn.levels["L4"].get("tvf_select")
    return f"SELECT {select} FROM (SELECT {rows})" if select else f"SELECT {rows}"
