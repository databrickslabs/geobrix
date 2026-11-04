"""gbx.uc.install / verify: deploy the UC surface from an env 6 notebook.

Importing this package pulls in no pyspark: the handlers module runs inside a UC Python sandbox.
"""

from __future__ import annotations

import os
import re
from typing import Iterable, List, Optional

from databricks.labs.gbx import __version__, manifest
from databricks.labs.gbx.uc import ddl, sqlbody

ENV = "6"  # the UC functions run on Serverless env 6 (their wheel is the env 6 wheel)
DEPS_MODES = ("per-function", "shared")


def _public_version() -> str:
    return __version__.split("+", 1)[0]


def _require_env6() -> None:
    env = os.environ.get("DATABRICKS_ENV_VERSION", "")
    rt = os.environ.get("DATABRICKS_RUNTIME_VERSION", "")
    major = rt.split(".")[0]
    if env == ENV or (major.isdigit() and int(major) >= 19):
        return
    raise RuntimeError(
        f"gbx.uc.install runs from an env {ENV} notebook or job (Serverless env {ENV} or DBR 19); "
        f"this runtime is env={env or '-'} runtime={rt or '-'}"
    )


def _env_wheel_name() -> str:
    return f"geobrix-{_public_version()}+env{ENV}-py3-none-any.whl"


def _single_wheel(volume_dir: str) -> str:
    """The one wheel the UC functions install from: the env 6 wheel of this version.

    A plain ``geobrix-<version>-py3-none-any.whl`` is accepted until the per-environment wheels
    (``+env5`` / ``+env6``) are published. A wheel of another environment is refused by name.
    """
    from databricks.labs.gbx.ds._listing import list_files

    pub = _public_version()
    found = list_files(
        volume_dir,
        filter_regex=rf".*/geobrix-{re.escape(pub)}[+-][^/]*\.whl$",
        recursive=False,
        raise_on_empty=False,
    )

    def env_of(path: str) -> Optional[str]:
        m = re.search(r"\+env([0-9]+)-", os.path.basename(path))
        return m.group(1) if m else None

    ok = [p for p in found if env_of(p) in (None, ENV)]
    other = [os.path.basename(p) for p in found if env_of(p) not in (None, ENV)]
    if ok and other:
        raise RuntimeError(
            f"{volume_dir} holds {os.path.basename(ok[0])} and {other[0]}, wheels for different environments. "
            f"Use one folder per environment and never put both environment wheels in one folder; "
            f"keep only {_env_wheel_name()} here"
        )
    if len(ok) == 1:
        return ok[0]
    if other:
        raise RuntimeError(
            f"{volume_dir} holds {other[0]}, a wheel for another environment; the UC functions run on "
            f"env {ENV}: stage {_env_wheel_name()} there (one wheel per environment folder)"
        )
    raise RuntimeError(
        f"{volume_dir} must hold exactly one geobrix-{pub}*.whl for env {ENV} (found {len(ok)}); "
        f"stage {_env_wheel_name()} there (one wheel per environment folder)"
    )


def _jar_name() -> str:
    return f"geobrix-{_public_version()}-jar-with-dependencies.jar"


def _single_jar(volume_dir: str) -> Optional[str]:
    """The geobrix JAR in ``volume_dir`` (the UC Scala handlers load it), or None when the folder holds none.

    Only the JAR with dependencies of THIS wheel's version is a candidate: a thin JAR has no JTS, and a JAR of another
    version answers with other code. The exact JAR is used when it is there, whatever else the folder holds (a sources
    JAR, a rollback copy). A folder with other geobrix JARs and not that one is refused, with the name to stage.
    """
    from databricks.labs.gbx.ds._listing import list_files

    found = list_files(
        volume_dir,
        filter_regex=r".*/geobrix[^/]*\.jar$",
        recursive=False,
        raise_on_empty=False,
    )
    ok = [p for p in found if os.path.basename(p) == _jar_name()]
    if len(ok) > 1:
        raise RuntimeError(
            f"{volume_dir} holds {_jar_name()} more than once ({', '.join(ok)}). Fix: keep one copy in the folder."
        )
    if not ok and found:
        other = sorted(os.path.basename(p) for p in found)
        raise RuntimeError(
            f"{volume_dir} holds {', '.join(other)}, not {_jar_name()} (the JAR with dependencies built with this "
            f"wheel). Fix: stage {_jar_name()} there and remove the others, or keep no geobrix JAR in the folder "
            "to create the functions as Python handlers."
        )
    return ok[0] if ok else None


def _full_name(n: str) -> str:
    return n.lower() if n.lower().startswith("gbx_") else f"gbx_{n.lower()}"


def _l4_names(only: Optional[Iterable[str]]) -> List[str]:
    man = manifest.load()
    uc_names = [n for n, f in man.functions.items() if "L4" in f.levels]
    if only is None:
        return uc_names
    want = [_full_name(n) for n in only]
    unknown = [n for n in want if n not in uc_names]
    if unknown:
        stub_only = [
            n for n in unknown if man.functions.get(n) and man.functions[n].stub
        ]
        hint = (
            f" ({stub_only} run in a session only: install(stubs=True) creates their stubs)"
            if stub_only
            else ""
        )
        raise ValueError(
            f"install(only=...) got name(s) with no UC function: {unknown}{hint}"
        )
    # A table function is a SQL body over another UC function, which has to exist when it is created.
    chosen = set(want) | {
        man.functions[n].levels["L4"]["tvf_over"]
        for n in want
        if "tvf_over" in man.functions[n].levels["L4"]
    }
    return [n for n in uc_names if n in chosen]


def install(
    catalog: str,
    schema: str,
    volume_dir: str,
    *,
    spark=None,
    only: Optional[Iterable[str]] = None,
    dry_run: bool = False,
    jar: Optional[str] = None,
    procedures: bool = False,
    stubs: bool = False,
    deps: str = "per-function",
) -> List[str]:
    """Create the UC functions in ``catalog.schema`` from the wheel in ``volume_dir``; return the DDL.

    The JVM-only BNG functions become ``LANGUAGE SCALA`` functions on the geobrix JAR when ``volume_dir`` holds one
    (``jar`` names a Volume JAR instead); without a JAR they are the Python handlers, and answer the same.

    ``dry_run`` only returns the DDL (the wheel name is the env 6 wheel's, not looked up, and no folder is read for a
    JAR: pass ``jar`` to see the Scala form).

    ``procedures=True`` also creates the governed ETL procedures (:mod:`databricks.labs.gbx.uc.procedures`), after
    every function they call, and verifies them with the functions. A procedure whose precondition does not hold is
    skipped, with one line saying why.

    A function with a SQL kernel (its L4 ``sql``) is a ``LANGUAGE SQL`` function, ahead of a Scala handler and a Python
    handler; its helper functions are created once, before the first function that calls them.

    ``stubs=True`` also creates, for each session-only SCALAR with a Fix text, a SQL function with the session
    signature that raises and says where the function runs (an aggregate or a generator gets none: the docs page lists
    them). With ``only``, a stub is created for each name it lists that has one.

    ``deps="shared"`` gives every Python function (and private routine) the same ENVIRONMENT: the manifest's whole
    dependency list, so a statement that calls several GeoBrix functions can share one environment. The default,
    ``"per-function"``, gives each function only the packages it imports.
    """
    # Everything that reaches a statement is checked before any statement is built or any folder read.
    ddl.quote_ident("catalog", catalog)
    ddl.quote_ident("schema", schema)
    ddl.check_volume_dir(volume_dir)
    if deps not in DEPS_MODES:
        raise ValueError(
            f"install(deps=...) must be one of {DEPS_MODES} (got {deps!r}). "
            "Fix: pass 'per-function' (each function its own packages) or 'shared' (one environment for all)."
        )
    shared = deps == "shared"
    man = manifest.load()
    stub_names: List[str] = []
    if stubs:
        every = [n for n, f in man.functions.items() if f.kind == "scalar" and f.stub]
        if only is None:
            stub_names = every
        else:
            stub_names = [n for n in map(_full_name, only) if n in every]
            only = [n for n in only if _full_name(n) not in stub_names]
    names = _l4_names(only)
    if not dry_run:
        _require_env6()
    wheel = (
        f"{volume_dir.rstrip('/')}/{_env_wheel_name()}"
        if dry_run
        else _single_wheel(volume_dir)
    )
    if jar is not None:
        ddl.check_jar_path(jar)
    elif not dry_run and any(ddl.needs_jar(man.functions[n]) for n in names):
        jar = _single_jar(
            volume_dir
        )  # only a selected function with a Scala handler needs a JAR
    stmts: List[str] = []
    made_private = set()
    made_helpers = set()
    for n in names:
        over = man.functions[n].levels["L4"].get("tvf_over")
        if over in man.private and over not in made_private:
            made_private.add(
                over
            )  # a table function is created after the private function it reads
            stmts.append(
                ddl.private_ddl(
                    man.private[over],
                    catalog,
                    schema,
                    "file://" + wheel,
                    man.uc_dependencies,
                    _public_version(),
                    shared_deps=shared,
                )
            )
        fn = man.functions[n]
        src = sqlbody.l4_source(fn)
        for h in (
            src[1] if src else ()
        ):  # a SQL kernel's helpers, once each, before the function that calls them
            if h["name"] not in made_helpers:
                made_helpers.add(h["name"])
                stmts.append(ddl.helper_ddl(h, fn, catalog, schema, _public_version()))
        stmts += ddl.function_ddl(
            fn,
            catalog,
            schema,
            "file://" + wheel,
            man.uc_dependencies,
            _public_version(),
            jar_uri=jar,
            shared_deps=shared,
            helpers=False,
        )
    stmts += [
        ddl.stub_ddl(man.functions[n], catalog, schema, _public_version())
        for n in stub_names
    ]
    procedure_names: List[str] = []
    if procedures:
        from databricks.labs.gbx.uc import procedures as _procedures

        # with `only`, a recipe whose functions are not all installed is skipped, not built over a missing function
        built, skipped = _procedures.build_all(
            catalog, schema, available=set(names) if only is not None else None
        )
        for name, why in skipped.items():
            print(f"geobrix: procedure {name} skipped: {why}")
        procedure_names = [name for name, _ in built]
        wanted = {c for n in procedure_names for c in _procedures.CALLS.get(n, ())}
        stmts += _private_for_procedures(
            man, catalog, schema, "file://" + wheel, made_private, wanted, shared
        )
        stmts += [text for _, text in built]
    verified = (
        names
        + sorted(made_helpers)
        + sorted(made_private)
        + stub_names
        + procedure_names
    )
    if not dry_run:
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
        for s in stmts:
            spark.sql(s)
        verify(spark, catalog, schema, verified)
    return stmts


# the private routines a procedure calls and no table function reads
_PROCEDURE_ONLY = ("gbx__h3_cellfill_cells", "gbx__st_asmvt_features")


def _private_for_procedures(
    man, catalog, schema, wheel_uri, made, wanted, shared: bool = False
) -> List[str]:
    """The DDL of the private routines the built procedures call (the ones no table function created), once each."""
    out = []
    for name in _PROCEDURE_ONLY:
        if name in wanted and name not in made:
            made.add(name)
            out.append(
                ddl.private_ddl(
                    man.private[name],
                    catalog,
                    schema,
                    wheel_uri,
                    man.uc_dependencies,
                    _public_version(),
                    shared_deps=shared,
                )
            )
    return out


def verify(
    spark, catalog: Optional[str], schema: Optional[str], names: Iterable[str]
) -> None:
    """register(target='uc'): every name exists in the schema, created by this wheel's version; else fail."""
    if not catalog or not schema:
        raise ValueError(
            "register(target='uc') needs catalog= and schema=. "
            "Fix: pass the catalog and schema the UC functions were installed in."
        )
    qc, qs = ddl.quote_ident("catalog", catalog), ddl.quote_ident("schema", schema)
    names = list(names)
    # Unity Catalog stores names lower-case whatever case they were created with: compare lower-case.
    rows = spark.sql(
        f"SELECT routine_name, comment FROM {qc}.information_schema.routines "
        f"WHERE lower(routine_schema) = {ddl.quote_str(schema.lower())}"
    ).collect()
    have = {r["routine_name"].lower(): r["comment"] for r in rows}
    # a function's COMMENT is `geobrix <version>: <summary>`: the text before the first colon is the version
    want = _public_version()
    bad = [n for n in names if ddl.version_of_comment(have.get(n.lower())) != want]
    if bad:
        raise RuntimeError(
            f"UC schema {catalog}.{schema} does not match wheel {__version__} for {bad} "
            f"(found {[have.get(n.lower()) for n in bad]}); run gbx.uc.install from an env {ENV} notebook"
        )
    print(
        f"geobrix: UC functions verified in {catalog}.{schema} ({__version__}); for unqualified names run: "
        f"SET PATH = {qc}.{qs}, system.session, system.builtin, system.ai, CURRENT_SCHEMA"
    )
