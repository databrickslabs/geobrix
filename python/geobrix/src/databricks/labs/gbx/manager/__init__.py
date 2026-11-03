"""The single registrar: each SQL name gets one implementation level, chosen here."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from databricks.labs.gbx import manifest
from databricks.labs.gbx.manager import capabilities, levels, messages
from databricks.labs.gbx.manager.levels import Choice


def _active():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def register(
    spark=None,
    target: str = "session",
    *,
    only: Optional[Iterable[str]] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    _levels: Optional[Dict[str, Tuple[str, str]]] = None,
    _prefer: Optional[Dict[str, str]] = None,
) -> Dict[str, Choice]:
    """Register GeoBrix SQL functions on the best level this session offers.

    With no ``only`` it registers every GeoBrix SQL function and the light data sources (``raster_gbx`` and
    the others). ``target='uc'`` verifies the Unity Catalog functions instead of registering session ones.
    ``only`` limits the names (SQL names or short names, e.g. ``bng_eastnorthasbng``).
    Returns the level chosen for each name. A name no level can serve here is skipped (one line
    says why) and the others still register. A reason that several names share prints once, with a count.
    ``_levels`` forces a level and is for tests. ``_prefer`` (name -> level) tries that level first and falls back
    as usual when it is unavailable; it is how a name is registered again at the level it already has.

    A generator (a function that returns many rows per input) is a table function: call it as
    ``FROM t, LATERAL gbx_...(args)``. That form works at every level. The select-list form
    (``SELECT gbx_...(args)``, and ``LATERAL VIEW``) exists only where the Catalyst generator is
    installed (L1, dedicated clusters): a light generator is a Python table function, which Spark
    does not resolve there, and the light tier has never offered that form.
    """
    if target not in ("session", "uc"):
        raise ValueError(f"target must be 'session' or 'uc' (got {target!r})")
    spark = spark if spark is not None else _active()
    messages.reminder(spark)
    man = manifest.load()

    def norm(n: str) -> str:
        n = n.lower()
        return n if n.startswith("gbx_") else f"gbx_{n}"

    requested = None if only is None else [norm(n) for n in only]
    if requested is None:
        names = [n for n, f in man.functions.items() if f.status != "mvp"]
    else:
        unknown = [n for n in requested if n not in man.functions]
        if unknown:
            raise ValueError(f"register(only=...) got unknown name(s): {unknown}")
        names = requested
    if target == "uc":
        from databricks.labs.gbx import uc

        uc.verify(
            spark,
            catalog,
            schema,
            [n for n in names if "L4" in man.functions[n].levels],
        )
        return {}
    overrides = _levels or {}
    prefer = _prefer or {}
    if overrides or only is None:
        # a forced level, or a full registration, may follow a JAR staged since the last call; the per-name first-use
        # registrations of the Python API (`only=[name]`) keep the session's probes
        capabilities.forget(spark)
    need_jvm = any(lv in man.functions[n].levels for n in names for lv in ("L1", "L2"))
    caps = capabilities.detect(spark, need_jvm)
    out: Dict[str, Choice] = {}
    lines: List[Tuple[str, str]] = []
    for n in names:
        fn = man.functions[n]
        try:
            choice = levels.select(fn, caps, overrides.get(n), prefer.get(n))
        except levels.NoLevelAvailable as exc:
            # The rest still register; the skipped name is reported once.
            why = "; ".join(r.removeprefix("geobrix: ") for r in exc.reasons)
            lines.append((n, f"geobrix: {n} not registered ({why})"))
            continue
        out[n] = _install(spark, fn, choice, caps, forced=n in overrides, lines=lines)
    if only is None:
        try:
            register_sources(spark)
        except (
            Exception
        ) as exc:  # noqa: BLE001 - the functions are registered; this failure is a line like any other
            why = (
                f"{type(exc).__name__}: {str(exc).splitlines()[0][:160]}"
                if str(exc)
                else type(exc).__name__
            )
            lines.append(
                (
                    "light data sources",
                    f"geobrix: light data sources not registered ({why})",
                )
            )
    messages.say_grouped(spark, lines)
    from databricks.labs.gbx import _api

    _api.mark_registered(spark, out.keys(), out)
    return out


def manifest_names(prefix: str = "gbx_") -> List[str]:
    """Every SQL name starting with ``prefix`` that the manager serves (the manifest rows that are not probes)."""
    return sorted(
        n
        for n, f in manifest.load().functions.items()
        if n.startswith(prefix) and f.status != "mvp"
    )


def register_sources(spark=None, only: Optional[Iterable[str]] = None) -> List[str]:
    """Register the light data sources (``raster_gbx``, ``gtiff_gbx`` and the others) and no functions.

    ``register(spark)`` does this as its last step; call this when a session needs only the readers and writers.
    ``only`` limits the formats (``raster_gbx`` or ``raster``); an unrecognized format raises ``ValueError``.
    Returns the registered format names.
    """
    from databricks.labs.gbx.ds import register as _ds

    return _ds._install(spark if spark is not None else _active(), only)


def names_at_level(prefixes: Iterable[str], level: str) -> list:
    """The names under ``prefixes`` that have a row at ``level``: the ``only=`` a pinned leg passes to
    ``register_family`` when it wants "every function of these prefixes that has this level" (a bench or parity
    leg over a whole family, where some names exist at L3 only)."""
    man = manifest.load()
    prefixes = (prefixes,) if isinstance(prefixes, str) else tuple(prefixes)
    return [
        n
        for n, f in man.functions.items()
        if f.status != "mvp" and n.startswith(prefixes) and level in f.levels
    ]


def register_family(
    spark,
    prefixes: Iterable[str],
    *,
    only: Optional[Iterable[str]] = None,
    level: Optional[str] = None,
) -> Dict[str, Choice]:
    """Register the names under ``prefixes`` (``('gbx_rst_',)``, ``('gbx_bng_', 'gbx_h3_')``), or ``only``, and
    nothing else: the part of a session a test, a bench leg or a self-check needs. ``level`` ('L1', 'L2' or 'L3')
    forces that level for them, so a test can compare two levels; without it the manager chooses, as in
    ``register(spark)``. With ``level``, a name that has no row at that level raises ``ValueError`` naming the names
    (with ``only`` or without), so a leg pinned to a level can never fall back to another one without saying so.
    """
    man = manifest.load()
    prefixes = (prefixes,) if isinstance(prefixes, str) else tuple(prefixes)
    if only is None:
        names = [
            n
            for n, f in man.functions.items()
            if f.status != "mvp" and n.startswith(prefixes)
        ]
    else:
        names = [n if n.lower().startswith("gbx_") else f"gbx_{n}" for n in only]
        names = [n.lower() for n in names]
    if level is not None:
        missing = sorted(
            n
            for n in names
            if n not in man.functions or level not in man.functions[n].levels
        )
        if missing:
            raise ValueError(
                f"register_family(level={level!r}): {len(missing)} name(s) have no {level} row in the manifest: "
                f"{', '.join(missing)}. Fix: pass only= with names that have {level} rows, or drop level= to let "
                f"the manager choose."
            )
    forced = None if level is None else {n: (level, "-") for n in names}
    return register(spark, only=names, _levels=forced) if names else {}


def _install(spark, fn, choice: Choice, caps, forced: bool, lines) -> Choice:
    """Install ``fn`` at ``choice``; each line it has to say goes into ``lines`` as (name, line)."""
    from databricks.labs.gbx.manager import l3

    reasons = list(choice.reasons)
    level = choice.level
    spec = fn.levels.get(level, {})
    if spec.get("engine") and choice.variant not in levels.ENGINE_L3:
        why = levels.engine_unavailable(fn, level, caps)
        if why:
            lines.append(
                (
                    fn.sql_name,
                    f"geobrix: {fn.sql_name} runs without the GDAL spec engine at {levels.NAMES[level]} ({why})",
                )
            )
    if level in ("L1", "L2"):
        from databricks.labs.gbx.manager import jvm

        install = jvm.install_l1 if level == "L1" else jvm.install_l2
        if level == "L1" and (
            choice.variant == levels.ENGINE or "companion" not in spec
        ):
            install = jvm.install_l1_engine
        why = install(spark, fn, caps)
        if why is None:
            for r in reasons:
                lines.append((fn.sql_name, f"{r}; using {levels.NAMES[level]}"))
            return choice
        if forced:
            raise RuntimeError(f"{fn.sql_name}: forced {level} failed: {why}")
        reasons.append(f"geobrix: {levels.NAMES[level]} unavailable: {why}")
        level = "L3"
    l3_spec = fn.levels["L3"]
    on_engine = (forced and choice.variant in levels.ENGINE_L3) or (
        not forced
        and l3_spec.get("engine")
        and levels.engine_unavailable(fn, "L3", caps) is None
    )
    if on_engine:
        form = choice.variant if choice.variant in levels.ENGINE_L3 else levels.ENGINE
        variant = l3.register_l3_engine(spark, fn, form)
        for r in reasons:
            lines.append((fn.sql_name, f"{r}; using {levels.NAMES['L3']}"))
        return Choice("L3", variant, tuple(reasons))
    variant, skip = (
        (choice.variant, None)
        if forced and choice.variant != "-"
        else l3.l3_variant(fn)
    )
    l3.register_l3(spark, fn, variant)
    for r in reasons:
        lines.append((fn.sql_name, f"{r}; using {levels.NAMES['L3']}"))
    if skip:
        lines.append((fn.sql_name, skip))
    return Choice("L3", variant, tuple(reasons))
