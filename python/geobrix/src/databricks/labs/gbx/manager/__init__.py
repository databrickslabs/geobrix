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
) -> Dict[str, Choice]:
    """Register GeoBrix SQL functions on the best level this session offers.

    With no ``only`` it registers every GeoBrix SQL function and the light data sources (``raster_gbx`` and
    the others). ``target='uc'`` verifies the Unity Catalog functions instead of registering session ones.
    ``only`` limits the names (SQL names or short names, e.g. ``bng_eastnorthasbng``).
    Returns the level chosen for each name. A name no level can serve here is skipped (one line
    says why) and the others still register. A reason that several names share prints once, with a count.
    ``_levels`` forces a level and is for tests.

    A generator (a function that returns many rows per input) is a table function: call it as
    ``FROM t, LATERAL gbx_...(args)``. That form works at every level. The select-list form
    (``SELECT gbx_...(args)``, and ``LATERAL VIEW``) exists only where the Catalyst generator is
    installed (L1, dedicated clusters): a light generator is a Python table function, which Spark
    does not resolve there, and the light tier has never offered that form.
    """
    if target not in ("session", "uc"):
        raise ValueError(f"target must be 'session' or 'uc' (got {target!r})")
    from databricks.labs.gbx.manager import _legacy

    spark = spark if spark is not None else _active()
    messages.reminder(spark)
    man = manifest.load()

    def norm(n: str) -> str:
        n = n.lower()
        return n if n.startswith("gbx_") else f"gbx_{n}"

    requested = None if only is None else [norm(n) for n in only]
    legacy_all = _legacy.names()
    if requested is None:
        names = [n for n, f in man.functions.items() if f.status != "mvp"]
        bridge = sorted(legacy_all - set(man.functions))
    else:
        unknown = [
            n for n in requested if n not in man.functions and n not in legacy_all
        ]
        if unknown:
            raise ValueError(f"register(only=...) got unknown name(s): {unknown}")
        names = [n for n in requested if n in man.functions]
        bridge = [n for n in requested if n not in man.functions]
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
    if overrides:  # a forced level may follow a JAR staged since the last call
        capabilities.forget(spark)
    need_jvm = any(lv in man.functions[n].levels for n in names for lv in ("L1", "L2"))
    caps = capabilities.detect(spark, need_jvm)
    out: Dict[str, Choice] = {}
    lines: List[Tuple[str, str]] = []
    for n in names:
        fn = man.functions[n]
        try:
            choice = levels.select(fn, caps, overrides.get(n))
        except levels.NoLevelAvailable as exc:
            # The rest still register; the skipped name is reported once.
            why = "; ".join(r.removeprefix("geobrix: ") for r in exc.reasons)
            lines.append((n, f"geobrix: {n} not registered ({why})"))
            continue
        out[n] = _install(spark, fn, choice, caps, forced=n in overrides, lines=lines)
    if bridge:
        done, skipped = _legacy.register_unmigrated(spark, bridge)
        lines += skipped
        out.update({n: Choice("L3", "legacy", ()) for n in bridge if n in done})
    if only is None:
        from databricks.labs.gbx.ds import register as _sources

        _sources.register(spark)
    messages.say_grouped(spark, lines)
    from databricks.labs.gbx import _api

    _api.mark_registered(spark, out.keys())
    return out


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
