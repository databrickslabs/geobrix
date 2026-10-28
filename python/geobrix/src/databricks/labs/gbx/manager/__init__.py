"""The single registrar: each SQL name gets one implementation level, chosen here."""

from __future__ import annotations

from typing import Dict, Iterable, Optional, Tuple

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

    ``target='uc'`` verifies the Unity Catalog functions instead of registering session ones.
    ``only`` limits the names (SQL names or short names, e.g. ``bng_eastnorthasbng``).
    Returns the level chosen for each name. A name no level can serve here is skipped (one line
    says why) and the others still register. ``_levels`` forces a level and is for tests.
    """
    if target not in ("session", "uc"):
        raise ValueError(f"target must be 'session' or 'uc' (got {target!r})")
    spark = spark if spark is not None else _active()
    messages.reminder(spark)
    man = manifest.load()
    names = (
        list(man.functions)
        if only is None
        else [
            n.lower() if n.lower().startswith("gbx_") else f"gbx_{n.lower()}"
            for n in only
        ]
    )
    unknown = [n for n in names if n not in man.functions]
    if unknown:
        raise ValueError(f"register(only=...) got unknown name(s): {unknown}")
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
    need_jvm = any(lv in man.functions[n].levels for n in names for lv in ("L1", "L2"))
    caps = capabilities.detect(spark, need_jvm)
    out: Dict[str, Choice] = {}
    for n in names:
        fn = man.functions[n]
        try:
            choice = levels.select(fn, caps, overrides.get(n))
        except levels.NoLevelAvailable as exc:
            # The rest still register; the skipped name is reported once.
            why = "; ".join(r.removeprefix("geobrix: ") for r in exc.reasons)
            messages.say(spark, f"geobrix: {n} not registered ({why})")
            continue
        out[n] = _install(spark, fn, choice, caps, forced=n in overrides)
    return out


def _install(spark, fn, choice: Choice, caps, forced: bool) -> Choice:
    from databricks.labs.gbx.manager import l3

    reasons = list(choice.reasons)
    level = choice.level
    if level in ("L1", "L2"):
        from databricks.labs.gbx.manager import jvm

        why = (jvm.install_l1 if level == "L1" else jvm.install_l2)(spark, fn, caps)
        if why is None:
            for r in reasons:
                messages.say(spark, f"{r}; using {levels.NAMES[level]}")
            return choice
        if forced:
            raise RuntimeError(f"{fn.sql_name}: forced {level} failed: {why}")
        reasons.append(f"geobrix: {levels.NAMES[level]} unavailable: {why}")
        level = "L3"
    variant, skip = (
        (choice.variant, None)
        if forced and choice.variant != "-"
        else l3.l3_variant(fn)
    )
    l3.register_l3(spark, fn, variant)
    for r in reasons:
        messages.say(spark, f"{r}; using {levels.NAMES['L3']}")
    if skip:
        messages.say(spark, skip)
    return Choice("L3", variant, tuple(reasons))
