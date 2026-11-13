"""The gate target of a profile, and a fast refusal when the workspace lacks it.

The gates (`gbx:uc:tests`, `gbx:test:lakeflow-connector`) run in the workspace of the profile you pass. What they
need there is local config in the untracked cluster config, one set of keys per profile. The profile name has every
character outside letters and digits turned into `_`, and is added after `__`:

    GBX_GATE_CATALOG__<profile>          the catalog of the scratch schemas
    GBX_UC_GATE_WAREHOUSE_ID__<profile>  the SQL warehouse
    GBX_GATE_VOLROOT__<profile>          the Volume folder that holds the staged wheel (the UC gate only)

An option (`--catalog`, `--warehouse-id`, `--volroot`) beats the key. A profile with no warehouse key falls back to
GBX_UC_GATE_WAREHOUSE_ID, then GBX_UC_WAREHOUSE_ID. The run prints where each value came from.

A value that the workspace lacks goes stale or is a mix-up between workspaces. Without a check the gate builds a
wheel, stages it and installs the functions before the first statement fails. One GET per value answers it first.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

UC_WAREHOUSE_KEYS = ("GBX_UC_GATE_WAREHOUSE_ID", "GBX_UC_WAREHOUSE_ID")
GATE_WAREHOUSE_KEY = "GBX_UC_GATE_WAREHOUSE_ID"
GATE_CATALOG_KEY = "GBX_GATE_CATALOG"
GATE_VOLROOT_KEY = "GBX_GATE_VOLROOT"
_DEAD_STATES = ("DELETED", "DELETING")


def profile_key(base: str, profile: str) -> str:
    """`base` for one profile: the profile name after `__`, every character outside letters and digits as `_`."""
    return f"{base}__{re.sub(r'[^A-Za-z0-9]', '_', profile)}"


def config_value(config: Optional[Path], key: str) -> Optional[str]:
    """The last non-empty value of `key` in the cluster config (never sourced), or None."""
    if config is None or not Path(config).is_file():
        return None
    values = [v.strip().strip("\"'") for v in re.findall(rf"^{re.escape(key)}=(.*)$", Path(config).read_text(), flags=re.M)]
    values = [v for v in values if v]
    return values[-1] if values else None


def _warehouse_keys(profile: str = "") -> tuple:
    return ((profile_key(GATE_WAREHOUSE_KEY, profile),) if profile else ()) + UC_WAREHOUSE_KEYS


def gate_warehouse(config: Optional[Path], profile: str = "") -> tuple:
    """(id, key) of the gate warehouse for `profile`: its own key, then the global gate key, then the shared one."""
    for key in _warehouse_keys(profile):
        value = config_value(config, key)
        if value:
            return value, key
    return None, None


def apply_gate_target(a, config: Optional[Path], default_catalog: str, default_volroot: Optional[str] = None) -> None:
    """Fill `a.catalog`, `a.warehouse_id` and `a.volroot` that no option set, from the keys of `a.profile`.

    `a.target_source` records where each value came from: "option", a config key, or "default".
    """
    profile = getattr(a, "profile", None) or ""
    src = {}
    if a.catalog:
        src["catalog"] = "option"
    else:
        key = profile_key(GATE_CATALOG_KEY, profile) if profile else None
        value = config_value(config, key) if key else None
        a.catalog, src["catalog"] = (value, key) if value else (default_catalog, "default")
    if a.warehouse_id:
        src["warehouse"] = "option"
    else:
        a.warehouse_id, key = gate_warehouse(config, profile)
        src["warehouse"] = key or "none"
    if default_volroot is not None:
        if getattr(a, "volroot", None):
            src["volroot"] = "option"
        else:
            key = profile_key(GATE_VOLROOT_KEY, profile) if profile else None
            value = config_value(config, key) if key else None
            a.volroot, src["volroot"] = (value, key) if value else (default_volroot, "default")
    a.target_source = src


def target_line(a, profile: str) -> str:
    """One line for the run log: the workspace profile and each value with its source."""
    src = getattr(a, "target_source", {})
    parts = [f"catalog {a.catalog} ({src.get('catalog', '?')})", f"warehouse {a.warehouse_id or '-'} ({src.get('warehouse', '?')})"]
    if getattr(a, "volroot", None):
        parts.append(f"volroot {a.volroot} ({src.get('volroot', '?')})")
    return f"target: profile {profile}: " + "; ".join(parts)


def config_key_of(warehouse_id: str, config: Optional[Path], profile: str = "") -> Optional[str]:
    """The key in the cluster config that holds `warehouse_id`, or None (the id came from --warehouse-id)."""
    for key in _warehouse_keys(profile):
        if config_value(config, key) == warehouse_id:
            return key
    return None


def warehouse_problem(w, warehouse_id: str, config: Optional[Path] = None, profile: str = "") -> Optional[str]:
    """None when the warehouse exists and can be used; otherwise a message with a Fix."""
    key = config_key_of(warehouse_id, config, profile)
    where = f"{key} in {config}" if key else "--warehouse-id"
    try:
        info = w.warehouses.get(warehouse_id)
    except Exception as exc:  # noqa: BLE001 - any API error is a reason not to start a long run
        name = type(exc).__name__
        if name not in ("ResourceDoesNotExist", "NotFound", "PermissionDenied"):
            return f"could not look up the SQL warehouse {warehouse_id} ({where}): {name}: {str(exc)[:200]}"
        why = "was deleted or does not exist" if name != "PermissionDenied" else "does not exist, or you may not use it"
        reason = f"{name}: {str(exc)[:160]}"
    else:
        state = str(getattr(info.state, "value", info.state))
        if state not in _DEAD_STATES:
            return None
        why = f"is {state}"
        reason = f"state {state}"
    fix = (
        f"pass --warehouse-id <id> or set {key or (profile_key(GATE_WAREHOUSE_KEY, profile) if profile else GATE_WAREHOUSE_KEY)} in examples/tests/databricks_cluster_config.env "
        f"(list the warehouses with `databricks warehouses list --profile {profile or '<profile>'}`)"
    )
    return f"the SQL warehouse {warehouse_id} ({where}) {why} [{reason}].\nFix: {fix}."


def catalog_problem(w, catalog: str, config: Optional[Path] = None, profile: str = "") -> Optional[str]:
    """None when the workspace has the catalog; otherwise a message with a Fix."""
    key = profile_key(GATE_CATALOG_KEY, profile) if profile else GATE_CATALOG_KEY
    where = f"{key} in {config}" if config_value(config, key) == catalog else "--catalog or the default"
    try:
        w.catalogs.get(catalog)
        return None
    except Exception as exc:  # noqa: BLE001 - any API error is a reason not to start a long run
        name = type(exc).__name__
        if name not in ("ResourceDoesNotExist", "NotFound", "PermissionDenied"):
            return f"could not look up the catalog {catalog} ({where}): {name}: {str(exc)[:200]}"
        reason = f"{name}: {str(exc)[:160]}"
    why = "does not exist, or you may not use it" if "PermissionDenied" in reason else "does not exist"
    fix = (
        f"pass --catalog <name> or set {key} in examples/tests/databricks_cluster_config.env "
        f"(list the catalogs with `databricks catalogs list --profile {profile or '<profile>'}`)"
    )
    return f"the catalog {catalog} ({where}) {why} in the workspace of profile {profile or '<profile>'} [{reason}].\nFix: {fix}."
