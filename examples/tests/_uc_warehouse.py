"""Fail fast when the SQL warehouse a UC command was given does not exist.

A warehouse id lives in the untracked cluster config (GBX_UC_GATE_WAREHOUSE_ID, GBX_UC_WAREHOUSE_ID) and goes
stale when the warehouse is deleted. Without this check the gate builds a wheel, stages it and installs the
functions before the first statement reaches the warehouse and fails. One GET answers it first.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

UC_WAREHOUSE_KEYS = ("GBX_UC_GATE_WAREHOUSE_ID", "GBX_UC_WAREHOUSE_ID")
_DEAD_STATES = ("DELETED", "DELETING")


def config_key_of(warehouse_id: str, config: Optional[Path]) -> Optional[str]:
    """The key in the cluster config that holds `warehouse_id`, or None (the id came from --warehouse-id)."""
    if config is None or not Path(config).is_file():
        return None
    text = Path(config).read_text()
    for key in UC_WAREHOUSE_KEYS:
        ids = [i.strip().strip("\"'") for i in re.findall(rf"^{key}=(.*)$", text, flags=re.M)]
        if ids and ids[-1] == warehouse_id:
            return key
    return None


def warehouse_problem(w, warehouse_id: str, config: Optional[Path] = None, profile: str = "") -> Optional[str]:
    """None when the warehouse exists and can be used; otherwise a message with a Fix."""
    key = config_key_of(warehouse_id, config)
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
        f"pass --warehouse-id <id> or set {key or 'GBX_UC_GATE_WAREHOUSE_ID'} in examples/tests/databricks_cluster_config.env "
        f"(list the warehouses with `databricks warehouses list --profile {profile or '<profile>'}`)"
    )
    return f"the SQL warehouse {warehouse_id} ({where}) {why} [{reason}].\nFix: {fix}."
