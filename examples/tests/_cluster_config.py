"""Where the launchers read the untracked ``databricks_cluster_config.env`` from.

A launcher run from a clean detached worktree has no such file: the ``gbx:*`` command that started it
finds the main checkout's and names it in ``GBX_CLUSTER_CONFIG_ENV``. A file next to the launcher wins.
"""

import os
from pathlib import Path

CONFIG_NAME = "databricks_cluster_config.env"


def cluster_config_file(tests_dir: Path) -> Path:
    """The config file for the launcher in ``tests_dir``: its own, else the one the command resolved."""
    own = Path(tests_dir) / CONFIG_NAME
    pointed = os.environ.get("GBX_CLUSTER_CONFIG_ENV")
    if not own.exists() and pointed:
        return Path(pointed)
    return own
