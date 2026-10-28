"""Shared ``--help`` handling for the examples/tests launchers.

A launcher builds a JAR, stages artifacts to a Volume, or submits a job. Asking one for
``--help`` must never do any of that, so each calls ``exit_if_help(__doc__)`` as the first
line of ``main()``: it prints the module docstring and exits 0 before any config is read,
any SDK client is built or any build is started.
"""

import sys

_FALLBACK = "Configuration is read from the environment (see databricks_cluster_config.example.env)."


def exit_if_help(doc, argv=None):
    """Print ``doc`` and exit 0 if ``--help`` or ``-h`` is among the arguments."""
    args = sys.argv[1:] if argv is None else argv
    if "--help" in args or "-h" in args:
        print((doc or _FALLBACK).strip())
        sys.exit(0)
