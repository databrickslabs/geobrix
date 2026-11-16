"""Create or update a raster-genie slice Genie Agent from `agent_spec` and print its space id.

    python seed_agent.py --profile <name> --warehouse-id <id> [--variant live|precomputed]
                         [--space-id <id>] [--dry-run]

Without `--space-id` a new space is created; with it the space is updated in place (its serialized
space and title are replaced by the ones built from `agent_spec`). `--variant live` (the default)
builds the space that computes every answer from the COGs; `--variant precomputed` builds the one
that also holds the saved severity table. `--dry-run` prints the instruction counts and touches
nothing. Record the printed ids as `GENIE_SPACE_ID` (live) and `GENIE_SPACE_ID_PRECOMPUTED` in
`config_nb`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import agent_spec as spec  # noqa: E402

from databricks.labs.gbx.genie.space import (  # noqa: E402
    apply_space,
    build_serialized_space,
)

TITLES = {
    "live": "raster-genie — Smokehouse Creek (live)",
    "precomputed": "raster-genie — Smokehouse Creek (precomputed)",
}
PARENT_PATH = "/Workspace/Users/mjohns@databricks.com/geobrix/raster-genie"


def main(argv=None, client=None) -> str:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile", help="Databricks config profile")
    ap.add_argument(
        "--warehouse-id", required=True, help="SQL warehouse the space runs on"
    )
    ap.add_argument("--space-id", help="update this space instead of creating one")
    ap.add_argument(
        "--variant",
        choices=sorted(spec.VARIANTS),
        default="live",
        help="live: every answer computed from the COGs; precomputed: also holds burn_severity_summary",
    )
    ap.add_argument("--title", help="space title (default: by variant)")
    ap.add_argument(
        "--parent-path", default=PARENT_PATH, help="workspace folder of a new space"
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="print the counts, change nothing"
    )
    args = ap.parse_args(argv)

    tables, functions, text, examples = spec.VARIANTS[args.variant]
    title = args.title or TITLES[args.variant]
    serialized = build_serialized_space(tables, functions, text, examples)
    count = len(functions) + len(examples) + 1
    print(
        f"... {args.variant}: {len(tables)} tables, {len(functions)} functions, {len(examples)} examples, "
        f"1 text block: {count} of 100 instructions"
    )
    if args.dry_run:
        print("... dry run: nothing changed")
        return args.space_id or ""
    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient(profile=args.profile)
    space_id = apply_space(
        title,
        args.warehouse_id,
        args.parent_path,
        serialized,
        space_id=args.space_id,
        client=client,
    )
    print(f"... {'updated' if args.space_id else 'created'} space: {space_id}")
    print(space_id)
    return space_id


if __name__ == "__main__":
    main()
