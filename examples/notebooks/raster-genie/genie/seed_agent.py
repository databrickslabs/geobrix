"""Create or update a raster-genie slice Genie Agent from `agent_spec` and print its space id.

    python seed_agent.py --profile <name> --warehouse-id <id> [--variant live|precomputed]
                         [--catalog <c>] [--schema <s>] [--uc-schema <s>] [--space-id <id>] [--dry-run]

Without `--space-id` the space with the variant's title under the parent folder is updated in place, or a new
one is created when there is none; with it that space is updated (its serialized space and title are replaced
by the ones built from `agent_spec`). `--variant live` (the default)
builds the space that computes every answer from the COGs; `--variant precomputed` builds the one
that also holds the saved severity table. `--dry-run` prints the instruction counts and touches
nothing. `--catalog`, `--schema` and `--uc-schema` name where the tables and the GeoBrix
functions live: pass the ones `config_nb` sets (the defaults are `agent_spec`'s). Record the printed
ids as `GENIE_SPACE_ID` (live) and `GENIE_SPACE_ID_PRECOMPUTED` in `config_nb`.
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
SPACES_PATH = "/api/2.0/genie/spaces"


def default_parent_path(client) -> str:
    """The folder of a new space: `/Workspace/Users/<current user>/geobrix/raster-genie`."""
    return f"/Workspace/Users/{client.current_user.me().user_name}/geobrix/raster-genie"


def _norm_path(path: str | None) -> str:
    """A workspace folder without the `/Workspace` prefix or a trailing slash (the API returns it either way)."""
    path = (path or "").rstrip("/")
    return path[len("/Workspace") :] if path.startswith("/Workspace/") else path


def find_space(client, title: str, parent_path: str) -> str | None:
    """The id of the space called `title` under `parent_path`, or None; two matches are an error."""
    found, token = [], None
    while True:
        query = {"page_size": 100, **({"page_token": token} if token else {})}
        resp = client.api_client.do("GET", SPACES_PATH, query=query) or {}
        found += [
            sp["space_id"] for sp in resp.get("spaces", []) if sp.get("title") == title
        ]
        token = resp.get("next_page_token")
        if not token:
            break
    here = [
        sid
        for sid in found
        if _norm_path(
            (client.api_client.do("GET", f"{SPACES_PATH}/{sid}") or {}).get(
                "parent_path"
            )
        )
        == _norm_path(parent_path)
    ]
    if len(here) > 1:
        raise RuntimeError(
            f"{len(here)} spaces are called {title!r} under {parent_path}: {here}; pass --space-id"
        )
    return here[0] if here else None


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
        "--parent-path",
        help="workspace folder of a new space (default: /Workspace/Users/<you>/geobrix/raster-genie)",
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="print the counts, change nothing"
    )
    spec.add_location_args(ap)
    args = ap.parse_args(argv)

    built = spec.spec(args.catalog, args.schema, args.uc_schema)
    tables, functions, text, examples = built.VARIANTS[args.variant]
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
    parent_path = args.parent_path or default_parent_path(client)
    space_id = args.space_id or find_space(client, title, parent_path)
    existed = bool(space_id)
    space_id = apply_space(
        title,
        args.warehouse_id,
        parent_path,
        serialized,
        space_id=space_id,
        client=client,
    )
    print(
        f"... {'updated' if existed else 'created'} space {title!r} in {parent_path}: {space_id}"
    )
    print(space_id)
    return space_id


if __name__ == "__main__":
    main()
