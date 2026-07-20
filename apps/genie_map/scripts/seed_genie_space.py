#!/usr/bin/env python3
"""Seed a Genie Space's instructions + example SQLs from docs/GENIE-SPACE.md.

The DAB `genie_space` resource only re-applies the table set
(genie_space.geniespace.json) on every deploy, which WIPES any instructions /
example SQL curated in the UI. This script re-seeds that curation from the
tracked single source of truth (docs/GENIE-SPACE.md, blocks A and B) via the
Genie spaces update API, so a redeploy no longer means manual re-pasting.

Schema notes (reverse-engineered — the update API validates serialized_space
loosely and only surfaces one error at a time):
  - instructions.text_instructions[]:      {id, content: [str, ...]}
  - instructions.example_question_sqls[]:  {id, question: [str], sql: [str]}
  - `id` MUST be a 32-char lowercase hex string (no hyphens).
  - example_question_sqls MUST be sorted by id.
We derive each id deterministically from an md5 of its content, so re-runs are
idempotent (same content -> same id) and the sort is stable.

Auth: shells out to the `databricks` CLI (`databricks api get/patch`) so it uses
the same OAuth profile as the rest of the gbx palette. Requires the profile to
be valid (`databricks auth profiles`).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


def _hid(*parts: str) -> str:
    """Deterministic 32-char lowercase hex id from content (md5 hexdigest)."""
    h = hashlib.md5("\x1e".join(parts).encode("utf-8")).hexdigest()
    return h  # md5 hexdigest is exactly 32 lowercase hex chars


def parse_block_a(md: str) -> str:
    """Extract the block-A instruction text (the fenced body under 'Paste block A')."""
    m = re.search(
        r"### Paste block A.*?\n```\n(.*?)\n```",
        md,
        re.DOTALL,
    )
    if not m:
        raise SystemExit("Could not find 'Paste block A' fenced block in GENIE-SPACE.md")
    return m.group(1).strip()


def parse_block_b(md: str) -> list[tuple[str, str]]:
    """Extract (question, sql) pairs from block B.

    Each example is a `**Title** — *"QUESTION"*` line followed by a ```sql fence.
    The italicised quoted phrase is the natural-language question Genie matches on.
    """
    b = md.split("### Paste block B", 1)
    if len(b) != 2:
        raise SystemExit("Could not find 'Paste block B' section in GENIE-SPACE.md")
    body = b[1]
    # Pair each example header (with a "..." question) to the next sql fence.
    pairs: list[tuple[str, str]] = []
    # Split on example headers; capture the quoted question and the following sql.
    example_re = re.compile(
        r'\*\*.*?\*\*.*?[""\"](?P<q>[^""\"]+)[""\"].*?\n```sql\n(?P<sql>.*?)\n```',
        re.DOTALL,
    )
    for m in example_re.finditer(body):
        q = m.group("q").strip()
        sql = m.group("sql").strip()
        if q and sql:
            pairs.append((q, sql))
    if not pairs:
        raise SystemExit("Parsed zero example SQLs from block B — check GENIE-SPACE.md formatting")
    return pairs


def _cli(args: list[str], profile: str) -> str:
    out = subprocess.run(
        ["databricks", *args, "--profile", profile],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        raise SystemExit(f"databricks {' '.join(args)} failed:\n{out.stderr.strip()}")
    return out.stdout


def get_space(space_id: str, profile: str) -> dict:
    raw = _cli(["api", "get", f"/api/2.0/genie/spaces/{space_id}?include_serialized_space=true"], profile)
    return json.loads(raw)


def build_serialized_space(current_ss: dict, instr_text: str, examples: list[tuple[str, str]]) -> dict:
    """Return an updated serialized_space dict: keep data_sources, replace instructions."""
    ss = dict(current_ss)  # keep version + data_sources (the table set)
    text_instructions = [{"id": _hid("text", instr_text), "content": [instr_text]}]
    example_objs = [
        {"id": _hid("ex", q, sql), "question": [q], "sql": [sql]}
        for (q, sql) in examples
    ]
    example_objs.sort(key=lambda e: e["id"])  # API requires sorted-by-id
    ss["instructions"] = {
        "text_instructions": text_instructions,
        "example_question_sqls": example_objs,
    }
    return ss


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed Genie Space instructions + examples from GENIE-SPACE.md")
    ap.add_argument("--space-id", required=True)
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--docs", default=str(Path(__file__).resolve().parents[1] / "docs" / "GENIE-SPACE.md"))
    ap.add_argument("--dry-run", action="store_true", help="Print what would be written; do not PATCH")
    args = ap.parse_args()

    md = Path(args.docs).read_text(encoding="utf-8")
    instr_text = parse_block_a(md)
    examples = parse_block_b(md)

    space = get_space(args.space_id, args.profile)
    current_ss = json.loads(space["serialized_space"])
    title = space.get("title", "")

    new_ss = build_serialized_space(current_ss, instr_text, examples)

    print(f"Parsed: 1 text-instruction block ({len(instr_text)} chars), {len(examples)} example SQLs:")
    for e in new_ss["instructions"]["example_question_sqls"]:
        print(f"  - {e['question'][0]}")

    if args.dry_run:
        print("\n[dry-run] not writing.")
        return 0

    body = {
        "serialized_space": json.dumps(new_ss),
        "warehouse_id": args.warehouse_id,
        "title": title,
    }
    tmp = Path("/tmp/gbx_seed_genie_req.json")
    tmp.write_text(json.dumps(body), encoding="utf-8")
    _cli(["api", "patch", f"/api/2.0/genie/spaces/{args.space_id}", "--json", f"@{tmp}"], args.profile)
    # Verify round-trip.
    verify = json.loads(get_space(args.space_id, args.profile)["serialized_space"]).get("instructions", {})
    n_text = len(verify.get("text_instructions", []))
    n_ex = len(verify.get("example_question_sqls", []))
    if n_text < 1 or n_ex != len(examples):
        raise SystemExit(f"Verification failed: space now has {n_text} text blocks, {n_ex} examples (expected 1, {len(examples)})")
    print(f"\nSeeded OK: {n_text} instruction block, {n_ex} example SQLs written to space {args.space_id}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
