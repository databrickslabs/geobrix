"""Build a Genie ``serialized_space`` and create or update the space through the SDK client."""

from __future__ import annotations

import hashlib
import json

_SPACES_PATH = "/api/2.0/genie/spaces"


def _hid(*parts: str) -> str:
    """Deterministic 32-char lowercase hex id from content (md5 hexdigest).

    Same derivation as the Genie Map seeder, so re-seeding a space is idempotent and the
    ids stay stable when that seeder moves onto this helper.
    """
    return hashlib.md5("\x1e".join(parts).encode("utf-8")).hexdigest()


def build_serialized_space(
    tables: list[str],
    functions: list[str],
    text: str,
    examples: list[tuple[str, str]],
) -> dict:
    """Return a version-2 ``serialized_space`` dict.

    ``tables`` and ``functions`` are fully qualified names. ``examples`` are
    ``(question, sql)`` pairs. Every list is sorted the way the API requires (tables by
    identifier, the rest by id), so the output does not depend on input order.
    """
    instructions: dict = {}
    if text:
        instructions["text_instructions"] = [
            {"id": _hid("text", text), "content": [text]}
        ]
    if examples:
        instructions["example_question_sqls"] = sorted(
            (
                {"id": _hid("ex", q, sql), "question": [q], "sql": [sql]}
                for q, sql in dict.fromkeys(examples)
            ),
            key=lambda e: e["id"],
        )
    if functions:
        instructions["sql_functions"] = sorted(
            ({"id": _hid("fn", f), "identifier": f} for f in set(functions)),
            key=lambda f: (f["id"], f["identifier"]),
        )
    return {
        "version": 2,
        "data_sources": {"tables": [{"identifier": t} for t in sorted(set(tables))]},
        "instructions": instructions,
    }


def _merge_serialized(existing: dict, new: dict) -> dict:
    """Merge ``new`` into ``existing``: union the tables, replace only the instruction keys given."""
    merged = dict(existing)
    merged.setdefault("version", new.get("version", 2))
    ds = dict(existing.get("data_sources") or {})
    # Existing table dicts stay as they are (description, column_configs, ...), including any
    # without an identifier; only identifiers not already present are added from ``new``.
    tables = list(ds.get("tables") or [])
    seen = {t.get("identifier") for t in tables if isinstance(t, dict)}
    for t in (new.get("data_sources") or {}).get("tables") or []:
        if t.get("identifier") not in seen:
            seen.add(t.get("identifier"))
            tables.append(t)
    ds["tables"] = sorted(tables, key=lambda t: t.get("identifier") or "")
    merged["data_sources"] = ds
    instructions = dict(existing.get("instructions") or {})
    instructions.update(new.get("instructions") or {})
    merged["instructions"] = instructions
    return merged


def apply_space(
    title: str,
    warehouse_id: str,
    parent_path: str,
    serialized: dict,
    space_id: str | None = None,
    client=None,
    merge: bool = False,
) -> str:
    """Create the space, or update it when ``space_id`` is given; return the space id.

    Create is ``POST /api/2.0/genie/spaces`` with ``{warehouse_id, title, parent_path,
    serialized_space}``. Update is ``PATCH /api/2.0/genie/spaces/{id}`` with
    ``{serialized_space, warehouse_id, title}``; ``parent_path`` applies only at create.

    The update REPLACES the whole ``serialized_space``: with the default ``merge=False``
    anything curated in the space that is not in ``serialized`` (tables, instructions,
    examples, attached functions) is dropped. Pass ``merge=True`` (with ``space_id``) to
    first read the current space (``GET .../{id}?include_serialized_space=true``) and
    merge: its table entries are kept unchanged (with their metadata) and ``serialized``'s new
    identifiers are added (sorted by identifier), instruction
    keys present in ``serialized`` replace the existing ones while the other instruction
    keys are kept, and the existing ``version`` is kept.
    """
    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
    if space_id and merge:
        current = client.api_client.do(
            "GET",
            f"{_SPACES_PATH}/{space_id}",
            query={"include_serialized_space": "true"},
        )
        existing = json.loads((current or {}).get("serialized_space") or "{}")
        serialized = _merge_serialized(existing, serialized)
    body = {
        "serialized_space": json.dumps(serialized),
        "warehouse_id": warehouse_id,
        "title": title,
    }
    if space_id:
        client.api_client.do("PATCH", f"{_SPACES_PATH}/{space_id}", body=body)
        return space_id
    body["parent_path"] = parent_path
    resp = client.api_client.do("POST", _SPACES_PATH, body=body)
    new_id = (resp or {}).get("space_id") or (resp or {}).get("id")
    if not new_id:
        raise RuntimeError("Genie create-space returned no space_id")
    return new_id
