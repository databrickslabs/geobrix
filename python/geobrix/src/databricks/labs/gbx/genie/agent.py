"""Ask a Genie Agent a question in Agent mode (server-sent events)."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Iterable

_RESPONSES_PATH = "/api/2.0/genie/agents/{space_id}/responses"
_BEARER = re.compile(r"(?i)\bbearer\s+\S+")


def _redact(text: str, headers: dict | None) -> str:
    """Remove auth header values and any bearer token from text before it is surfaced."""
    for v in (headers or {}).values():
        if isinstance(v, str) and len(v) >= 6:
            text = text.replace(v, "***")
            if v.lower().startswith("bearer "):
                text = text.replace(v[7:], "***")
    return _BEARER.sub("Bearer ***", text)


def _requests_post(timeout_s: int) -> Callable:
    """Default transport: a streaming ``requests`` POST that yields decoded SSE lines."""

    def _post(url: str, headers: dict, json: dict) -> Iterable[str]:  # noqa: A002
        import requests

        resp = requests.post(
            url, json=json, headers=headers, stream=True, timeout=(30, timeout_s)
        )
        try:
            if resp.status_code != 200:
                raise RuntimeError(
                    f"HTTP {resp.status_code}: {(resp.text or '')[:500]}"
                )
            yield from resp.iter_lines(decode_unicode=True)
        finally:
            resp.close()

    return _post


def ask(
    space_id: str,
    question: str,
    *,
    conversation_id: str | None = None,
    client=None,
    timeout_s: int = 300,
    _post: Callable | None = None,
) -> dict:
    """Ask the Genie Agent ``space_id`` a question; return the parsed answer.

    Returns ``{status, seconds, conversation_id, sql, text, error}``. ``status`` is
    ``COMPLETED`` or ``FAILED`` (a failed event, a transport error, a timeout, or a stream
    that ended without a terminal event). ``sql`` lists the SQL the agent executed;
    ``text`` the message texts of the final answer. ``_post(url, headers, json)`` is the
    injectable HTTP layer and returns an iterable of SSE lines.
    """
    if not question or not question.strip():
        raise ValueError("question must be non-empty")
    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
    post = _post or _requests_post(timeout_s)

    body: dict = {
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": question}],
            }
        ]
    }
    if conversation_id:
        body["conversation_id"] = conversation_id
    url = client.config.host.rstrip("/") + _RESPONSES_PATH.format(space_id=space_id)
    headers = {
        **client.config.authenticate(),
        "Accept": "text/event-stream",
        "Content-Type": "application/json",
    }

    out: dict = {
        "status": None,
        "seconds": 0.0,
        "conversation_id": conversation_id,
        "sql": [],
        "text": [],
        "error": None,
    }
    t0 = time.monotonic()
    try:
        for line in post(url, headers, body):
            if time.monotonic() - t0 > timeout_s:
                raise TimeoutError(f"no terminal event within {timeout_s}s")
            if not line or not line.startswith("data:"):
                continue  # blank separators, "event:" names, comments
            try:
                ev = json.loads(line[5:].strip())
            except ValueError:
                continue  # keep-alives such as "data: [DONE]"
            if isinstance(ev, dict):
                _apply(ev, out)
            if out["status"]:
                break
        if not out["status"]:
            out["status"], out["error"] = (
                "FAILED",
                "stream ended without a terminal event",
            )
    except Exception as e:  # report, never raise: callers measure many questions
        out["status"], out["error"] = "FAILED", _redact(
            f"{type(e).__name__}: {e}", headers
        )
    out["seconds"] = round(time.monotonic() - t0, 2)
    return out


def _apply(ev: dict, out: dict) -> None:
    """Fold one SSE event into the result."""
    ty = ev.get("type")
    if ty == "response.created":
        resp = ev.get("response") or {}
        out["conversation_id"] = (
            resp.get("conversation_id")
            or ev.get("conversation_id")
            or out["conversation_id"]
        )
    elif ty == "response.output_item.done":
        item = ev.get("item") or {}
        if item.get("type") == "function_call":
            try:
                out["sql"].append(json.loads(item["arguments"])["sql"])
            except (KeyError, TypeError, ValueError):
                pass  # not an execute_sql call, or arguments not JSON
    elif ty == "response.completed":
        out["status"] = "COMPLETED"
        for item in (ev.get("response") or {}).get("output") or []:
            if item.get("type") == "message":
                out["text"] += [
                    c["text"] for c in item.get("content") or [] if c.get("text")
                ]
    elif ty == "response.failed":
        out["status"] = "FAILED"
        resp = ev.get("response") or {}
        err = resp.get("error") or ev.get("error") or ev
        out["error"] = (
            err.get("message")
            if isinstance(err, dict) and err.get("message")
            else json.dumps(err)[:1000]
        )
