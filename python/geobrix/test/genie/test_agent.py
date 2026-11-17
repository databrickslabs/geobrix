"""Offline tests for databricks.labs.gbx.genie.agent (Agent-mode SSE client)."""

import json

import pytest

from databricks.labs.gbx.genie.agent import ask

_SQL = (
    "SELECT c.tile_name, mx.value AS max_slope_deg\n"
    "FROM cat.sch.gbx_genie_raster_stats('v_tiles','all','slope','max') mx\n"
    "JOIN cat.sch.tile_catalog c ON c.tile_id = mx.id ORDER BY mx.value DESC"
)
_REPORT = "# Steepest terrain: tile_a\n\nThe tile_a tile has the steepest terrain, about 66 degrees."
_TABLE = "**Slope by tile**\n\n| tile_name | max_slope_deg |\n| --- | --- |\n| tile_a | 66.7 |"
_CALL = {
    "type": "function_call",
    "id": "call_1",
    "call_id": "call_1",
    "status": "completed",
    "name": "execute_sql",
    "arguments": json.dumps({"title": "Slope by tile", "sql": _SQL}),
}
_MESSAGE = {
    "type": "message",
    "role": "assistant",
    "content": [
        {"type": "output_text", "text": _REPORT, "annotations": []},
        {"type": "output_text", "text": _TABLE, "annotations": []},
    ],
}
_EVENTS = [
    {
        "type": "response.created",
        "sequence_number": 0,
        "response": {
            "id": "resp_1",
            "status": "in_progress",
            "output": [],
            "conversation_id": "conv_1",
        },
    },
    {
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "type": "reasoning",
            "id": "r_1",
            "content": [{"type": "reasoning_text", "text": "Rank by slope."}],
        },
    },
    {"type": "response.output_item.added", "output_index": 1, "item": _CALL},
    {"type": "response.output_item.done", "output_index": 1, "item": _CALL},
    {
        "type": "response.output_item.done",
        "output_index": 2,
        "item": {
            "type": "function_call_output",
            "id": "call_1_output",
            "call_id": "call_1",
            "output": _TABLE,
        },
    },
    {"type": "response.output_item.done", "output_index": 3, "item": _MESSAGE},
    {
        "type": "response.completed",
        "response": {
            "id": "resp_1",
            "status": "completed",
            "conversation_id": "conv_1",
            "output": [_CALL, _MESSAGE],
        },
    },
]


def _sse(events):
    lines = []
    for e in events:
        lines += ["event: " + e["type"], "data: " + json.dumps(e), ""]
    return lines


class _FakeClient:
    class config:  # noqa: N801 - mimics databricks.sdk Config
        host = "https://dummy-workspace.example.com/"

        @staticmethod
        def authenticate():
            return {"Authorization": "Bearer SECRET-TOKEN-123"}


def _post_of(lines, seen=None):
    def _post(url, headers, json):  # noqa: A002 - matches the documented seam
        if seen is not None:
            seen.update(url=url, headers=dict(headers), json=json)
        return iter(lines)

    return _post


def test_parses_completed_stream():
    out = ask(
        "space1",
        "Which tile is steepest?",
        client=_FakeClient(),
        _post=_post_of(_sse(_EVENTS)),
    )
    assert out["status"] == "COMPLETED"
    assert out["conversation_id"] == "conv_1"
    assert out["sql"] == [_SQL]
    assert out["text"] == [_REPORT, _TABLE]
    assert out["error"] is None
    assert out["seconds"] >= 0


def test_request_shape_and_auth():
    seen = {}
    ask("space1", "Q?", client=_FakeClient(), _post=_post_of(_sse(_EVENTS), seen))
    assert (
        seen["url"]
        == "https://dummy-workspace.example.com/api/2.0/genie/agents/space1/responses"
    )
    assert seen["headers"]["Accept"] == "text/event-stream"
    assert seen["headers"]["Authorization"] == "Bearer SECRET-TOKEN-123"
    assert seen["json"] == {
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "Q?"}],
            }
        ]
    }


def test_conversation_id_is_forwarded():
    seen = {}
    ask(
        "space1",
        "Q?",
        conversation_id="conv_9",
        client=_FakeClient(),
        _post=_post_of(_sse(_EVENTS), seen),
    )
    assert seen["json"]["conversation_id"] == "conv_9"


def test_failed_stream():
    failed = {
        "type": "response.failed",
        "response": {"status": "failed", "error": {"message": "warehouse is stopped"}},
    }
    out = ask(
        "s", "Q?", client=_FakeClient(), _post=_post_of(_sse(_EVENTS[:2] + [failed]))
    )
    assert out["status"] == "FAILED"
    assert "warehouse is stopped" in out["error"]
    assert out["conversation_id"] == "conv_1"
    assert out["sql"] == []


def test_stream_without_terminal_event_is_failed():
    out = ask("s", "Q?", client=_FakeClient(), _post=_post_of(_sse(_EVENTS[:4])))
    assert out["status"] == "FAILED"
    assert "without" in out["error"]
    assert out["sql"] == [_SQL]


def test_ignores_noise_lines_and_bad_json():
    lines = [": keep-alive", "data: not json", "data: [DONE]", ""] + _sse(_EVENTS)
    out = ask("s", "Q?", client=_FakeClient(), _post=_post_of(lines))
    assert out["status"] == "COMPLETED"
    assert out["sql"] == [_SQL]


def test_unparseable_function_arguments_do_not_crash():
    call = dict(_CALL, arguments="{not json")
    ev = [_EVENTS[0], {"type": "response.output_item.done", "item": call}, _EVENTS[-1]]
    out = ask("s", "Q?", client=_FakeClient(), _post=_post_of(_sse(ev)))
    assert out["status"] == "COMPLETED"
    assert out["sql"] == []


def test_transport_error_is_reported_and_token_redacted():
    def _boom(url, headers, json):  # noqa: A002
        raise RuntimeError("401 for " + headers["Authorization"])

    out = ask("s", "Q?", client=_FakeClient(), _post=_boom)
    assert out["status"] == "FAILED"
    assert "SECRET-TOKEN-123" not in out["error"]
    assert "401" in out["error"]


class _Resp:
    def __init__(self, status_code=200, text="", lines=()):
        self.status_code = status_code
        self.text = text
        self._lines = lines
        self.encoding = None
        self.encoding_at_read = "unset"

    def iter_lines(self, decode_unicode=False):
        self.encoding_at_read = self.encoding
        return iter(self._lines)

    def close(self):
        pass


def test_default_transport_error_status_is_redacted(monkeypatch):
    # requests is a runtime/[stac] dependency, not installed in every CI environment.
    requests = pytest.importorskip("requests")

    calls = {}

    def _fake_post(
        url, json=None, headers=None, stream=None, timeout=None
    ):  # noqa: A002
        calls.update(stream=stream, timeout=timeout)
        return _Resp(500, "boom Bearer SECRET-TOKEN-123")

    monkeypatch.setattr(requests, "post", _fake_post)
    out = ask("s", "Q?", client=_FakeClient(), timeout_s=7)
    assert calls["stream"] is True and calls["timeout"] is not None
    assert out["status"] == "FAILED"
    assert "500" in out["error"]
    assert "SECRET-TOKEN-123" not in out["error"]


def test_default_transport_200_decodes_utf8(monkeypatch):
    # requests is a runtime/[stac] dependency, not installed in every CI environment.
    requests = pytest.importorskip("requests")

    report = "Médian slope 12.5°"
    msg = {"type": "message", "content": [{"type": "output_text", "text": report}]}
    events = [
        _EVENTS[0],
        {"type": "response.completed", "response": {"output": [msg]}},
    ]
    resp = _Resp(200, lines=_sse(events))
    monkeypatch.setattr(requests, "post", lambda *a, **k: resp)
    out = ask("s", "Q?", client=_FakeClient())
    assert resp.encoding_at_read == "utf-8"
    assert out["status"] == "COMPLETED"
    assert out["text"] == [report]


def test_multiline_data_payload_is_joined():
    full = json.dumps(
        _EVENTS[0], indent=1
    ).splitlines()  # a payload split over several lines
    lines = ["event: response.created"] + ["data: " + p for p in full] + [""]
    lines += _sse(_EVENTS[1:])
    out = ask("s", "Q?", client=_FakeClient(), _post=_post_of(lines))
    assert out["conversation_id"] == "conv_1"
    assert out["status"] == "COMPLETED"


def test_last_event_without_trailing_blank_line_is_used():
    lines = _sse(_EVENTS)[:-1]  # drop the final blank separator
    out = ask("s", "Q?", client=_FakeClient(), _post=_post_of(lines))
    assert out["status"] == "COMPLETED"


def test_completed_tolerates_malformed_items():
    done = {
        "type": "response.completed",
        "response": {
            "conversation_id": "conv_late",
            "output": [
                "junk",
                None,
                {"type": "message", "content": "nope"},
                {"type": "message", "content": [None, 3, {"text": "ok"}]},
            ],
        },
    }
    created_no_conv = {"type": "response.created", "response": {"id": "r"}}
    out = ask(
        "s", "Q?", client=_FakeClient(), _post=_post_of(_sse([created_no_conv, done]))
    )
    assert out["status"] == "COMPLETED"
    assert out["text"] == ["ok"]
    assert out["conversation_id"] == "conv_late"


def test_failed_message_is_redacted():
    failed = {
        "type": "response.failed",
        "response": {"error": {"message": "denied for Bearer SECRET-TOKEN-123"}},
    }
    out = ask("s", "Q?", client=_FakeClient(), _post=_post_of(_sse([failed])))
    assert out["status"] == "FAILED"
    assert "SECRET-TOKEN-123" not in out["error"]
    assert "denied" in out["error"]


def test_ask_requires_nonempty_question():
    with pytest.raises(ValueError):
        ask("s", "  ", client=_FakeClient(), _post=_post_of([]))
