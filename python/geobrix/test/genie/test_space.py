"""Offline tests for databricks.labs.gbx.genie.space."""

import json
import re

from databricks.labs.gbx.genie.space import apply_space, build_serialized_space

_HEX32 = re.compile(r"^[0-9a-f]{32}$")
_TABLES = ["c.s.zeta", "c.s.alpha"]
_FNS = ["c.s.gbx_genie_raster_stats", "c.s.gbx_genie_other"]
_EXAMPLES = [
    ("Which tile is steepest?", "SELECT 1"),
    ("How many tiles?", "SELECT count(*) FROM t"),
]


def _build(**kw):
    args = dict(
        tables=_TABLES,
        functions=_FNS,
        text="Use the raster function.",
        examples=_EXAMPLES,
    )
    args.update(kw)
    return build_serialized_space(**args)


def test_shape_and_sorting():
    ss = _build()
    assert ss["version"] == 2
    assert ss["data_sources"]["tables"] == [
        {"identifier": "c.s.alpha"},
        {"identifier": "c.s.zeta"},
    ]
    ins = ss["instructions"]
    assert len(ins["text_instructions"]) == 1
    assert ins["text_instructions"][0]["content"] == ["Use the raster function."]
    for key in ("example_question_sqls", "sql_functions"):
        ids = [e["id"] for e in ins[key]]
        assert ids == sorted(ids)
    ex = ins["example_question_sqls"][0]
    assert set(ex) == {"id", "question", "sql"}
    assert isinstance(ex["question"], list) and isinstance(ex["sql"], list)
    assert {f["identifier"] for f in ins["sql_functions"]} == set(_FNS)


def test_ids_are_32_hex_and_deterministic():
    a, b = _build(), _build()
    assert a == b
    ins = a["instructions"]
    ids = [ins["text_instructions"][0]["id"]]
    ids += [e["id"] for e in ins["example_question_sqls"]]
    ids += [f["id"] for f in ins["sql_functions"]]
    assert len(set(ids)) == len(ids)
    assert all(_HEX32.match(i) for i in ids)


def test_ids_follow_the_genie_map_seeder_hid():
    import hashlib

    from databricks.labs.gbx.genie.space import _hid

    assert _hid("a", "b") == hashlib.md5("a\x1eb".encode("utf-8")).hexdigest()


def test_input_order_does_not_change_output():
    fwd = _build()
    rev = _build(tables=_TABLES[::-1], functions=_FNS[::-1], examples=_EXAMPLES[::-1])
    assert fwd == rev


def test_sql_functions_only_when_given():
    assert "sql_functions" not in _build(functions=[])["instructions"]
    assert _build()["instructions"]["sql_functions"]


def test_serializable():
    json.dumps(_build())


class _FakeApi:
    def __init__(self, response):
        self.calls = []
        self._response = response
        self.get_response = {}

    def do(self, method, path, body=None, query=None, **kw):
        self.calls.append((method, path, body))
        if method == "GET":
            return self.get_response
        return self._response


class _FakeClient:
    def __init__(self, response=None):
        self.api_client = _FakeApi(
            response if response is not None else {"space_id": "new123"}
        )


def test_apply_space_creates_when_no_id():
    client = _FakeClient()
    ss = _build()
    sid = apply_space("T", "wh1", "/Workspace/Users/x/g", ss, client=client)
    assert sid == "new123"
    ((method, path, body),) = client.api_client.calls
    assert (method, path) == ("POST", "/api/2.0/genie/spaces")
    assert body["title"] == "T" and body["warehouse_id"] == "wh1"
    assert body["parent_path"] == "/Workspace/Users/x/g"
    assert json.loads(body["serialized_space"]) == ss


def test_apply_space_patches_when_id_given():
    client = _FakeClient(response={})
    ss = _build()
    sid = apply_space("T", "wh1", "/p", ss, space_id="abc", client=client)
    assert sid == "abc"
    ((method, path, body),) = client.api_client.calls
    assert (method, path) == ("PATCH", "/api/2.0/genie/spaces/abc")
    assert json.loads(body["serialized_space"]) == ss
    assert body["warehouse_id"] == "wh1" and body["title"] == "T"


def test_default_update_does_not_read_the_space():
    client = _FakeClient(response={})
    apply_space("T", "wh1", "/p", _build(), space_id="abc", client=client)
    assert [c[0] for c in client.api_client.calls] == ["PATCH"]


def test_merge_keeps_existing_tables_and_other_instruction_keys():
    existing = {
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": "c.s.mid"}, {"identifier": "c.s.zeta"}]
        },
        "instructions": {
            "text_instructions": [{"id": "0" * 32, "content": ["old text"]}],
            "example_question_sqls": [
                {"id": "1" * 32, "question": ["old q"], "sql": ["SELECT 0"]}
            ],
            "sql_functions": [{"id": "2" * 32, "identifier": "c.s.old_fn"}],
        },
        "config": {"sample_questions": [{"id": "3" * 32, "question": ["hi"]}]},
    }
    client = _FakeClient(response={})
    client.api_client.get_response = {"serialized_space": json.dumps(existing)}
    new = _build(functions=[], examples=[])  # text + tables only
    apply_space("T", "wh1", "/p", new, space_id="abc", client=client, merge=True)
    (g_method, g_path, _), (p_method, p_path, body) = client.api_client.calls
    assert (g_method, g_path) == ("GET", "/api/2.0/genie/spaces/abc")
    assert (p_method, p_path) == ("PATCH", "/api/2.0/genie/spaces/abc")
    sent = json.loads(body["serialized_space"])
    assert [t["identifier"] for t in sent["data_sources"]["tables"]] == [
        "c.s.alpha",
        "c.s.mid",
        "c.s.zeta",
    ]
    ins = sent["instructions"]
    assert (
        ins["text_instructions"] == new["instructions"]["text_instructions"]
    )  # replaced
    assert (
        ins["example_question_sqls"]
        == existing["instructions"]["example_question_sqls"]
    )  # kept
    assert ins["sql_functions"] == existing["instructions"]["sql_functions"]  # kept
    assert sent["version"] == 2 and sent["config"] == existing["config"]


def test_merge_replaces_keys_the_caller_provides():
    existing = {
        "version": 2,
        "data_sources": {"tables": []},
        "instructions": {
            "sql_functions": [{"id": "2" * 32, "identifier": "c.s.old_fn"}]
        },
    }
    client = _FakeClient(response={})
    client.api_client.get_response = {"serialized_space": json.dumps(existing)}
    new = _build()
    apply_space("T", "wh1", "/p", new, space_id="abc", client=client, merge=True)
    sent = json.loads(client.api_client.calls[-1][2]["serialized_space"])
    assert sent["instructions"]["sql_functions"] == new["instructions"]["sql_functions"]


def test_merge_without_space_id_just_creates():
    client = _FakeClient()
    apply_space("T", "wh1", "/p", _build(), client=client, merge=True)
    assert [c[0] for c in client.api_client.calls] == ["POST"]
