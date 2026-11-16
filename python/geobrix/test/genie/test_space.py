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

    def do(self, method, path, body=None, **kw):
        self.calls.append((method, path, body))
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
