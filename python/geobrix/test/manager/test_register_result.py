"""``register`` returns a dict that shows itself as a one-line summary: a notebook cell that ends in
``gbx.register(spark)`` displays the return value, and a line per function is noise."""

import copy
import pickle

from databricks.labs.gbx import manager
from databricks.labs.gbx.manager.levels import Choice

from .connect_stub import ConnectStub


def _result(n_l3=3, n_l1=2, notes=()):
    from databricks.labs.gbx.manager import RegisterResult

    out = RegisterResult(notes=list(notes))
    for i in range(n_l3):
        out[f"gbx_f{i}"] = Choice("L3", "c0", ())
    for i in range(n_l1):
        out[f"gbx_g{i}"] = Choice("L1", "-", ())
    return out


def test_repr_is_a_short_summary_not_a_line_per_function():
    out = _result(210, 48)
    text = repr(out)
    assert len(text.splitlines()) <= 3
    assert "258 functions registered" in text and "L3 210" in text and "L1 48" in text
    assert text.startswith("GeoBrix ")
    assert "Choice(" not in text


def test_the_result_is_still_a_dict_with_the_same_items():
    out = _result()
    assert isinstance(out, dict)
    assert out == {k: v for k, v in out.items()}
    assert list(out) == ["gbx_f0", "gbx_f1", "gbx_f2", "gbx_g0", "gbx_g1"]
    assert out["gbx_f1"].level == "L3" and len(out) == 5
    assert dict(out) == out
    # pickling and copying keep the items and the notes
    for clone in (copy.copy(out), pickle.loads(pickle.dumps(out))):
        assert clone == out and isinstance(clone, dict)


def test_notes_follow_the_summary_and_stay_within_three_lines():
    notes = [f"geobrix: reason {i}" for i in range(10)]
    text = repr(_result(notes=notes))
    lines = text.splitlines()
    assert len(lines) <= 3
    assert "reason 0" in text and "more" in lines[-1]


def test_html_and_pretty_displays_are_short_and_escaped():
    out = _result(notes=["geobrix: a <b> note"])
    html = out._repr_html_()
    assert "5 functions registered" in html and "&lt;b&gt;" in html
    assert html.count("<br>") <= 2 and len(html) < 600

    class P:
        text_out = ""

        def text(self, s):
            self.text_out += s

    p = P()
    out._repr_pretty_(p, False)
    assert p.text_out == repr(out)
    p = P()
    out._repr_pretty_(p, True)
    assert "..." in p.text_out


def test_an_empty_result_does_not_claim_functions():
    from databricks.labs.gbx.manager import RegisterResult

    assert "0 functions registered" in repr(RegisterResult())


def test_a_real_register_returns_the_summary_type_with_its_notes(capsys):
    stub = ConnectStub()
    out = manager.register(stub, only=["gbx_st_makevalid"])
    assert isinstance(out, dict) and "gbx_st_makevalid" in out
    assert "1 function registered" in repr(out)
    assert len(repr(out).splitlines()) <= 3


def test_say_grouped_hands_back_the_lines_it_prints(capsys):
    from databricks.labs.gbx.manager import messages

    spark = object.__new__(type("S9", (), {}))
    line = (
        "geobrix: JVM level unavailable: Spark Connect session; using the Python level"
    )
    got = messages.say_grouped(spark, [(f"gbx_rst_f{i}", line) for i in range(5)])
    assert got == capsys.readouterr().out.strip().splitlines()
    # a second call prints nothing (seen) but still hands the line back for the summary
    again = messages.say_grouped(spark, [(f"gbx_rst_f{i}", line) for i in range(5)])
    assert again == got and capsys.readouterr().out == ""
