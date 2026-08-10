"""Test that every function in function-info.json has a 'bindings' list including 'sql'."""
import json
from pathlib import Path

FUNCTION_INFO = (
    Path(__file__).resolve().parents[2]
    / "src/main/resources/com/databricks/labs/gbx/function-info.json"
)


def _load():
    data = json.loads(FUNCTION_INFO.read_text())
    return data.get("functions", data)


def test_every_function_has_a_bindings_list_including_sql():
    fns = _load()
    checked = 0
    for name, entry in fns.items():
        if name.startswith("_"):
            continue
        assert "bindings" in entry, f"{name} missing bindings"
        assert isinstance(entry["bindings"], list)
        # Any function with a non-empty example must advertise the sql binding.
        if entry.get("examples"):
            assert "sql" in entry["bindings"], f"{name} has examples but no sql binding"
        checked += 1
    assert checked > 0


def test_bindings_values_are_from_the_known_set():
    known = {"sql", "python-light", "python-heavy", "scala"}
    for name, entry in _load().items():
        if name.startswith("_"):
            continue
        for b in entry.get("bindings", []):
            assert b in known, f"{name} has unknown binding {b!r}"
