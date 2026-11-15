"""The fallback lines register prints on Serverless, composed by the real code path.

The shipped example notebooks carry these lines in their executed outputs. When the wording changes, the
outputs are hand-patched (no re-run); OLD_TO_NEW is the mapping they were patched with, and this test checks
that each NEW line is exactly what register prints today (the run's own function counts and names kept).
"""

import contextlib
import io
import re

import pytest

from .connect_stub import ConnectStub

# old notebook output line -> the line register prints now for the same reason, counts and names
OLD_TO_NEW = {
    "geobrix: heavy (Catalyst) unavailable: Spark Connect session (Serverless); using light (48 functions)": (
        "geobrix: JVM level unavailable: Spark Connect session (Serverless); using the Python level (48 functions)"
    ),
    "geobrix: SQL unavailable: 5 functions (gbx_bng_cellarea, gbx_bng_eastnorthasbng, gbx_quadbin_distance, ...) "
    "need a SQL function wrapper, and SQL functions are unavailable here; using light": (
        "geobrix: SQL expression level unavailable: 5 functions (gbx_bng_cellarea, gbx_bng_eastnorthasbng, "
        "gbx_quadbin_distance, ...) need a SQL function wrapper, and SQL functions are unavailable here; "
        "using the Python level"
    ),
}

# each printed line as a template: the count and the names shown are the parts a run fills in
_TEMPLATES = (
    (
        r"geobrix: JVM level unavailable: Spark Connect session \(Serverless\); using the Python level "
        r"\((?P<n>\d+) functions\)"
    ),
    (
        r"geobrix: SQL expression level unavailable: (?P<n>\d+) functions \((?P<names>[^)]*)\) need a SQL "
        r"function wrapper, and SQL functions are unavailable here; using the Python level"
    ),
)


@pytest.fixture(scope="module")
def serverless_lines():
    from databricks.labs.gbx import manager

    mp = pytest.MonkeyPatch()
    mp.setenv("DATABRICKS_RUNTIME_VERSION", "client.6.0")
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            manager.register(ConnectStub())
        return buf.getvalue().splitlines()
    finally:
        mp.undo()


@pytest.mark.parametrize("template", _TEMPLATES, ids=["jvm", "sql-expression"])
def test_register_on_serverless_prints_the_level_line(serverless_lines, template):
    assert any(
        re.fullmatch(template, line) for line in serverless_lines
    ), serverless_lines


@pytest.mark.parametrize("old", list(OLD_TO_NEW), ids=["jvm", "sql-expression"])
def test_each_patched_notebook_line_is_what_register_prints(serverless_lines, old):
    new = OLD_TO_NEW[old]
    matched = [re.fullmatch(t, new) for t in _TEMPLATES]
    assert any(matched), new
    # the run's count is kept: the new line names as many functions as the old one did
    m = next(m for m in matched if m)
    assert f"{m.group('n')} functions" in old
    if "names" in m.groupdict():
        assert f"({m.group('names')})" in old
    assert "heavy" not in new and "using light" not in new
