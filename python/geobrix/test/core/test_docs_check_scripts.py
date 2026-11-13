"""The docs/scripts checks that read the function manifest: the release-notes name diff, the doc and diagram
coverage, and the docs example checks."""

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
SCRIPTS = REPO / "docs/scripts"


def _load(name):
    path = SCRIPTS / f"{name}.py"
    sys.path.insert(0, str(SCRIPTS))
    try:
        spec = importlib.util.spec_from_file_location(name.replace("-", "_"), path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(SCRIPTS))
    return mod


def _run(name):
    return subprocess.run(
        [sys.executable, str(SCRIPTS / f"{name}.py")],
        capture_output=True,
        text=True,
        cwd=REPO,
    )


# --- check-release-notes-functions.py -------------------------------------------------------------------


def _notes(monkeypatch, files):
    """The script with `_show` returning canned text per (rev, file name)."""
    mod = _load("check-release-notes-functions")
    monkeypatch.setattr(mod, "_show", lambda rev, path: files.get((rev, path.name)))
    return mod


MANIFEST = '{"functions": [{"sql_name": "gbx_new_b"}, {"sql_name": "gbx_old_a"}]}'


def test_added_names_are_the_set_difference_between_the_ranges_ends(monkeypatch):
    mod = _notes(
        monkeypatch,
        {
            ("base", "functions.json"): '{"functions": [{"sql_name": "gbx_old_a"}]}',
            ("end", "functions.json"): MANIFEST,
        },
    )
    # only the new name: not the one that was already there
    assert mod.added_functions("base..end") == ["gbx_new_b"]


def test_a_manifest_absent_at_the_base_counts_all_its_rows_as_added(monkeypatch):
    mod = _notes(
        monkeypatch,
        {
            ("base", "functions.json"): "",  # the revision exists; the file did not
            ("base", "function-info.json"): "",  # nor the old registry
            ("end", "functions.json"): MANIFEST,
        },
    )
    assert mod.added_functions("base..end") == ["gbx_new_b", "gbx_old_a"]


def test_a_base_before_the_manifest_reads_its_function_info(monkeypatch):
    """A revision before the 0.6 manifest lists its functions in function-info.json (main before 0.6)."""
    mod = _notes(
        monkeypatch,
        {
            ("base", "functions.json"): "",
            ("base", "function-info.json"): json.dumps(
                {"functions": {"_package_x": "---", "gbx_old_a": {}}}
            ),
            ("end", "functions.json"): MANIFEST,
        },
    )
    assert mod.added_functions("base..end") == ["gbx_new_b"]


def test_an_unreadable_revision_or_manifest_is_an_advisory_skip(monkeypatch):
    mod = _notes(monkeypatch, {})  # _show gives None for every file
    assert mod.added_functions("base..end") is None
    mod = _notes(
        monkeypatch,
        {
            ("base", "functions.json"): "{not json",
            ("end", "functions.json"): MANIFEST,
        },
    )
    assert mod.added_functions("base..end") is None
    mod = _notes(
        monkeypatch,
        {
            ("base", "functions.json"): '{"functions": [{"no_name": 1}]}',
            ("end", "functions.json"): MANIFEST,
        },
    )
    assert mod.added_functions("base..end") is None


def test_the_range_ends_are_parsed_and_a_three_dot_range_starts_at_the_merge_base():
    mod = _load("check-release-notes-functions")
    assert mod.range_ends("a..b") == ("a", "b")
    assert mod.range_ends("a..") == ("a", "HEAD")
    assert mod.range_ends("a") == ("a", "HEAD")
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        cwd=REPO,
        check=True,
    ).stdout.strip()
    assert mod.range_ends("HEAD...HEAD") == (sha, "HEAD")


# --- check-doc-coverage.py / check-diagram-coverage.py --------------------------------------------------


def test_check_doc_coverage_passes_and_counts_every_manifest_function():
    r = _run("check-doc-coverage")
    assert r.returncode == 0, r.stdout[-800:] + r.stderr[-800:]
    mod = _load("check-doc-coverage")
    n = len(mod.canonical_sql())
    assert n == len(mod._manifest_rows()) > 200
    assert f"Canonical registered functions: {n}" in r.stdout


def test_check_doc_coverage_uses_the_row_page_and_fails_an_unmapped_name(tmp_path):
    mod = _load("check-doc-coverage")
    page = tmp_path / "p.mdx"
    page.write_text("### zz_documented\n")
    mod._manifest_rows = lambda: {
        "gbx_zz_documented": {"doc": {"page": "x"}},
        "gbx_zz_missing": {"doc": {"page": "x"}},
        "gbx_zz_nopage": {},
    }
    mod.REPO_ROOT = tmp_path
    (tmp_path / "docs/docs").mkdir(parents=True)
    (tmp_path / "docs/docs/x.mdx").write_text("### zz_documented\n")
    ok, report = mod.check_d2(["gbx_zz_documented", "gbx_zz_missing"])
    assert not ok and "gbx_zz_missing" in report and "gbx_zz_documented" not in report
    ok, report = mod.check_d2(["gbx_zz_nopage"])
    assert not ok and "gbx_zz_nopage" in report


def test_check_doc_coverage_fails_on_an_empty_scan(monkeypatch):
    mod = _load("check-doc-coverage")
    ok, report = mod.check_d2([])
    assert not ok and "0 registered functions" in report
    monkeypatch.setattr(mod, "SQL_FILE_MAP", {})
    ok, report = mod.check_d3(["gbx_rst_slope"])
    assert not ok and "0 example output constants" in report
    monkeypatch.setattr(mod, "canonical_sql", lambda: [])
    assert mod.main() == 1
    # Names present, but no TILE-returning function found (the Scala sources moved): still a failure.
    monkeypatch.undo()
    monkeypatch.setattr(mod, "_classify_tile_returning_functions", lambda: set())
    assert mod.main() == 1


def test_the_diagram_check_counts_the_manifests_rst_names():
    mod = _load("check-diagram-coverage")
    names = mod.canonical_rst()
    assert {"rst_transformcrs", "rst_merge_agg"} <= names
    assert not any(n.startswith("gbx_") for n in names)


def test_the_diagram_check_passes_on_the_committed_diagram():
    r = _run("check-diagram-coverage")
    assert r.returncode == 0, r.stdout[-1200:] + r.stderr[-800:]
    n = len(_load("check-diagram-coverage").canonical_rst())
    assert (
        f"PASS D4a: diagram pills match the registered rst_ set exactly ({n} functions)"
        in r.stdout
    )
    assert "PASS D4b" in r.stdout and "PASS D4c" in r.stdout


def test_the_diagram_check_fails_a_missing_stale_or_doubled_pill():
    mod = _load("check-diagram-coverage")
    pills = mod.diagram_pills(mod.DIAGRAM_HTML.read_text())
    reg = mod.canonical_rst()
    assert set(pills) == reg and len(pills) == len(reg)
    ok, report = mod.check_d4a(reg, pills[1:])
    assert not ok and pills[0] in report and "MISSING" in report
    ok, report = mod.check_d4a(reg, pills + ["rst_gone"])
    assert not ok and "rst_gone" in report
    ok, report = mod.check_d4a(reg, pills + pills[:1])
    assert not ok and "more than once" in report
    ok, _ = mod.check_d4a(set(), [])
    assert not ok


def test_the_diagram_check_fails_a_wrong_count_or_badge():
    mod = _load("check-diagram-coverage")
    html = mod.DIAGRAM_HTML.read_text()
    n = len(mod.canonical_rst())
    assert mod.check_d4b(n, html)[0]
    ok, report = mod.check_d4b(n + 1, html)
    assert not ok and f"'{n} RasterX functions' (has {n}, expected {n + 1})" in report
    # Drop one pill from the first card: its badge no longer matches.
    title, body = mod.CARD_RE.findall(html)[0]
    first_pill = re.search(r'<g class="pill".*?</text></g>', body).group(0)
    ok, report = mod.check_d4b(n, html.replace(first_pill, "", 1))
    assert not ok and f"card '{title}'" in report
    ok, report = mod.check_d4b(n, "<html></html>")
    assert not ok and "no count mention" in report and "no category cards" in report


def test_the_diagram_check_fails_a_stale_diagram():
    mod = _load("check-diagram-coverage")
    html = mod.DIAGRAM_HTML.read_text()
    assert mod.check_d4c(html)[0]
    ok, report = mod.check_d4c(html.replace("Accessors", "Readers", 1))
    assert not ok and "stale" in report


# --- check-docs-examples.py -----------------------------------------------------------------------------


def test_docs_examples_passes_and_reports_the_delegated_blocks():
    r = _run("check-docs-examples")
    assert r.returncode == 0, r.stdout[-800:] + r.stderr[-800:]
    assert "blocks belong to manifest rows" in r.stdout


def test_docs_examples_fails_when_a_scan_is_empty(monkeypatch, tmp_path, capsys):
    mod = _load("check-docs-examples")
    monkeypatch.setattr(mod, "_example_modules", lambda: [])
    assert any("scanned 0" in f for f in mod.check_output_tables())
    mdx = tmp_path / "page.mdx"
    mdx.write_text("no examples here")
    monkeypatch.setattr(mod, "MDX", mdx)
    monkeypatch.setattr(mod, "REPO_ROOT", tmp_path)
    assert any("0 <FunctionExamples> blocks" in f for f in mod.check_tab_completeness())
    monkeypatch.setattr(mod, "_output_constants", lambda path: iter(()))
    assert any("compared 0" in f for f in mod.check_annotation_consistency())
    empty = tmp_path / "functions.json"
    empty.write_text('{"functions": []}')
    monkeypatch.setattr(mod, "MANIFEST_JSON", empty)
    assert any("no stable rows" in f for f in mod.check_tab_completeness())


# --- scripts/manifest/check_surfaces.py -----------------------------------------------------------------


def test_the_replacement_gates_fail_on_a_manifest_with_no_stable_row(monkeypatch):
    import types

    from databricks.labs.gbx import manifest

    path = REPO / "scripts/manifest/check_surfaces.py"
    spec = importlib.util.spec_from_file_location("gbx_check_surfaces_empty", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    def load():
        return types.SimpleNamespace(functions={}, private={})

    load.cache_clear = lambda: None
    monkeypatch.setattr(manifest, "load", load)
    found = mod.run()
    assert found and "0 stable function rows" in found[0]
