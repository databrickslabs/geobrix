"""The docs/scripts checks that now skip or read the function manifest: the path neutralizer of the function-info
generator, the release-notes name diff, and the manifest exclusion of each legacy check (a stable manifest name is
the manifest gate's; every other name is still checked)."""

import importlib.util
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


# --- generate-function-info.py: neutral_paths -----------------------------------------------------------


def test_neutral_paths_shows_a_checkout_path_as_a_volume_path_and_keeps_the_file_name():
    gen = _load("generate-function-info")
    root = gen.REPO_ROOT
    text = f"x '{root}/src/test/resources/a/b.clr') y"
    assert (
        gen.neutral_paths(text) == "x '/Volumes/<catalog>/<schema>/<volume>/b.clr') y"
    )
    assert (
        gen.neutral_paths(f'"{root}/c.tif"')
        == '"/Volumes/<catalog>/<schema>/<volume>/c.tif"'
    )
    assert (
        gen.neutral_paths("SELECT '/Volumes/c/s/v/d.tif'")
        == "SELECT '/Volumes/c/s/v/d.tif'"
    )


# --- check-release-notes-functions.py -------------------------------------------------------------------


def _notes(monkeypatch, files):
    """The script with `_show` returning canned text per (rev, file name)."""
    mod = _load("check-release-notes-functions")
    monkeypatch.setattr(mod, "_show", lambda rev, path: files.get((rev, path.name)))
    return mod


LEGACY = "gbx_old_a\n# comment\n"
MANIFEST = (
    '{"functions": [{"sql_name": "gbx_new_b"}, {"sql_name": "gbx_old_a"},'
    ' {"sql_name": "gbx_mvp_c", "status": "mvp"}]}'
)


def test_added_names_are_the_set_difference_between_the_ranges_ends(monkeypatch):
    mod = _notes(
        monkeypatch,
        {
            ("base", "registered_functions.txt"): LEGACY,
            ("base", "functions.json"): '{"functions": [{"sql_name": "gbx_old_a"}]}',
            ("end", "registered_functions.txt"): LEGACY,
            ("end", "functions.json"): MANIFEST,
        },
    )
    # only the new stable manifest name: not the probe row, not the name that was already there
    assert mod.added_functions("base..end") == ["gbx_new_b"]


def test_a_manifest_absent_at_the_base_counts_all_its_stable_rows_as_added(monkeypatch):
    mod = _notes(
        monkeypatch,
        {
            ("base", "registered_functions.txt"): "",
            ("base", "functions.json"): "",  # the revision exists; the file did not
            ("end", "registered_functions.txt"): "",
            ("end", "functions.json"): MANIFEST,
        },
    )
    assert mod.added_functions("base..end") == ["gbx_new_b", "gbx_old_a"]


def test_an_unreadable_revision_or_manifest_is_an_advisory_skip(monkeypatch):
    mod = _notes(monkeypatch, {})  # _show gives None for every file
    assert mod.added_functions("base..end") is None
    mod = _notes(
        monkeypatch,
        {
            ("base", "registered_functions.txt"): "",
            ("base", "functions.json"): "{not json",
            ("end", "registered_functions.txt"): "",
            ("end", "functions.json"): MANIFEST,
        },
    )
    assert mod.added_functions("base..end") is None
    mod = _notes(
        monkeypatch,
        {
            ("base", "registered_functions.txt"): "",
            ("base", "functions.json"): '{"functions": [{"no_name": 1}]}',
            ("end", "registered_functions.txt"): "",
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


# --- check-since.py -------------------------------------------------------------------------------------


def test_check_since_passes_on_the_committed_tree():
    r = _run("check-since")
    assert r.returncode == 0, r.stdout + r.stderr


def test_check_since_reads_the_manifest_rows(monkeypatch, capsys):
    mod = _load("check-since")
    rows = {
        "gbx_rst_ok": {"since": "0.5.0"},
        "gbx_rst_nosince": {},
        "gbx_rst_bad": {"since": "v1"},
        "gbx_rst_split": {"since": "0.6.0"},
    }
    monkeypatch.setattr(mod, "stable_rows", lambda: rows)
    monkeypatch.setattr(mod, "_load_registered", lambda: ["gbx_zz_legacy_probe"])
    monkeypatch.setattr(
        mod, "_load_since", lambda *a, **k: {"gbx_rst_split": "0.5.0", "gbx_x": "0.1.0"}
    )
    assert mod.main() == 1
    err = capsys.readouterr().err
    assert "gbx_rst_nosince" in err and "gbx_zz_legacy_probe" in err
    assert "gbx_rst_bad" in err and "not a version" in err
    assert "gbx_rst_split" in err and "function-since.tsv says 0.5.0" in err
    assert "gbx_rst_ok" not in err


def test_check_since_fails_on_a_row_that_disagrees_with_the_tsv_alone(
    monkeypatch, capsys
):
    mod = _load("check-since")
    monkeypatch.setattr(mod, "stable_rows", lambda: {"gbx_rst_a": {"since": "0.6.0"}})
    monkeypatch.setattr(mod, "_load_registered", lambda: [])
    monkeypatch.setattr(mod, "_load_since", lambda *a, **k: {"gbx_rst_a": "0.5.0"})
    assert mod.main() == 1
    assert "function-since.tsv says 0.5.0" in capsys.readouterr().err


def test_check_since_fails_on_an_empty_source(monkeypatch, capsys):
    mod = _load("check-since")
    monkeypatch.setattr(mod, "stable_rows", lambda: {})
    assert mod.main() == 1
    assert "nothing to check" in capsys.readouterr().err


# --- check-doc-coverage.py / check-diagram-coverage.py --------------------------------------------------


def test_check_doc_coverage_passes_and_counts_every_manifest_function():
    r = _run("check-doc-coverage")
    assert r.returncode == 0, r.stdout[-800:] + r.stderr[-800:]
    mod = _load("check-doc-coverage")
    n = len(mod.canonical_sql())
    assert n >= len(mod.stable_rows()) > 200
    assert f"Canonical registered functions: {n}" in r.stdout


def test_check_doc_coverage_uses_the_row_page_and_fails_an_unmapped_name(tmp_path):
    mod = _load("check-doc-coverage")
    page = tmp_path / "p.mdx"
    page.write_text("### zz_documented\n")
    mod.stable_rows = lambda: {
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
    monkeypatch.setattr(mod, "stable_names", lambda: set())
    assert any("no stable rows" in f for f in mod.check_tab_completeness())


# --- check-param-names.py -------------------------------------------------------------------------------


def test_param_names_hold_the_published_names_to_the_manifest(monkeypatch):
    mod = _load("check-param-names")
    assert mod.check_against_manifest() == []
    assert len(mod.load_fixture()) > 150  # a real number of signatures is checked
    rows = mod.stable_rows()
    renamed = {n: dict(r) for n, r in rows.items()}
    first = next(n for n in mod.load_fixture() if n not in mod.load_waiver())
    renamed[first]["params"] = [{"name": "zz"}]
    monkeypatch.setattr(mod, "stable_rows", lambda: renamed)
    assert any(first in v for v in mod.check_against_manifest())
    monkeypatch.setattr(
        mod, "stable_rows", lambda: {k: v for k, v in rows.items() if k != first}
    )
    assert any("not a stable manifest row" in v for v in mod.check_against_manifest())


def test_param_names_fail_on_an_empty_source(monkeypatch):
    mod = _load("check-param-names")
    monkeypatch.setattr(mod, "stable_rows", lambda: {})
    assert any(v.startswith("[empty]") for v in mod.check_against_manifest())
    monkeypatch.setattr(mod, "load_fixture", lambda: {})
    monkeypatch.setattr(mod, "stable_rows", lambda: {"gbx_a": {"params": []}})
    assert any("lists no functions" in v for v in mod.check_against_manifest())
    assert mod.main.__module__  # main() returns 1 on any violation
    monkeypatch.setattr(sys, "argv", ["check-param-names"])
    assert mod.main() == 1


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
