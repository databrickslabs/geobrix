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


# --- check-binding-parity.py ----------------------------------------------------------------------------


def test_binding_parity_skips_a_stable_manifest_name_but_fails_a_legacy_name(
    monkeypatch, capsys
):
    mod = _load("check-binding-parity")
    monkeypatch.setattr(
        mod, "canonical_sql", lambda: {"gbx_bng_eastnorthasbng", "gbx_rst_fake"}
    )
    monkeypatch.setattr(mod, "scala_names", lambda: set())
    monkeypatch.setattr(mod, "python_names", lambda: set())
    monkeypatch.setattr(mod, "function_info_keys", lambda: set())
    assert mod.main() == 1
    out = capsys.readouterr().out
    assert "gbx_rst_fake" in out and "gbx_bng_eastnorthasbng" not in out


# --- check-since.py -------------------------------------------------------------------------------------


def test_check_since_passes_on_the_committed_tree():
    r = _run("check-since")
    assert r.returncode == 0, r.stdout + r.stderr


def test_check_since_reports_a_legacy_name_without_a_row_but_not_a_manifest_name(
    monkeypatch, capsys
):
    mod = _load("check-since")
    owned = mod.stable_names()
    legacy = next(n for n in mod._load_registered() if n not in owned)
    assert "gbx_rst_slope" in mod._load_registered() and "gbx_rst_slope" in owned
    monkeypatch.setattr(mod, "_load_since", lambda *a, **k: {})
    assert mod.main() == 1
    err = capsys.readouterr().err
    assert legacy in err and "gbx_rst_slope" not in err


# --- check-doc-coverage.py / check-diagram-coverage.py --------------------------------------------------


def test_check_doc_coverage_passes_and_leaves_the_manifest_names_out_of_its_count():
    r = _run("check-doc-coverage")
    assert r.returncode == 0, r.stdout[-800:] + r.stderr[-800:]
    mod = _load("check-doc-coverage")
    listed = [
        ln.strip()
        for ln in mod.REGISTERED_TXT.read_text().splitlines()
        if ln.strip() and not ln.startswith("#")
    ]
    owned = mod.stable_names()
    kept = len([n for n in listed if n not in owned])
    assert f"Canonical registered functions: {kept}" in r.stdout
    assert kept < len(listed)  # gbx_rst_slope is in the legacy list and the manifest


def test_the_diagram_check_counts_the_manifests_rst_names():
    mod = _load("check-diagram-coverage")
    names = mod.canonical_rst()
    assert {"rst_transformcrs", "rst_merge_agg"} <= names
    assert not any(n.startswith("gbx_") for n in names)
