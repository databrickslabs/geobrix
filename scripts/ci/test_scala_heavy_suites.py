"""The heavy CI job runs the Scala suites listed in .github/actions/scala_build/suites-heavy.txt.

The action joins that file into -Dsuites when it is given no suite_pattern. A suite that matches no entry is skipped
without a sound, and an entry that matches nothing (a renamed or deleted suite) runs nothing, so this reads the
test sources and checks both ways. No Docker, no Maven.
"""

import re

from conftest import REPO_ROOT

LIST = REPO_ROOT / ".github" / "actions" / "scala_build" / "suites-heavy.txt"
TESTS = REPO_ROOT / "src" / "test" / "scala"
_CLASS = re.compile(r"^\s*(abstract\s+)?class\s+(\w+)[^{=]*?extends\s+([^{]*)", re.M)
_PACKAGE = re.compile(r"^package\s+([\w.]+)", re.M)
_SUITE_BASE = re.compile(r"Suite|Spec|Test")


def _suites():
    """Every concrete suite class under src/test/scala, by fully qualified name."""
    found = set()
    for f in TESTS.rglob("*.scala"):
        text = f.read_text()
        pkg = _PACKAGE.search(text)
        for abstract, name, bases in _CLASS.findall(text):
            if not abstract and _SUITE_BASE.search(bases):
                found.add(f"{pkg.group(1)}.{name}" if pkg else name)
    return found


def _list():
    """(entries, excluded) from the list file."""
    entries, excluded = [], []
    for raw in LIST.read_text().splitlines():
        line = raw.strip()
        if line.startswith("# excluded:"):
            excluded.append(line.split(":", 1)[1].strip())
        elif line and not line.startswith("#"):
            entries.append(line)
    return entries, excluded


def _matches(entry, fqcn):
    return fqcn.startswith(entry[:-1]) if entry.endswith(".*") else fqcn == entry


def test_suites_are_found():
    assert len(_suites()) > 100  # the scan itself works


def test_every_entry_matches_a_suite():
    suites = _suites()
    dead = [e for e in _list()[0] if not any(_matches(e, s) for s in suites)]
    assert not dead, f"suites-heavy.txt entries that match no suite under src/test/scala: {dead}"


def test_every_excluded_name_is_a_suite_and_matches_no_entry():
    entries, excluded = _list()
    suites = _suites()
    assert excluded, "the dropped suites are named on '# excluded:' lines"
    missing = [x for x in excluded if x not in suites]
    assert not missing, f"'# excluded:' names that are no suite under src/test/scala (delete the line): {missing}"
    runs = [x for x in excluded if any(_matches(e, x) for e in entries)]
    assert not runs, f"excluded suites that an entry still runs: {runs}"


def test_every_suite_is_run_or_excluded():
    entries, excluded = _list()
    skipped = sorted(s for s in _suites() if s not in excluded and not any(_matches(e, s) for e in entries))
    assert not skipped, (
        f"suites that CI would skip without a sound; add an entry to suites-heavy.txt or an '# excluded:' line: {skipped}"
    )


def test_the_action_reads_the_list_when_given_no_pattern():
    action = (REPO_ROOT / ".github" / "actions" / "scala_build" / "action.yml").read_text()
    assert "suites-heavy.txt" in action
    # the per-package workflows pass their own pattern and must keep doing so
    for wf in ("build_scala_by_package.yml", "codecov-scala-parallel.yml"):
        assert "suite_pattern:" in (REPO_ROOT / ".github" / "workflows" / wf).read_text(), wf
