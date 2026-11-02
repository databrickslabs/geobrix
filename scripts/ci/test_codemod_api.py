"""The API codemod (a migration tool): a second run over the repo's callers changes nothing."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "manifest"))

import codemod_api  # noqa: E402


def test_a_second_codemod_run_over_the_callers_changes_nothing():
    from databricks.labs.gbx import manifest

    targets = codemod_api._targets(sorted(manifest.load().functions))
    for root in ("python/geobrix/test", "docs/tests/python", "examples"):
        for path in sorted((REPO_ROOT / root).rglob("*")):
            if path.suffix == ".py":
                old = path.read_text()
                new, _ = codemod_api.rewrite(old, targets)
                assert new == old, f"{path} still changes"
            elif path.suffix == ".ipynb":
                old, new, _ = codemod_api._notebook(path, targets)
                assert new == old, f"{path} still changes"

    # the new alias bound only inside another function does not reach the rewritten call
    name = next(n for n, m in targets.items() if m == "gridx.bng")
    src = (
        "from databricks.labs.gbx.pygx import functions as gx\n\n\n"
        "def other():\n"
        "    from databricks.labs.gbx.gridx.bng import functions as bx\n\n"
        "    return bx\n\n\n"
        f"def test_it():\n    return gx.{name}(1)\n"
    )
    new, issues = codemod_api.rewrite(src, targets)
    assert "\nfrom databricks.labs.gbx.gridx.bng import functions as bx\n" in new
    assert codemod_api._undefined(new, {"bx"}) == [] and not issues
    assert codemod_api.rewrite(new, targets)[0] == new
