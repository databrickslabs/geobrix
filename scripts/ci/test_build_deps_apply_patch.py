"""apply_patch in scripts/native/build-deps.sh: a pinned source patch (pins.conf OPENJPEG_PATCH_*) on unpacked source.

- the patch's sha256 is checked against its pin first: a wrong sum refuses, before any file changes;
- a patch that applies exactly is applied;
- --fuzz=0 --forward: a hunk that does not apply exactly, or a patch already in, fails the build rather than fuzzing.

The function is cut out of the script and run in a bash subprocess on a tmp source tree (the script itself sources
/recipes/build-lib.sh and builds in a container).
"""

import hashlib
import re
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "native" / "build-deps.sh"
PINS = Path(__file__).resolve().parents[1] / "native" / "pins.conf"
BASH = shutil.which("bash")

NEEDS = shutil.which("sha256sum") and shutil.which("patch")
pytestmark = pytest.mark.skipif(not NEEDS, reason="needs sha256sum and patch")

ORIGINAL = "".join(f"line {i}\n" for i in range(1, 21))
PATCH = """\
--- a/f.txt
+++ b/f.txt
@@ -8,7 +8,7 @@
 line 8
 line 9
 line 10
-line 11
+line 11 fixed
 line 12
 line 13
 line 14
"""


def _function():
    m = re.search(r"^apply_patch\(\) \{\n.*?^\}\n", SCRIPT.read_text(), re.S | re.M)
    assert m, "apply_patch() not found in build-deps.sh"
    return m.group(0)


def _apply(tmp_path, name, sha, tree_text=ORIGINAL, patch_text=PATCH):
    """Run apply_patch <name> <sha> in a source tree holding f.txt; the patches live in SRC. -> (rc, output, f.txt)."""
    src, tree = tmp_path / "src", tmp_path / "tree"
    src.mkdir(exist_ok=True)
    tree.mkdir(exist_ok=True)
    (src / name).write_text(patch_text)
    (tree / "f.txt").write_text(tree_text)
    res = subprocess.run(
        [BASH, "-c", f'set -euo pipefail\nSRC="{src}"\nDEP=openjpeg\n{_function()}\napply_patch "{name}" "{sha}"'],
        cwd=tree, capture_output=True, text=True,
    )  # fmt: skip
    return res.returncode, res.stdout + res.stderr, (tree / "f.txt").read_text()


def _sha(text=PATCH):
    return hashlib.sha256(text.encode()).hexdigest()


def test_a_patch_with_its_pinned_sum_applies(tmp_path):
    rc, out, text = _apply(tmp_path, "fix.patch", _sha())
    assert rc == 0, out
    assert "line 11 fixed\n" in text and "line 11\n" not in text
    assert not list((tmp_path / "tree").glob("*.orig"))  # --no-backup-if-mismatch


def test_a_wrong_sum_refuses_before_changing_a_file(tmp_path):
    rc, out, text = _apply(tmp_path, "fix.patch", "0" * 64)
    assert rc != 0
    assert "BUILD FAILED: fix.patch sha256" in out
    assert text == ORIGINAL


def test_a_patch_that_does_not_apply_exactly_fails_instead_of_fuzzing(tmp_path):
    # the outer context lines differ (an upstream edit): with fuzz allowed, patch would still apply it
    moved = ORIGINAL.replace("line 8\n", "line eight\n").replace("line 14\n", "line fourteen\n")
    rc, out, text = _apply(tmp_path, "fix.patch", _sha(), tree_text=moved)
    assert rc != 0
    assert "BUILD FAILED: fix.patch does not apply to openjpeg" in out
    assert text == moved and not list((tmp_path / "tree").glob("*.orig"))


def test_a_second_application_fails_the_build(tmp_path):
    assert _apply(tmp_path, "fix.patch", _sha())[0] == 0
    # the fix is already in: --forward refuses it, and the function turns that into a build failure
    rc, out, text = _apply(tmp_path, "fix.patch", _sha(), tree_text=ORIGINAL.replace("line 11\n", "line 11 fixed\n"))
    assert rc != 0
    assert "does not apply to openjpeg" in out
    assert text.count("line 11 fixed") == 1


def test_the_openjpeg_patch_pins_are_what_the_build_passes():
    pins = dict(re.findall(r"^(OPENJPEG_PATCH_\w+)=(.*)$", PINS.read_text(), re.M))
    assert re.fullmatch(r"[0-9a-f]{64}", pins["OPENJPEG_PATCH_SHA256"])
    assert pins["OPENJPEG_PATCH_FILE"].endswith(".patch")
    assert 'apply_patch "$OPENJPEG_PATCH_FILE" "$OPENJPEG_PATCH_SHA256"' in SCRIPT.read_text()


def test_the_repository_ships_the_openjpeg_patch_with_its_pinned_sum():
    """prefetch.sh copies the patch from scripts/native/patches/ (never github.com): its bytes are the pinned ones,
    and OpenJPEG's BSD-2-Clause licence ships beside it."""
    pins = dict(re.findall(r"^(OPENJPEG_PATCH_\w+)=(.*)$", PINS.read_text(), re.M))
    shipped = PINS.parent / "patches" / pins["OPENJPEG_PATCH_FILE"]
    assert hashlib.sha256(shipped.read_bytes()).hexdigest() == pins["OPENJPEG_PATCH_SHA256"]
    assert "2-clauses" in (PINS.parent / "patches" / "LICENSE-openjpeg").read_text()
