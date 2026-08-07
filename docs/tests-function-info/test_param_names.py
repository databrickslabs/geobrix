import subprocess, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

def test_param_names_check_passes():
    r = subprocess.run(
        [sys.executable, str(REPO / "docs/scripts/check-param-names.py")],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, f"check-param-names failed:\n{r.stdout}\n{r.stderr}"
