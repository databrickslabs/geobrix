import subprocess, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

def test_param_names_check_passes():
    r = subprocess.run(
        [sys.executable, str(REPO / "docs/scripts/check-param-names.py")],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, f"check-param-names failed:\n{r.stdout}\n{r.stderr}"

def test_invariant_b_present():
    import importlib.util, sys
    from pathlib import Path
    p = Path(__file__).resolve().parents[2] / "docs/scripts/check-param-names.py"
    spec = importlib.util.spec_from_file_location("cpn", p)
    m = importlib.util.module_from_spec(spec); sys.modules["cpn"] = m
    spec.loader.exec_module(m)
    assert hasattr(m, "check_invariant_b"), "check_invariant_b not implemented"
