import subprocess, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

def test_param_names_check_passes():
    r = subprocess.run(
        [sys.executable, str(REPO / "docs/scripts/check-param-names.py")],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, f"check-param-names failed:\n{r.stdout}\n{r.stderr}"

def test_find_def_strips_inline_comment():
    import importlib.util, sys, tempfile
    from pathlib import Path as pl
    p = Path(__file__).resolve().parents[2] / "docs/scripts/check-param-names.py"
    spec = importlib.util.spec_from_file_location("cpn", p)
    cpn = importlib.util.module_from_spec(spec); spec.loader.exec_module(cpn)
    src = 'def rst_evi(  # noqa: E741\n    tile,\n    red_idx,\n) -> int:\n    return 0\n'
    with tempfile.TemporaryDirectory() as d:
        fp = pl(d) / "functions.py"; fp.write_text(src)
        params = cpn.extract_py_params(fp, "gbx_rst_evi")
    assert params == ["tile", "red_idx"], f"inline-comment def misparsed: {params}"

def test_invariant_b_present():
    import importlib.util, sys
    from pathlib import Path
    p = Path(__file__).resolve().parents[2] / "docs/scripts/check-param-names.py"
    spec = importlib.util.spec_from_file_location("cpn", p)
    m = importlib.util.module_from_spec(spec); sys.modules["cpn"] = m
    spec.loader.exec_module(m)
    assert hasattr(m, "check_invariant_b"), "check_invariant_b not implemented"
