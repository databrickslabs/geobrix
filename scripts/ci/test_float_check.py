"""gbx:build:wheel --float-check's pure parts (scripts/native/float_check.py): reading a pip report.

- a package the runtime preinstalls at another version is a move, with the requirement chain that asks for it;
- a package the runtime lacks is new; the requested wheel itself is neither;
- an extra's requirement counts only for that extra; a marker for another platform does not count;
- the stub dist-infos make pip see the runtime's packages installed, pip itself left real;
- the installs are the core, each extra alone and all extras together; an extra's moves are judged as install_moves'
  'full' install.
"""

import importlib.util
import subprocess
import sys
from pathlib import Path

NATIVE = Path(__file__).resolve().parents[1] / "native"
sys.path.insert(0, str(NATIVE))
_spec = importlib.util.spec_from_file_location("float_check", NATIVE / "float_check.py")
fc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fc)


def _item(name, version, requires=(), requested=False):
    return {"requested": requested, "metadata": {"name": name, "version": version, "requires_dist": list(requires)}}


REPORT = {
    "install": [
        _item("geobrix", "0.6.0+env6", ['httpx2>=2.12', 'contextily; extra == "vizx"', 'urllib3<3'], requested=True),
        _item("httpx2", "2.16.0", ["idna>=3.18", "truststore", 'pywin32; sys_platform == "win32"']),
        _item("idna", "3.20"),
        _item("truststore", "0.10.5"),
        _item("contextily", "1.7.0", ["xyzservices", "requests"]),
        _item("xyzservices", "2026.9.1"),
    ]
}
RUNTIME = {"idna": "3.11", "urllib3": "2.5.0", "requests": "2.32.5", "pillow": "12.0.0"}


def test_classify_names_the_move_and_its_chain():
    moves, new = fc.classify(REPORT, RUNTIME, ())
    assert moves == [("idna", "3.11", "3.20", "geobrix -> httpx2 -> idna")]
    assert ("truststore", "0.10.5", "geobrix -> httpx2 -> truststore") in new
    assert all(n != "geobrix" for n, _v, _c in new)  # the requested wheel is neither


def test_an_extras_requirement_counts_only_for_that_extra():
    assert "contextily" not in fc.chains(REPORT, ())
    chains = fc.chains(REPORT, ("vizx",))
    assert chains["xyzservices"] == "geobrix[vizx] -> contextily -> xyzservices"
    assert "pywin32" not in chains  # a marker for another platform


def test_no_move_when_the_report_keeps_the_runtimes_versions():
    report = {"install": [_item("geobrix", "0.6.0+env6", ["pillow>=12.0.0,<14"], requested=True)]}
    assert fc.classify(report, RUNTIME, ()) == ([], [])  # pillow 12.0.0 satisfies: pip lists nothing


def test_a_downgrade_is_a_move_too():
    report = {"install": [_item("geobrix", "0.6.0+env6", ["urllib3<2.6"], requested=True), _item("urllib3", "2.5.0")]}
    moves, _new = fc.classify(report, {"urllib3": "2.6.3"}, ())
    assert moves == [("urllib3", "2.6.3", "2.5.0", "geobrix -> urllib3")]


def test_installed_stubs_are_what_pip_sees(tmp_path):
    n = fc.make_installed(tmp_path, {"pip": "25.0", "pillow": "12.0.0", "typing-extensions": "4.15.0"})
    assert n == 2 and not list(tmp_path.glob("pip-*"))
    out = subprocess.run(
        [sys.executable, "-c", "import importlib.metadata as m, sys; d = {x.metadata['Name']: x.version for x in "
         "m.distributions(path=[sys.argv[1]])}; print(sorted(d.items()))", str(tmp_path)],
        capture_output=True, text=True, check=True,
    ).stdout  # fmt: skip
    assert "('pillow', '12.0.0')" in out and "('typing-extensions', '4.15.0')" in out


def test_installs_and_their_allowed_moves():
    labels = [label for label, _x in fc.installs(6)]
    assert labels[0] == "core" and labels[-1] == "full"
    assert set(labels[1:-1]) == set(fc.extras_of(6)) and "zarr" in labels  # one per extra-<name>.txt
    assert "requests" not in fc.allowed_for(5, "core") and "requests" in fc.allowed_for(5, "stac")
    assert "idna" in fc.allowed_for(6, "core") and "anyio" not in fc.allowed_for(6, "full")
