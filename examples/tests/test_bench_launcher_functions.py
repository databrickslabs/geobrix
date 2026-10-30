"""The bench launcher stops on a mistyped ``bng_*`` name before it submits anything.

The on-cluster notebook rejects such a name too, but only after the job has started. Pure: the
launcher reads the leg table and the registry as text. Run:
  gbx:test:notebooks --path test_bench_launcher_functions.py
"""

import re

import push_and_run_bench_on_cluster as launcher


def test_a_mistyped_bng_name_is_refused_and_every_real_one_is_not():
    # names the bench knows: a leg (bng_kringexplode), a registered function (bng_cellfill)
    assert launcher._unknown_bng_names("bng_kringexplode,bng_cellfill,rst_width") == []
    assert launcher._unknown_bng_names("bng_kringexplod,rst_width,bng_nope") == [
        "bng_kringexplod",
        "bng_nope",
    ]
    # a list typed with spaces after the commas is the same list
    assert launcher._unknown_bng_names("rst_width, bng_nope , bng_cellfill") == [
        "bng_nope"
    ]
    assert launcher._function_names(" rst_width, bng_nope ,, ") == [
        "rst_width",
        "bng_nope",
    ]

    # every leg the bench can run is read from the table (a reformatted table would flag them all)
    readers = (launcher._BENCH_SRC / "readers.py").read_text()
    legs = [n[len("run_") :] for n in re.findall(r"^def (run_bng_\w+)\(", readers, re.M)]
    assert len(legs) >= 20 and launcher._unknown_bng_names(",".join(legs)) == []


def test_the_bench_installs_the_env_wheel_of_the_clusters_runtime():
    # DBR 17.3 / 18 run the env5 wheel, DBR 19 the env6 wheel (the other family's refuses to import)
    for key, env in (("17.3.x-scala2.13", 5), ("18.x-aarch64-scala2.13", 5), ("19.x-scala2.13", 6),
                     ("19.x-cpu-ml-scala2.13", 6)):  # fmt: skip
        assert launcher.env_of_runtime(key) == env, key


# ---- run hardening: the preflight, the smoke verdict and the watchdog wiring (no workspace)

import json
import types

import _preflight
import _run_watchdog

WHEEL = "/Volumes/c/s/v/geobrix-0.6.0+env6-py3-none-any.whl"


def test_the_preflight_refuses_a_spec_without_the_wheel_a_wrong_family_and_a_staged_wheel_that_is_not_the_build(tmp_path):
    assert _preflight.check_env_spec(_preflight.serverless_dependencies(WHEEL), WHEEL, 6) == []
    assert "does not name the wheel" in _preflight.check_env_spec(["markdown"], WHEEL, 6)[0]
    assert "+env6 family" in _preflight.check_env_spec([WHEEL], WHEEL, 5)[0]
    # the staged bytes against the local build
    (tmp_path / "dist" / "env6").mkdir(parents=True)
    built = tmp_path / "dist" / "env6" / "geobrix-0.6.0+env6-py3-none-any.whl"
    built.write_bytes(b"the build")
    sha = _preflight.file_sha256(built)
    local = _preflight.local_wheel(tmp_path, built.name)
    assert _preflight.check_wheel(local, sha, WHEEL) == []
    assert "not the local build" in _preflight.check_wheel(local, "0" * 64, WHEEL)[0]
    assert "no local build" in _preflight.check_wheel(None, sha, WHEEL)[0]


def run_outcome(payload, result="SUCCESS"):
    state = types.SimpleNamespace(life_cycle_state="TERMINATED", result_state=result, state_message="")
    return launcher.Outcome(run=types.SimpleNamespace(state=state), payload=json.dumps(payload))


def test_a_smoke_is_clean_only_when_the_import_check_passes_and_no_form_failed():
    ok = {"import_check": {"ok": True, "version": "0.6.0", "arch": "x86_64"}, "forms": [{"fn": "f"}], "errors": []}
    assert launcher.smoke_problems(run_outcome(ok), require_forms=True) == []
    bad = launcher.smoke_problems(
        run_outcome({**ok, "import_check": {"ok": False, "error": "ModuleNotFoundError: databricks.labs"}}),
        require_forms=True,
    )
    assert "worker-side import check failed" in bad[0] and "databricks.labs" in bad[0]
    err = {"fn": "gbx_bng", "level": "L4", "form": "uc-scalar", "failure": "UDF_USER_CODE_ERROR"}
    failed = launcher.smoke_problems(run_outcome({**ok, "errors": [err]}), require_forms=True)
    assert "gbx_bng L4/uc-scalar failed" in failed[0]
    assert launcher.smoke_problems(run_outcome({**ok, "errors": [err]}), require_forms=True, allow_errors=True) == []
    assert launcher.smoke_problems(run_outcome({**ok, "forms": []}), require_forms=True)  # nothing ran
    assert launcher.smoke_problems(run_outcome({}), require_forms=True)  # no exit payload


def test_a_run_whose_heartbeat_names_a_stuck_form_is_cancelled_by_the_launcher_and_reported(capsys):
    cancelled = []
    stuck = {"seq": 1, "state": "running", "form": "gbx_bng:L4:uc-scalar", "form_elapsed_s": 0.0,
             "stack": ["File levels_forms.py, line 470, in uc_scalar"]}  # fmt: skip

    class Files:
        @staticmethod
        def download(path):
            return types.SimpleNamespace(contents=types.SimpleNamespace(read=lambda: json.dumps(stuck).encode()))

    class Jobs:
        state = ("RUNNING", "")

        def get_run(self, run_id):
            life = types.SimpleNamespace(value=self.state[0])
            return types.SimpleNamespace(state=types.SimpleNamespace(life_cycle_state=life, result_state=None), tasks=[])

        def cancel_run(self, run_id):
            cancelled.append(run_id)
            self.state = ("TERMINATED", "CANCELED")

        def export_run(self, **kw):
            raise RuntimeError("no export in the fake")

    w = types.SimpleNamespace(jobs=Jobs(), files=Files)
    out = launcher.watch_to_end(w, 77, hb_path="/Volumes/x/heartbeat.json", form_cap_s=0.05, stale_s=60, poll_secs=0.02)
    text = capsys.readouterr().out
    assert cancelled == [77] and out.stall is not None and out.run is None
    assert "STALLED: gbx_bng:L4:uc-scalar after" in text and "uc_scalar" in text
    assert _run_watchdog.EXIT_STALLED == 3
