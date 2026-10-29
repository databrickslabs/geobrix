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
