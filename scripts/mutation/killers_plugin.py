"""pytest plugin for the full-mutant guidance run: record which test killed each mutant.

Loaded into the throwaway copy only (``-p gbx_mut_killers``), beside the Tier 0 plugin. mutmut runs a mutant's tests with
``-x``, so the first test that fails under a mutant is the test that killed it. The plugin appends one JSON line per
failure (mutant name, test id) to the file named by ``GBX_MUT_KILLERS``; the mutmut worker that runs the mutant is the
process that writes, and an append of one short line is atomic, so the workers share the file.
"""

import json
import os

_NOT_A_MUTANT = ("", "fail", "stats", "mutant_generation", "list_all_tests")


def pytest_runtest_logreport(report):
    if report.outcome != "failed":
        return
    path = os.environ.get("GBX_MUT_KILLERS")
    if not path:
        return
    try:
        from mutmut.mutation.trampoline import get_mutant_under_test

        mutant = get_mutant_under_test()
    except Exception:  # noqa: BLE001 - a plugin must never break the run it watches
        return
    if mutant in _NOT_A_MUTANT:
        return
    line = (
        json.dumps({"mutant": mutant, "test": report.nodeid, "when": report.when})
        + "\n"
    )
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, line.encode())
    finally:
        os.close(fd)
