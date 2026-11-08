"""pytest plugin for the mutation gate: Tier 0 means no Spark session, so a test that needs one is skipped.

Loaded into the throwaway copy only (``-p gbx_mut_tier0``), never into the repo's own test runs. It skips on the
fixture names (so the ``spark`` fixtures in the copied conftests are never built): the closure of a test's fixtures
lists the transitive ones too.
"""

import pytest

SPARK_FIXTURES = {
    "spark",
    "jvm_spark",
    "spark_connect_local",
    "spark_session",
    "spark_delta",
}


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    needed = SPARK_FIXTURES & set(getattr(item, "fixturenames", ()))
    if needed:
        pytest.skip(
            "Tier 0: no Spark under the mutation gate (%s)" % ", ".join(sorted(needed))
        )
