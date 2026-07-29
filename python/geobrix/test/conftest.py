"""Test-root conftest: dependency-aware collection guard for the light-tier suites.

WHY: The light-tier test dirs import packages whose import chain pulls in
light-tier deps -- ``rasterio``, ``shapely``, ``h3``, ``quadbin``, ``pmtiles``,
``pandas``, ``numpy``, ``scipy`` -- that are NOT installed in the remote
heavyweight CI Python environment (``requirements-ci.txt`` ships none of them;
the lightweight CI uses ``requirements-pyrx-ci.txt``). pytest imports a test
module at COLLECTION time to read it, so a bare ``-m "not bench"`` marker filter
does not help: the import (and its ``ModuleNotFoundError``) fires before the
marker is ever seen, turning into a collection ERROR that fails the build.

A directory-level ``collect_ignore`` prevents pytest from even importing those
dirs when the deps are absent. It is gated on ``rasterio`` being importable (the
canonical signal for the light-tier dependency set; ``pmtiles``/``pandas``/
``shapely`` et al. ship in the same locks):

  * Remote heavyweight CI (no rasterio) -> every light dir ignored -> skipped.
  * Local / Docker / pyrx CI (rasterio present) -> not ignored -> collected and run.
  * Explicit ``gbx:test:python --path test/<dir>/...`` in Docker -> deps present,
    so nothing is ignored and the targeted tests still run.

This is robust to the real cause (missing deps) rather than relying on each
caller remembering to pass ``--ignore``.

CONDITION TO MAINTAIN (every light-tier addition must do ALL THREE):
  1. Add the new light test dir to ``_LIGHT_TEST_DIRS`` below, so the heavyweight
     CI phase skips it (otherwise its module-level light imports -- e.g.
     ``from pmtiles.reader import ...`` / ``import pandas`` -- raise
     ``ModuleNotFoundError`` at collection and fail the heavy build).
  2. Add the new light test dir to the explicit pytest dir list in the LIGHT CI
     phase (``.github/actions/pyrx_build/action.yml``), so it is actually RUN.
     The light tier is exercised ONLY in the light phase; the heavy phase skips it.
  3. Add the new light test dir to the explicit ``--ignore`` list in the HEAVY CI
     phase (``.github/actions/python_build/action.yml``, ``LIGHT_IGNORES``). That
     list MUST stay identical to ``_LIGHT_TEST_DIRS``. The collect_ignore fallback
     below already skips light dirs when rasterio is absent, but the explicit
     --ignore is belt-and-suspenders: it guarantees a light dir never runs in heavy
     even if the heavy env drifts (gains rasterio) or a caller targets a light dir
     directly.
Light test dirs so far: pyrx, pyvx, pygx, pmtiles_light, stac, earthdata, vizx,
sample, plus bench + ds.

``bench`` is a light dir (its modules import rasterio/shapely/h3/quadbin via the
bench harness) and satisfies all three conditions: (1) it is in ``_LIGHT_TEST_DIRS``
below; (2) it RUNS in the light phase -- listed in the pyrx_build action pytest dir
list; (3) it is skipped in the heavy phase -- present in the heavy action's
``LIGHT_IGNORES`` (--ignore=test/bench). Its unit tests guard the light bench FnSpec
registry and the cross-tier coverage/count invariants (previously CI ran none of
test/bench, so bench-spec drift went uncaught).
"""

import importlib.util

# Every test dir whose modules import light-tier-only deps. Ignored in the
# heavyweight CI env (no rasterio); collected + run in the light env.
_LIGHT_TEST_DIRS = [
    "bench",
    "ds",
    "pyrx",
    "pyvx",
    "pygx",
    "pmtiles_light",
    "stac",
    "earthdata",
    "vizx",
    "sample",
]

# Skip the light-tier suites when their dependencies are not installed.
if importlib.util.find_spec("rasterio") is None:
    collect_ignore = list(_LIGHT_TEST_DIRS)
