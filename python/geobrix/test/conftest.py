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

CONDITION TO MAINTAIN (every light-tier addition must do BOTH):
  1. Add the new light test dir to ``_LIGHT_TEST_DIRS`` below, so the heavyweight
     CI phase skips it (otherwise its module-level light imports -- e.g.
     ``from pmtiles.reader import ...`` / ``import pandas`` -- raise
     ``ModuleNotFoundError`` at collection and fail the heavy build).
  2. Add the new light test dir to the explicit pytest dir list in the LIGHT CI
     phase (``.github/actions/pyrx_build/action.yml``), so it is actually RUN.
     The light tier is exercised ONLY in the light phase; the heavy phase skips it.
Light test dirs so far: pyrx, pyvx, pygx, pmtiles_light, stac, plus bench + ds, viz.
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
    "vizx",
]

# Skip the light-tier suites when their dependencies are not installed.
if importlib.util.find_spec("rasterio") is None:
    collect_ignore = list(_LIGHT_TEST_DIRS)
