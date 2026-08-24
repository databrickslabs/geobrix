package com.databricks.labs.gbx.bench

import org.scalatest.Tag

/**
 * ScalaTest tag for tests that require an isolated PROJ search-path context —
 * i.e., tests that must run in a fresh JVM where PROJ has not yet cached a grid
 * lookup for the fixture grid filename.
 *
 * PROJ caches grid lookups process-globally on first use. In the default CI run
 * (`forkMode once`, all suites sharing one JVM), dozens of suites performing
 * plain EPSG transforms initialize GDAL/PROJ before
 * `RST_TransformCrsGridSpec`, freezing the PROJ context so a later
 * `SetConfigOption("PROJ_DATA", …)` no longer takes effect for the fixture
 * filename. Tests tagged `RequiresProjIsolation` only produce a meaningful
 * result in an isolated JVM (e.g., `-Dsuites=<single spec>`) and must therefore
 * be excluded from the shared-JVM default CI run.
 *
 * Excluded by default via the `${tagsToExclude}` Maven property (pom.xml). To
 * run these tests on demand in isolation:
 *
 * {{{
 *   mvn test -PskipScoverage -DskipTests=false \
 *     -Dsuites='com.databricks.labs.gbx.rasterx.RST_TransformCrsGridSpec' \
 *     -DtagsToExclude=com.databricks.labs.gbx.bench.NoOpExcludeSentinel
 * }}}
 *
 * or via the gbx:test:scala command:
 *
 * {{{
 *   bash scripts/commands/gbx-test-scala.sh \
 *     --suite 'com.databricks.labs.gbx.rasterx.RST_TransformCrsGridSpec' \
 *     --tags-to-exclude com.databricks.labs.gbx.bench.NoOpExcludeSentinel
 * }}}
 */
object RequiresProjIsolation extends Tag("com.databricks.labs.gbx.bench.RequiresProjIsolation")
