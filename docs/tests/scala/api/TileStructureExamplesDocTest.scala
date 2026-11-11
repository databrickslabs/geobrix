/*
 * Compile-time validation for the Tile Structure Scala examples.
 */
package docs.tests.scala.api

import org.scalatest.funsuite.AnyFunSuite

class TileStructureExamplesDocTest extends AnyFunSuite {

  test("TileStructureExamples constants are defined") {
    assert(TileStructureExamples.ACCESSING_TILE_FIELDS_SCALA.nonEmpty)
    assert(TileStructureExamples.ACCESSING_TILE_FIELDS_SCALA_output.nonEmpty)
    assert(TileStructureExamples.ACCESSING_TILE_FIELDS_SCALA_output.contains("|cellid|"))
  }
}
