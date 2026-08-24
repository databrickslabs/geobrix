// ProjGridRegistrySpec.scala
package com.databricks.labs.gbx.operations

import org.scalatest.funsuite.AnyFunSuite

class ProjGridRegistrySpec extends AnyFunSuite {
  test("accumulates and dedupes preserving order") {
    ProjGridRegistry.set(Seq("/Volumes/a"), replace = true)
    ProjGridRegistry.set(Seq("/Volumes/b", "/Volumes/a"), replace = false)
    assert(ProjGridRegistry.get == Seq("/Volumes/a", "/Volumes/b"))
  }
  test("replace resets") {
    ProjGridRegistry.set(Seq("/Volumes/a", "/Volumes/b"), replace = true)
    assert(ProjGridRegistry.set(Seq("/Volumes/c"), replace = true) == Seq("/Volumes/c"))
  }
}
