package com.databricks.labs.gbx.bench

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

/**
 * GDAL-free tests for BenchFingerprint.ofArray(Array[java.lang.Double]) — the boxed overload used by
 * the value reducers (RST_Max/Min/Avg/Median) now that they return NULL for all-nodata bands.
 *
 * Separate suite (no GDAL beforeAll) so it runs without the gdalalljni native lib: ofArray touches
 * only Jackson + java.lang.Double, and the BenchFingerprint object initializes to just an ObjectMapper.
 *
 * The contract under test: a null element must serialize as JSON `null` (matching python bench
 * fingerprint.py `_py(None) -> null`), NOT as NaN — so cross-tier fingerprints agree on all-nodata input.
 */
class BenchFingerprintNullTest extends AnyFunSuite {
  private val mapper = new ObjectMapper()

  test("boxed ofArray emits JSON null for a null element (matches python null, not NaN)") {
    val fp = BenchFingerprint.ofArray(Array[java.lang.Double](java.lang.Double.valueOf(42.0), null))
    val node = mapper.readTree(fp)
    assert(node.get("kind").asText() == "scalar_list")
    val values = node.get("values")
    assert(values.get(0).asDouble() == 42.0)
    // the null element must be a JSON null token — NOT a NaN number
    assert(values.get(1).isNull, s"expected JSON null for empty-band element, got: ${values.get(1)}")
    assert(!values.get(1).isNumber, "null element must not serialize as a (NaN) number")
    // and the serialized form contains `null`, never `NaN`
    assert(fp.contains("null"), s"serialized fingerprint should contain a JSON null: $fp")
    assert(!fp.contains("NaN"), s"serialized fingerprint must not contain NaN: $fp")
  }

  test("boxed ofArray with all real values matches the primitive overload") {
    val boxed = BenchFingerprint.ofArray(Array[java.lang.Double](
      java.lang.Double.valueOf(1.0), java.lang.Double.valueOf(2.0)))
    val prim = BenchFingerprint.ofArray(Array(1.0, 2.0))
    assert(boxed == prim, s"boxed and primitive overloads must agree on non-null input: $boxed vs $prim")
  }

  test("boxed ofArray with all-null (fully empty band set) emits all JSON nulls") {
    val fp = BenchFingerprint.ofArray(Array[java.lang.Double](null, null))
    val values = mapper.readTree(fp).get("values")
    assert(values.get(0).isNull && values.get(1).isNull)
    assert(!fp.contains("NaN"))
  }
}
