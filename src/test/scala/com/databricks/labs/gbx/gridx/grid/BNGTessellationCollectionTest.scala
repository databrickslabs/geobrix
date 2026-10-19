package com.databricks.labs.gbx.gridx.grid

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.scalatest.funsuite.AnyFunSuite

class BNGTessellationCollectionTest extends AnyFunSuite {

    // Valid, hole-free polygons crossing Easting 400000. Their eastern clips
    // contain two polygons + a boundary line, or a polygon + a boundary point.
    private val fixtures = Seq(
      "POLYGON ((399998 500001, 400002 500001, 400002 500002, 400000 500002, " +
          "400000 500003, 400002 500003, 400002 500004, 399998 500004, 399998 500001))",
      "POLYGON ((399998 500001, 400002 500001, 400002 500002, 399999 500002, " +
          "400000 500003, 399999 500004, 399998 500004, 399998 500001))"
    )

    test("BNG tessellation retains polygonal cells after mixed-collection clipping") {
        for (wkt <- fixtures) {
            val polygon = JTS.fromWKT(wkt)
            assert(polygon.isValid)
            val east = BNG.pointToCellID(400001, 500001, 2)
            val west = BNG.pointToCellID(399999, 500001, 2)
            val clipped = BNG.cellIdToGeometry(east).intersection(polygon)
            assert(clipped.getGeometryType == "GeometryCollection")
            val chips = BNG.tessellate(polygon, 2, keepCoreGeom = true).toVector
            assert(chips.map(_._1).toSet == Set(east, west))
            assert(chips.forall(_._3.getDimension == 2))
            assert(math.abs(chips.map(_._3.getArea).sum - polygon.getArea) < 1e-8)
            if (wkt == fixtures.head) {
                assert(chips.find(_._1 == east).get._3.getGeometryType == "MultiPolygon")
            }
            val withoutCore = BNG.tessellate(polygon, 2, keepCoreGeom = false).toVector
            assert(withoutCore.map(_._1).toSet == Set(east, west))
        }
    }

    test("BNG tessellation retains Polygon chips from a MultiPolygon input") {
        val polygon = JTS.fromWKT(fixtures.last)
        val multi = polygon.getFactory.createMultiPolygon(Array(polygon.asInstanceOf[org.locationtech.jts.geom.Polygon]))
        val chips = BNG.tessellate(multi, 2, keepCoreGeom = true).toVector
        assert(chips.nonEmpty)
        assert(chips.forall(_._3.getGeometryType == "Polygon"))
        assert(math.abs(chips.map(_._3.getArea).sum - polygon.getArea) < 1e-8)
    }

    test("BNG tessellation permits null geometry for full core cells") {
        val polygon = JTS.fromWKT("POLYGON ((395000 495000, 425000 495000, 425000 525000, 395000 525000, 395000 495000))")
        val chips = BNG.tessellate(polygon, 2, keepCoreGeom = false).toVector
        assert(chips.exists(t => t._2 && t._3 == null))
        assert(chips.forall(t => t._3 == null || !t._3.isEmpty))
    }
}
