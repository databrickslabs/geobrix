package com.databricks.labs.gbx.vectorx.jts.legacy

import com.databricks.labs.gbx.vectorx.jts.GeometryTypeEnum
import org.locationtech.jts.geom.{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class InternalGeometryTest extends AnyFunSuite {

    // ====== Construction ======

    test("InternalGeometry should accept geometry data") {
        val boundaries = Array(Array(InternalCoord(Seq(0.0, 0.0))))
        val holes = Array(Array.empty[Array[InternalCoord]])
        val geom = InternalGeometry(
          typeId = 1,
          srid = 4326,
          boundaries = boundaries,
          holes = holes
        )
        geom.typeId shouldBe 1
        geom.srid shouldBe 4326
        geom.boundaries should not be null
        geom.holes should not be null
    }

    // ====== toJTS - POINT ======

    test("toJTS should convert POINT to JTS") {
        val boundaries = Array(Array(InternalCoord(Seq(1.0, 2.0))))
        val holes = Array(Array.empty[Array[InternalCoord]])
        val geom = InternalGeometry(
          typeId = GeometryTypeEnum.POINT.id,
          srid = 4326,
          boundaries = boundaries,
          holes = holes
        )
        val jts = geom.toJTS
        jts should not be null
        jts shouldBe a[Point]
        jts.getCoordinate.getX shouldBe 1.0
        jts.getCoordinate.getY shouldBe 2.0
    }

    // ====== toJTS - MULTIPOINT ======

    test("toJTS should convert MULTIPOINT to JTS") {
        val boundaries = Array(Array(
          InternalCoord(Seq(1.0, 2.0)),
          InternalCoord(Seq(3.0, 4.0))
        ))
        val holes = Array(Array.empty[Array[InternalCoord]])
        val geom = InternalGeometry(
          typeId = GeometryTypeEnum.MULTIPOINT.id,
          srid = 4326,
          boundaries = boundaries,
          holes = holes
        )
        val jts = geom.toJTS
        jts should not be null
        jts shouldBe a[MultiPoint]
        jts.getNumGeometries shouldBe 2
    }

    // ====== toJTS - LINESTRING ======

    test("toJTS should convert LINESTRING to JTS") {
        val boundaries = Array(Array(
          InternalCoord(Seq(0.0, 0.0)),
          InternalCoord(Seq(1.0, 1.0)),
          InternalCoord(Seq(2.0, 2.0))
        ))
        val holes = Array(Array.empty[Array[InternalCoord]])
        val geom = InternalGeometry(
          typeId = GeometryTypeEnum.LINESTRING.id,
          srid = 4326,
          boundaries = boundaries,
          holes = holes
        )
        val jts = geom.toJTS
        jts should not be null
        jts shouldBe a[LineString]
    }

    // ====== toJTS - MULTILINESTRING ======

    test("toJTS should convert MULTILINESTRING to JTS") {
        val boundaries = Array(
          Array(InternalCoord(Seq(0.0, 0.0)), InternalCoord(Seq(1.0, 1.0))),
          Array(InternalCoord(Seq(2.0, 2.0)), InternalCoord(Seq(3.0, 3.0)))
        )
        val holes = Array(Array.empty[Array[InternalCoord]], Array.empty[Array[InternalCoord]])
        val geom = InternalGeometry(
          typeId = GeometryTypeEnum.MULTILINESTRING.id,
          srid = 4326,
          boundaries = boundaries,
          holes = holes
        )
        val jts = geom.toJTS
        jts should not be null
        jts shouldBe a[MultiLineString]
        jts.getNumGeometries shouldBe 2
    }

    // ====== toJTS - POLYGON ======

    test("toJTS should convert POLYGON to JTS") {
        val boundaries = Array(Array(
          InternalCoord(Seq(0.0, 0.0)),
          InternalCoord(Seq(1.0, 0.0)),
          InternalCoord(Seq(1.0, 1.0)),
          InternalCoord(Seq(0.0, 1.0)),
          InternalCoord(Seq(0.0, 0.0))
        ))
        val holes = Array(Array.empty[Array[InternalCoord]])
        val geom = InternalGeometry(
          typeId = GeometryTypeEnum.POLYGON.id,
          srid = 4326,
          boundaries = boundaries,
          holes = holes
        )
        val jts = geom.toJTS
        jts should not be null
        jts shouldBe a[Polygon]
    }

    // ====== toJTS - MULTIPOLYGON ======

    test("toJTS should convert MULTIPOLYGON to JTS") {
        val boundaries = Array(
          Array(
            InternalCoord(Seq(0.0, 0.0)),
            InternalCoord(Seq(1.0, 0.0)),
            InternalCoord(Seq(1.0, 1.0)),
            InternalCoord(Seq(0.0, 0.0))
          ),
          Array(
            InternalCoord(Seq(2.0, 2.0)),
            InternalCoord(Seq(3.0, 2.0)),
            InternalCoord(Seq(3.0, 3.0)),
            InternalCoord(Seq(2.0, 2.0))
          )
        )
        val holes = Array(Array.empty[Array[InternalCoord]], Array.empty[Array[InternalCoord]])
        val geom = InternalGeometry(
          typeId = GeometryTypeEnum.MULTIPOLYGON.id,
          srid = 4326,
          boundaries = boundaries,
          holes = holes
        )
        val jts = geom.toJTS
        jts should not be null
        jts shouldBe a[MultiPolygon]
        jts.getNumGeometries shouldBe 2
    }

    // ====== toJTS - GEOMETRYCOLLECTION (unsupported) ======

    test("toJTS should throw for GEOMETRYCOLLECTION") {
        val boundaries = Array(Array.empty[InternalCoord])
        val holes = Array(Array.empty[Array[InternalCoord]])
        val geom = InternalGeometry(
          typeId = GeometryTypeEnum.GEOMETRYCOLLECTION.id,
          srid = 4326,
          boundaries = boundaries,
          holes = holes
        )
        an[IllegalAccessException] should be thrownBy geom.toJTS
    }

    // ====== Companion Object ======

    test("InternalCoordType should be ArrayType of DoubleType") {
        InternalGeometry.InternalCoordType should not be null
        InternalGeometry.InternalCoordType.toString should include("ArrayType")
    }

}
