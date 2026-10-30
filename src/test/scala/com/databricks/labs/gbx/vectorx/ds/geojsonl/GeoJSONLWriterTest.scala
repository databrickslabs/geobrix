package com.databricks.labs.gbx.vectorx.ds.geojsonl

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/**
  * Tests for the heavyweight multi-file `geojsonl_ogr` vector writer.
  *
  * Like the lightweight `geojsonl_gbx`, it writes a DIRECTORY of newline-delimited GeoJSONL
  * shards — one per partition, NO driver merge — with an optional `maxRecordsPerFile` that splits
  * a partition into several shards. Round-trips with the `geojson_ogr` (`multi=true`) directory
  * reader.
  */
class GeoJSONLWriterTest extends PlanTest with SilentSparkSession {

    private val gf = new GeometryFactory()

    /** Convenience: WKB bytes for a single lon/lat Point. */
    private def wkb(lon: Double, lat: Double): Array[Byte] =
        JTS.toWKB(gf.createPoint(new Coordinate(lon, lat)))

    private val tmpDir: String = Files.createTempDirectory("gbx_geojsonl_").toString

    /** A DataFrame in the *_ogr reader's writer shape: geom_0 (WKB) + srid + proj + attrs. */
    private def wkbDf(n: Int) = {
        val rows = (0 until n).map { i =>
            (s"$i", i, JTS.toWKB(gf.createPoint(new Coordinate(i.toDouble / 10.0, 40.0))), "4326", "")
        }
        spark.createDataFrame(rows)
            .toDF("name", "pop", "geom_0", "geom_0_srid", "geom_0_srid_proj")
    }

    private def shards(dir: java.io.File): Array[String] =
        Option(dir.list()).getOrElse(Array.empty)
            .filter(n => n.endsWith(".geojsonl") || n.endsWith(".geojsons"))

    test("geojsonl_ogr writer emits exactly one shard per partition and round-trips") {
        spark.sparkContext.setLogLevel("ERROR")
        val out = Files.createTempDirectory("gbx_geojsonl_out_").toFile
        out.delete() // let the writer create it
        val df = wkbDf(6).repartition(3)
        val nparts = df.rdd.getNumPartitions

        df.write.format("geojsonl_ogr").mode("overwrite").save(out.getAbsolutePath)

        out.isDirectory shouldBe true
        val sh = shards(out)
        sh.length shouldEqual nparts

        val back = spark.read.format("geojson_ogr").option("multi", "true").load(out.getAbsolutePath)
        back.count() shouldEqual 6L
        // The reader type-infers attribute columns, so 'name' may come back as a numeric type;
        // compare by string value rather than assuming String.
        val names = back.collect().map(_.get(back.schema.fieldIndex("name")).toString).toSet
        names shouldEqual (0 until 6).map(_.toString).toSet
        // geometry column round-trips as a parseable point
        val gcol = back.schema.fields.map(_.name).find(_.endsWith("_srid")).get.dropRight("_srid".length)
        val wkbs = back.select(gcol).collect().map(_.getAs[Array[Byte]](0))
        wkbs.foreach(b => JTS.fromWKB(b).getGeometryType shouldEqual "Point")
    }

    test("maxRecordsPerFile splits one partition into ceil(M/k) shards") {
        spark.sparkContext.setLogLevel("ERROR")
        val out = Files.createTempDirectory("gbx_geojsonl_split_").toFile
        out.delete()
        val (m, k) = (10, 3) // ceil(10/3) == 4
        wkbDf(m).repartition(1).write.format("geojsonl_ogr").mode("overwrite")
            .option("maxRecordsPerFile", k.toString).save(out.getAbsolutePath)

        val expected = (m + k - 1) / k // ceil(m/k) = ceil(10/3) = 4
        shards(out).length shouldEqual expected

        val back = spark.read.format("geojson_ogr").option("multi", "true").load(out.getAbsolutePath)
        back.count() shouldEqual m.toLong
    }

    test("overwrite clears prior shards") {
        spark.sparkContext.setLogLevel("ERROR")
        val out = Files.createTempDirectory("gbx_geojsonl_ow_").toFile
        out.delete()
        wkbDf(8).repartition(4).write.format("geojsonl_ogr").mode("overwrite").save(out.getAbsolutePath)
        val first = shards(out).toSet
        wkbDf(2).repartition(1).write.format("geojsonl_ogr").mode("overwrite").save(out.getAbsolutePath)
        val second = shards(out).toSet
        assert(first.intersect(second).isEmpty, "overwrite left stale shards behind")
        val back = spark.read.format("geojson_ogr").option("multi", "true").load(out.getAbsolutePath)
        back.count() shouldEqual 2L
    }

    test("append mode is rejected") {
        spark.sparkContext.setLogLevel("ERROR")
        val out = Files.createTempDirectory("gbx_geojsonl_append_").toFile
        out.delete()
        wkbDf(4).repartition(2).write.format("geojsonl_ogr").mode("overwrite").save(out.getAbsolutePath)
        val ex = intercept[Exception] {
            wkbDf(4).write.format("geojsonl_ogr").mode("append").save(out.getAbsolutePath)
        }
        // unwrap to the root message
        val msg = Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null)
            .map(_.getMessage).filter(_ != null).mkString(" | ")
        assert(msg.toLowerCase.contains("append"), s"expected an 'append' rejection, got: $msg")
    }

    test("geomCol/sridCol options write a non-convention frame") {
        val rows = Seq(
            ("a", wkb(-73.9, 40.7), "4326", ""),
            ("b", wkb(-0.1, 51.5), "4326", "")
        )
        val df = spark.createDataFrame(rows).toDF("name", "the_geom", "epsg", "p4")
        val out = s"$tmpDir/renamed"
        df.write
            .format("geojsonl_ogr")
            .mode("overwrite")
            .option("geomCol", "the_geom")
            .option("sridCol", "epsg")
            .option("projCol", "p4")
            .save(out)
        val back = spark.read.format("geojson_ogr").option("multi", "true").load(out)
        back.count() shouldEqual 2
    }

    test("resolveRoles honors overrides and requires srid") {
        val sch = StructType(Seq(
            StructField("the_geom", BinaryType), StructField("epsg", StringType),
            StructField("p4", StringType), StructField("v", LongType)))
        val r = GeoJSONL_DataSource.resolveRoles(
            sch, Some("the_geom"), Some("epsg"), Some("p4"))
        r.geomCol shouldEqual "the_geom"
        r.sridCol shouldEqual "epsg"
        // geomCol given but no srid resolvable -> error
        val bad = StructType(Seq(StructField("the_geom", BinaryType)))
        an[IllegalArgumentException] should be thrownBy
            GeoJSONL_DataSource.resolveRoles(bad, Some("the_geom"), None, None)
    }

    test("the writer's PROJ4 column goes through the canonical resolver: a grid named by path is refused") {
        spark.sparkContext.setLogLevel("ERROR")
        val out = Files.createTempDirectory("gbx_geojsonl_refused_").toFile
        out.delete()
        // The PROJ4 string names a grid by path: OGR's ImportFromProj4 would open it; the resolver refuses it with a Fix.
        val proj = "+proj=longlat +datum=WGS84 +nadgrids=/etc/hostname +no_defs"
        val df = spark.createDataFrame(Seq(("a", 1, JTS.toWKB(gf.createPoint(new Coordinate(0.1, 40.0))), "0", proj)))
            .toDF("name", "pop", "geom_0", "geom_0_srid", "geom_0_srid_proj")
        val e = intercept[Exception] {
            df.write.format("geojsonl_ogr").mode("overwrite").save(out.getAbsolutePath)
        }
        val messages = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).map(_.getMessage).toSeq
        assert(messages.exists(m => m != null && m.contains("names a grid or init file by path") && m.contains("Fix:")),
          messages.mkString(" | "))
        assert(!out.exists() || shards(out).isEmpty, "a refused CRS wrote a shard")
    }

    test("the layer CRS is the srid's, else the PROJ4 column's; a non-integer srid that is no CRS is loud") {
        spark.sparkContext.setLogLevel("ERROR")
        // the writer reprojects to WGS84 (GeoJSONSeq is RFC 7946): a point 10 degrees east in EPSG:3857 comes back at
        // lon 10 only when the layer carried the CRS
        val merc = "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +no_defs"
        val x = 1113194.9079327357
        for ((srid, proj) <- Seq(("3857", ""), ("0", merc), ("999999", merc))) {
            val out = Files.createTempDirectory("gbx_geojsonl_crs_").toFile
            out.delete()
            spark.createDataFrame(Seq(("a", 1, JTS.toWKB(gf.createPoint(new Coordinate(x, 0.0))), srid, proj)))
                .toDF("name", "pop", "geom_0", "geom_0_srid", "geom_0_srid_proj")
                .write.format("geojsonl_ogr").mode("overwrite").save(out.getAbsolutePath)
            val back = spark.read.format("geojson_ogr").option("multi", "true").load(out.getAbsolutePath)
            val gcol = back.schema.fields.map(_.name).find(_.endsWith("_srid")).get.dropRight("_srid".length)
            val lon = JTS.fromWKB(back.select(gcol).first().getAs[Array[Byte]](0)).getCoordinate.x
            assert(math.abs(lon - 10.0) < 1e-6, s"srid=$srid proj=$proj: lon $lon (the layer lost its CRS)")
        }
        val out = Files.createTempDirectory("gbx_geojsonl_badsrid_").toFile
        out.delete()
        val e = intercept[Exception] {
            spark.createDataFrame(Seq(("a", 1, JTS.toWKB(gf.createPoint(new Coordinate(0.1, 40.0))), "NOT:ACRS", "")))
                .toDF("name", "pop", "geom_0", "geom_0_srid", "geom_0_srid_proj")
                .write.format("geojsonl_ogr").mode("overwrite").save(out.getAbsolutePath)
        }
        val messages = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).map(_.getMessage).toSeq
        assert(messages.exists(m => m != null && m.contains("srid column") && m.contains("Fix:")), messages.mkString(" | "))
    }

    test("an unparseable PROJ4 column is loud: the write fails with the column and a Fix") {
        spark.sparkContext.setLogLevel("ERROR")
        val out = Files.createTempDirectory("gbx_geojsonl_bad_").toFile
        out.delete()
        val df = spark.createDataFrame(Seq(("a", 1, JTS.toWKB(gf.createPoint(new Coordinate(0.1, 40.0))), "0",
            "this is not a coordinate system")))
            .toDF("name", "pop", "geom_0", "geom_0_srid", "geom_0_srid_proj")
        val e = intercept[Exception] {
            df.write.format("geojsonl_ogr").mode("overwrite").save(out.getAbsolutePath)
        }
        val messages = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).map(_.getMessage).toSeq
        assert(messages.exists(m => m != null && m.contains("PROJ4 column") && m.contains("Fix:")), messages.mkString(" | "))
    }
}
