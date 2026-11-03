package com.databricks.labs.gbx.pmtiles

import com.databricks.labs.gbx.vectorx.jts.JTS
import com.databricks.labs.gbx.vectorx.mvt.{MvtDecoder, MvtWriter}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

import java.nio.{ByteBuffer, ByteOrder}

/**
  * End-to-end test for the `gbx_pmtiles_agg` UDAF.
  *
  * Validates that the aggregator produces a valid PMTile v3 blob with the expected
  * header magic, version byte, addressed-tiles count, and that tile bytes round-trip
  * through the tile-data section. Includes regression tests for the vector-merge
  * fix (multiple MVT blobs at the same (z,x,y) must merge into one multi-feature tile)
  * and the raster duplicate-tileid fix (two raster blobs at the same (z,x,y) must
  * produce exactly one directory entry — not a malformed archive).
  */
class PMTiles_AggTest extends PlanTest with SilentSparkSession {

    private val gf = new GeometryFactory()

    /** Build a tile-local polygon WKB in the [0, 4096] pixel space. */
    private def polygonWkb(x0: Int, y0: Int): Array[Byte] = {
        val ring = gf.createLinearRing(Array(
            new Coordinate(x0, y0),
            new Coordinate(x0 + 100, y0),
            new Coordinate(x0 + 100, y0 + 100),
            new Coordinate(x0, y0 + 100),
            new Coordinate(x0, y0)
        ))
        JTS.toWKB(gf.createPolygon(ring))
    }

    /** Encode one tile-local polygon as a real single-feature MVT blob. */
    private def realMvtBlob(id: Int, x0: Int, y0: Int, layer: String = "bldg"): Array[Byte] =
        MvtWriter.encode(layer, MvtWriter.DefaultExtent, Seq((polygonWkb(x0, y0), Map[String, Any]("id" -> id))))

    test("pmtiles_agg encodes a 9-tile pyramid into a valid PMTile blob") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val tiles = (for {
            x <- 0 until 3
            y <- 0 until 3
        } yield (2, x, y, s"tile_${x}_${y}".getBytes("UTF-8"))).toSeq

        val df = spark.createDataFrame(tiles).toDF("z", "x", "y", "bytes")
        val out = df
            .agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y"), lit("{}")).as("pmt"))
            .collect()
            .head
            .getAs[Array[Byte]]("pmt")

        assert(out != null, "pmtiles_agg result should not be null")
        assert(out.length > 127, s"PMTile must be at least header+data; got ${out.length}")
        // Magic + version check.
        assert(out(0) == 'P'.toByte && out(7) == 0x03.toByte, "magic+version must match PMTiles v3")
        // addressed_tiles_count at offset 72 = 9.
        val addressed = ByteBuffer.wrap(out, 72, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
        assert(addressed == 9L, s"expected 9 addressed tiles; got $addressed")
    }

    test("pmtiles_agg works with 4-arg signature (no metadata)") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val df = spark.createDataFrame(Seq((1, 0, 0, "AAA".getBytes("UTF-8"))))
            .toDF("z", "x", "y", "bytes")
        val out = df
            .agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect()
            .head
            .getAs[Array[Byte]]("pmt")
        assert(out != null && out(0) == 'P'.toByte)
    }

    test("pmtiles_agg auto-detects tile_type from first non-null tile bytes (PNG / JPEG / MVT)") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val pngBytes = Array[Byte](0x89.toByte, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D)
        val jpegBytes = Array[Byte](0xFF.toByte, 0xD8.toByte, 0xFF.toByte, 0xE0.toByte, 0x00, 0x10)
        val mvtBytes = "plain_text_tile".getBytes("UTF-8") // no image magic → defaults to MVT

        // Tile type byte is at offset 99 in the v3 header.
        val cases = Seq(
            (pngBytes, PMTilesV3Encoder.TILE_TYPE_PNG, "PNG"),
            (jpegBytes, PMTilesV3Encoder.TILE_TYPE_JPEG, "JPEG"),
            (mvtBytes, PMTilesV3Encoder.TILE_TYPE_MVT, "MVT")
        )
        cases.foreach { case (bytes, expectedType, label) =>
            val df = spark.createDataFrame(Seq((1, 0, 0, bytes))).toDF("z", "x", "y", "bytes")
            val out = df.agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
                .collect().head.getAs[Array[Byte]]("pmt")
            assert(out(99) == expectedType, s"expected $label tile_type ($expectedType); got ${out(99)}")
        }
    }

    test("pmtiles_agg returns NULL for a group with no tile payload (as the light tier does)") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val df = spark.createDataFrame(Seq.empty[(Int, Int, Int, Array[Byte])])
            .toDF("z", "x", "y", "bytes")
        val out = df.agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")
        assert(out == null)
    }

    test("pmtiles_agg survives a multi-partition shuffle merge") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        // Generate enough tiles across multiple partitions to force a shuffle.
        val tiles = (0 until 64).map(i => (3, i % 8, i / 8, s"tile_$i".getBytes("UTF-8")))
        val df = spark.createDataFrame(tiles).toDF("z", "x", "y", "bytes").repartition(4)
        val out = df.agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")
        val addressed = ByteBuffer.wrap(out, 72, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
        assert(addressed == 64L, s"expected 64 addressed tiles after merge; got $addressed")
    }

    // ── Vector-merge tests ──────────────────────────────────────────────────────

    test("pmtiles_agg merges two MVT blobs for the same (z,x,y) into one tile with 2 features") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val blobA = realMvtBlob(id = 1, x0 = 100, y0 = 100)
        val blobB = realMvtBlob(id = 2, x0 = 300, y0 = 300)
        val df = spark.createDataFrame(Seq(
            (3, 2, 4, blobA),
            (3, 2, 4, blobB)
        )).toDF("z", "x", "y", "bytes")

        val archive = df
            .agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")

        val tileBytes = PMTilesTestHelper.readTile(archive, z = 3, x = 2, y = 4)
        assert(tileBytes.nonEmpty, "merged tile must be present in archive")
        val features = MvtDecoder.decode(tileBytes)
        val ids = features.map(_._3.get("id").map(_.toString.toInt).getOrElse(-1)).toSet
        assert(ids == Set(1, 2), s"expected both feature ids {1,2}; got $ids")
    }

    test("pmtiles_agg preserves POLYGON geometry type in merged MVT tile") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val blobA = realMvtBlob(id = 1, x0 = 100, y0 = 100)
        val blobB = realMvtBlob(id = 2, x0 = 300, y0 = 300)
        val df = spark.createDataFrame(Seq((3, 2, 4, blobA), (3, 2, 4, blobB)))
            .toDF("z", "x", "y", "bytes")
        val archive = df
            .agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")
        val tileBytes = PMTilesTestHelper.readTile(archive, z = 3, x = 2, y = 4)
        val features = MvtDecoder.decode(tileBytes)
        assert(features.nonEmpty, "merged tile must decode to features")
        features.foreach { case (_, wkb, _) =>
            val geom = JTS.fromWKB(wkb)
            assert(geom != null && !geom.isEmpty, "decoded geometry is null/empty")
            assert(
                geom.getGeometryType == "Polygon" || geom.getGeometryType == "MultiPolygon",
                s"expected Polygon; got ${geom.getGeometryType}"
            )
        }
    }

    test("pmtiles_agg raster first-wins unchanged after vector-merge change") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val pngA = Array[Byte](0x89.toByte, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x01, 0x00)
        val pngB = Array[Byte](0x89.toByte, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x02, 0x00)
        val df = spark.createDataFrame(Seq((1, 0, 0, pngA), (1, 0, 0, pngB)))
            .toDF("z", "x", "y", "bytes")
        val archive = df
            .agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")
        val tileBytes = PMTilesTestHelper.readTile(archive, z = 1, x = 0, y = 0)
        assert(tileBytes.sameElements(pngA), "raster first-wins violated")
    }

    test("pmtiles_agg merged MVT tile preserves tile-local coordinates") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        // Two single-feature blobs at known tile-local positions.
        // polygonWkb(100, 100) → polygon at corners (100,100)-(200,200) in [0,4096] space.
        // polygonWkb(300, 300) → polygon at corners (300,300)-(400,400) in [0,4096] space.
        val blobA = realMvtBlob(id = 1, x0 = 100, y0 = 100)
        val blobB = realMvtBlob(id = 2, x0 = 300, y0 = 300)
        val df = spark.createDataFrame(Seq(
            (3, 2, 4, blobA),
            (3, 2, 4, blobB)
        )).toDF("z", "x", "y", "bytes")

        val archive = df
            .agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")

        val tileBytes = PMTilesTestHelper.readTile(archive, z = 3, x = 2, y = 4)
        assert(tileBytes.nonEmpty, "merged tile must be present in archive")

        val features = MvtDecoder.decode(tileBytes)
        assert(features.size == 2, s"expected 2 features after merge; got ${features.size}")

        // Collect coordinates from all decoded features.
        val allCoords = features.flatMap { case (_, wkb, _) =>
            val geom = JTS.fromWKB(wkb)
            assert(geom != null && !geom.isEmpty, "decoded geometry null/empty")
            geom.getCoordinates.toSeq
        }

        // PRIMARY ASSERTION: all coordinates must be in [0, 4096] tile-local space.
        // If a double world-transform bug exists, coordinates would be ~±2e7 (EPSG:3857
        // world metres), which trivially fails this range check.
        allCoords.foreach { c =>
            assert(c.x >= 0 && c.x <= 4096,
                s"decoded x=${c.x} is out of tile-local [0,4096] range — double world-transform bug")
            assert(c.y >= 0 && c.y <= 4096,
                s"decoded y=${c.y} is out of tile-local [0,4096] range — double world-transform bug")
        }

        // SECONDARY ASSERTION: the two features are spatially distinct.
        // Each polygon occupies a 100×100 region; with extent=4096, their bounding boxes
        // must not overlap if both are faithfully encoded. The max-x of all decoded
        // coordinates must be at least 200 units beyond the min-x (polygon 2 is 300+
        // units away from polygon 1 in the input; OGR quantization won't collapse that gap).
        val xValues = allCoords.map(_.x)
        val xMin = xValues.min
        val xMax = xValues.max
        assert(xMax - xMin > 200.0,
            s"decoded coordinate range (xMax=$xMax - xMin=$xMin = ${xMax - xMin}) is too narrow; " +
            s"the two polygons should be spatially distinct after merge")
    }

    // ── Raster duplicate-tileid regression (malformed-archive fix) ──────────────

    test("pmtiles_agg raster duplicate tileid produces exactly one directory entry") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val pngA = Array[Byte](0x89.toByte, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x01, 0x00)
        val pngB = Array[Byte](0x89.toByte, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x02, 0x00)
        // Two DIFFERENT raster blobs at the same (z,x,y) — the old code would produce
        // two directory entries (malformed), the new code must produce exactly one.
        val df = spark.createDataFrame(Seq((2, 1, 1, pngA), (2, 1, 1, pngB)))
            .toDF("z", "x", "y", "bytes")
        val archive = df
            .agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")

        // addressed_tiles_count at offset 72 — must be 1, not 2.
        val addressed = ByteBuffer.wrap(archive, 72, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
        assert(addressed == 1L,
            s"duplicate raster tileid must produce addressed_tiles_count=1; got $addressed")

        // tile_entries_count at offset 80 — also must be 1.
        val entries = ByteBuffer.wrap(archive, 80, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
        assert(entries == 1L,
            s"duplicate raster tileid must produce tile_entries_count=1; got $entries")

        // The stored tile must be first-wins (pngA).
        val tileBytes = PMTilesTestHelper.readTile(archive, z = 2, x = 1, y = 1)
        assert(tileBytes.sameElements(pngA), "raster first-wins violated in dedup regression")
    }
}
