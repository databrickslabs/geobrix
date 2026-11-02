package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.kernels.spec.{GdalSpecEngine, GdalSpecs}
import org.gdal.gdal.{Dataset, TranslateOptions, gdal}
import org.gdal.gdalconst.gdalconstConstants

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

/** A tile's bytes the JVM does not read: a driver that can name other files, or a tile decoded over the read cap. It
  * is a data error, so a raster function gives the error tile with this reason (and its `Fix:`), never a failed task.
  */
final class TileBytesRefused(msg: String) extends RuntimeException(msg)

/** The rule for reading a materialized tile's bytes on the JVM, applied once, at [[RasterDriver.readFromBytes]] (the
  * point every raster function and Java UDF reads a tile's bytes through). It is the lightweight tier's rule
  * (`_gdal_policy.open_bytes`), and the driver classes are the manifest's (`gdal.raster_drivers`, read from the JAR's
  * copy of `functions.json`), so there is no second list here:
  *
  *   - bytes of a driver in `bytes_refused_light` (VRT, PDS, ENVI, ...: formats that can name other files) are refused
  *     before anything is opened, with the light tier's reason and `Fix:`;
  *   - the open lists the `allowed` drivers less that class (a `refused` driver is never registered);
  *   - a tile decoded over the read cap is refused from its header, before any pixel is read;
  *   - a driver outside the self-contained `bytes` class (netCDF, HDF5, HFA, JP2OpenJPEG, GPKG, ...) is translated to
  *     GeoTIFF in `/vsimem`, so every raster function reads the same pixels it would from a GeoTIFF tile.
  *
  * The read cap is the decoded size, in bytes, one tile may have here: the session's [[ReadCapKey]] (the Python
  * manager sets it from the light tier's budget), passed to each task with its `ExpressionConfig`
  * ([[useReadCap]], called by `RST_ExpressionUtil.init`); a caller with no configuration (a Java UDF) gets the classic
  * budget, [[GdalSpecEngine.DecodedBudget]].
  */
object TileBytes {

    /** The session configuration key of the read cap, in bytes. */
    val ReadCapKey = "spark.databricks.labs.gbx.read_cap"

    // (task attempt, cap): a value holds for the task that set it, so a later task on the same thread (a Java UDF,
    // which has no configuration) reads at the default; -1 is a read with no task (driver-side evaluation).
    private val cap = new ThreadLocal[(Long, Long)]

    private def attempt: Long = Option(org.apache.spark.TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)

    /** The read cap of this task's tile reads: its configuration's value, or the default when it has none. */
    def useReadCap(value: Option[Long]): Unit = value match {
        case Some(v) => cap.set((attempt, v))
        case None    => cap.remove()
    }

    def readCap: Long =
        Option(cap.get).filter(_._1 == attempt).map(_._2).getOrElse(GdalSpecEngine.DecodedBudget)

    private val Fix =
        "Fix: store the tile as a GeoTIFF, or pass a VRT as a virtual tile (a path), which the virtual-tile policy judges"

    // Bytes that start like these are a self-contained format; GDAL's identify is not asked about them (light's
    // `_BINARY_MAGICS`).
    private val BinaryMagics: Seq[Array[Byte]] = Seq(
      Array[Byte](0x49, 0x49, 0x2a, 0x00),
      Array[Byte](0x4d, 0x4d, 0x00, 0x2a),
      Array[Byte](0x49, 0x49, 0x2b, 0x00),
      Array[Byte](0x4d, 0x4d, 0x00, 0x2b),
      Array[Byte](0x89.toByte, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a),
      Array[Byte](0xff.toByte, 0xd8.toByte, 0xff.toByte),
      "GIF8".getBytes(StandardCharsets.US_ASCII),
      Array[Byte](0x43, 0x44, 0x46, 0x01),
      Array[Byte](0x43, 0x44, 0x46, 0x02),
      Array[Byte](0x43, 0x44, 0x46, 0x05),
      Array[Byte](0x89.toByte, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a)
    )
    // How the class's drivers begin where GDAL's identify needs more than the bytes (light's `_BYTES_XML` and
    // `_BYTES_AT_START`).
    private val XmlRoots = Seq("<VRTDataset" -> "VRT", "<Dimap_Document" -> "DIMAP", "<Product_Observational" -> "PDS4",
      "<Product_Ancillary" -> "PDS4")
    private val TextStarts = Seq("PDS_VERSION_ID" -> "PDS", "CCSD3ZF" -> "ISIS2", "LBLSIZE=" -> "VICAR",
      "Object = IsisCube" -> "ISIS3", "DatasetHeader Begin" -> "ERS")
    private val XmlSkip = """^(?s)(\s+|<\?.*?\?>|<!--.*?-->)""".r
    private val HeadBytes = 4096

    private def startsWith(bytes: Array[Byte], magic: Array[Byte]): Boolean =
        bytes.length >= magic.length && magic.indices.forall(i => bytes(i) == magic(i))

    private def xmlRoot(head: String): String = {
        var s = head.stripPrefix("ï»¿")
        var next = XmlSkip.findFirstIn(s)
        while (next.exists(_.nonEmpty)) {
            s = s.substring(next.get.length)
            next = XmlSkip.findFirstIn(s)
        }
        s
    }

    /** The driver of the `bytes_refused_light` class that `bytes` (written at `path`) are, else None: GDAL's identify
      * with the class's drivers, then how each of them begins. Nothing is opened. */
    private[gdal] def refusedClassOf(path: String, bytes: Array[Byte]): Option[String] = {
        val names = GdalSpecs.section.bytesRefusedLight
        if (names.isEmpty || BinaryMagics.exists(startsWith(bytes, _))) return None
        val identified = Option(
          gdal.IdentifyDriverEx(path, gdalconstConstants.OF_RASTER, new java.util.Vector[String](names.asJava))
        ).map(_.getShortName).filter(names.contains)
        identified.orElse {
            val head = new String(bytes.take(HeadBytes), StandardCharsets.ISO_8859_1)
            val root = xmlRoot(head)
            val start = head.dropWhile(_.isWhitespace)
            XmlRoots.collectFirst { case (m, d) if root.startsWith(m) && names.contains(d) => d }
                .orElse(TextStarts.collectFirst { case (m, d) if start.startsWith(m) && names.contains(d) => d })
        }
    }

    private[gdal] def refusal(driver: String): TileBytesRefused =
        new TileBytesRefused(
          s"the raster bytes: is a $driver raster, a format that can name other files, which GeoBrix does not open " +
              s"from tile bytes. $Fix"
        )

    /** The drivers an open of tile bytes may use: the manifest's allowed drivers less the refused class. */
    private[gdal] def openDrivers: java.util.Vector[String] = {
        val section = GdalSpecs.section
        val refused = section.bytesRefusedLight.toSet
        new java.util.Vector[String](section.allowedDrivers.filterNot(refused).asJava)
    }

    /** The decoded size of `ds` (every band at the first band's type), from its header. */
    private[gdal] def decodedSize(ds: Dataset): Long = {
        val bands = ds.getRasterCount
        if (bands == 0) 0L
        else {
            val item = math.max(1, gdal.GetDataTypeSize(ds.GetRasterBand(1).getDataType) / 8)
            ds.getRasterXSize.toLong * ds.getRasterYSize * bands * item
        }
    }

    /** The refusal of a tile decoded to `decodedBytes` when that is over the read cap, else None: the one rule and
      * text of the decoded-size gate (the light tier's read gate: an error tile with a Fix), for every reader of
      * tile bytes. The cap is `conf`'s [[ReadCapKey]] when it names one, else this task's ([[readCap]]). */
    def checkDecoded(decodedBytes: Long, conf: Option[ExpressionConfig]): Option[TileBytesRefused] = {
        val limit = conf.flatMap(_.readCap).getOrElse(readCap)
        if (decodedBytes > limit) {
            def size(n: Long) = if (n >= (1L << 20)) s"${n >> 20} MiB" else s"${n >> 10} KiB"
            Some(
              new TileBytesRefused(
                s"the tile is ${size(decodedBytes)} decoded, over this compute's read cap (${size(limit)}). " +
                    "Fix: retile it first (gbx_rst_retile) so each tile is smaller"
              )
            )
        } else None
    }

    /** Refuse `ds` when it is decoded over the read cap: [[checkDecoded]] for its header's decoded size. */
    private[gdal] def checkSize(ds: Dataset): Unit =
        checkDecoded(decodedSize(ds), None).foreach(r => throw r)

    /** True when `bytes` start like a TIFF (classic or BigTIFF, either byte order). */
    def isTiff(bytes: Array[Byte]): Boolean = BinaryMagics.take(4).exists(startsWith(bytes, _))

    /** A tile's metadata once its bytes are read as `ds`: a `driver` outside the self-contained class names the
      * GeoTIFF it was translated to, so the result is written as GeoTIFF too. */
    def metadataAfterRead(metadata: Map[String, String], ds: Dataset): Map[String, String] =
        metadata.get("driver") match {
            case Some(d) if ds != null && !GdalSpecs.section.bytesDrivers.contains(d) &&
                    ds.GetDriver().getShortName == "GTiff" => metadata + ("driver" -> "GTiff")
            case _ => metadata
        }

    /** True when `ds`'s driver reads the bytes alone (the manifest's `bytes` class); any other is translated. */
    private[gdal] def selfContained(ds: Dataset): Boolean =
        GdalSpecs.section.bytesDrivers.contains(ds.GetDriver().getShortName)

    /** `ds` translated to the GeoTIFF `dst` in `/vsimem` (`-of GTiff`, nothing else), opened read-only; the caller
      * releases both (releasing the result unlinks `dst`). */
    private[gdal] def toGTiff(ds: Dataset, dst: String): Dataset = {
        val options = new TranslateOptions(new java.util.Vector[String](Seq("-of", "GTiff").asJava))
        try {
            gdal.ErrorReset()
            val out = gdal.Translate(dst, ds, options)
            if (out == null) {
                gdal.Unlink(dst)
                throw new TileBytesRefused(
                  s"the raster bytes (${ds.GetDriver().getShortName}) could not be converted to GeoTIFF: " +
                      gdal.GetLastErrorMsg()
                )
            }
            out.delete() // closes the written GeoTIFF; it is read back as a plain tile
            val read = gdal.Open(dst, gdalconstConstants.GA_ReadOnly)
            if (read == null) gdal.Unlink(dst)
            read
        } finally options.delete()
    }

}
