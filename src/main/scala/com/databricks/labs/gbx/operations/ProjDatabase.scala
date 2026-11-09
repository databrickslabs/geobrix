package com.databricks.labs.gbx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.internal.Logging
import org.gdal.gdal.gdal
import org.gdal.osr.{SpatialReference, osr}

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Raised when the PROJ context of this thread cannot read PROJ's database (`proj.db`), and pointing PROJ at this JVM's
  * PROJ data again did not help. Every `safeEval` re-throws it: a CRS read without the database loses its authority
  * code (a GeoTIFF's CRS comes back as a `LOCAL_CS`), so the row's answer would be a wrong value, not a data error. */
final class ProjDatabaseException(msg: String, cause: Throwable = null) extends IllegalStateException(msg, cause)

/**
  * Guards the CRS reads whose answer depends on PROJ's database.
  *
  * GDAL gives each thread its own PROJ context, which opens `proj.db` at its first use. A context that cannot read it
  * (the PROJ data directory is gone or unreadable, as for an executor that outlives the Python environment it loaded
  * GDAL from) still answers: a GeoTIFF's CRS is built from its keys alone, with no authority code, and an EPSG code
  * does not resolve. Nothing raises. A result that may come from such a context (no authority code, an unresolved CRS)
  * is checked here: when the database cannot be read, PROJ is pointed at this JVM's PROJ data again
  * ([[GDALManager.recoverProjData]]) and the read runs once more; when it still cannot be read, the read fails with
  * the reason ([[ProjDatabaseException]]).
  *
  * Known limit: the check is a database lookup on this thread's context, which PROJ may answer from its cache, plus
  * a readable `proj.db` on the search path. A context whose database reads fail only now and then is not detected.
  * When the JVM loaded GDAL from a node-local copy of its payload (which a finished run does not remove), a thread
  * whose check passed is not checked again until this JVM changes PROJ's search path ([[invalidate]]). A JVM that
  * loaded GDAL in place checks every suspect answer, as a database removed later under an unchanged path is possible.
  */
object ProjDatabase extends Logging {

    /** The directories PROJ searches: its search path when set, else PROJ_DATA / PROJ_LIB. Empty: PROJ's own default. */
    private def searchDirs(): Seq[String] = {
        val set = Try(Option(osr.GetPROJSearchPaths()).map(_.asScala.map(_.toString).toSeq).getOrElse(Nil)).getOrElse(Nil)
        val dirs = if (set.nonEmpty) set else Seq("PROJ_DATA", "PROJ_LIB").flatMap(k => Option(gdal.GetConfigOption(k)))
        dirs.flatMap(_.split(":")).map(_.trim).filter(_.nonEmpty)
    }

    /** True when this thread's PROJ context reads PROJ's database: a `proj.db` is readable on the search path (when
      * one is set) and EPSG:4326 resolves. */
    def reachable(): Boolean = {
        val dirs = searchDirs()
        val fileOk = dirs.isEmpty || dirs.exists(d => Files.isReadable(Paths.get(d, "proj.db")))
        fileOk && {
            val sr = new SpatialReference()
            try Try(sr.ImportFromEPSG(4326)).toOption.contains(0)
            finally sr.delete()
        }
    }

    // the search-path generation at which this thread's check last passed (-1: never)
    private val generation = new java.util.concurrent.atomic.AtomicLong(0L)
    private val confirmedAt = ThreadLocal.withInitial[java.lang.Long](() => -1L)

    /** A change of PROJ's search path: every thread checks again at its next suspect answer. */
    def invalidate(): Unit = { generation.incrementAndGet(); () }

    /** [[reachable]], once per thread and search path: a check that passed holds until [[invalidate]]. A check that
      * fails is not kept, so the next suspect answer checks again. */
    def reachableCached(): Boolean = reachableCachedWith(GDALManager.loadedFromNodeCopy, () => reachable())

    /** A GDAL loaded in place keeps its PROJ database in the Python environment, which can be removed later: its check
      * is never kept. */
    private[gbx] def reachableCachedWith(fromCopy: Boolean, check: () => Boolean): Boolean =
        if (fromCopy) cachedWith(check) else check()

    private[gbx] def cachedWith(check: () => Boolean): Boolean = {
        val g = generation.get()
        if (confirmedAt.get() == g) true
        else {
            val ok = check()
            if (ok) confirmedAt.set(g)
            ok
        }
    }

    private def unreachable(dirs: Seq[String], cause: Throwable): ProjDatabaseException =
        new ProjDatabaseException(
          s"geobrix: this thread's PROJ context cannot read PROJ's database (proj.db) under ${dirs.mkString(":")}, " +
              "so a CRS read here loses its authority code. Fix: keep the GDAL payload's PROJ data (share/proj beside " +
              "its native directory) readable on every node for the life of the cluster, or restart the cluster.",
          cause
        )

    /** Point PROJ at this JVM's PROJ data again; a [[ProjDatabaseException]] when the database still cannot be read. */
    def recoverOrFail(cause: Throwable = null): Unit = recoverOrFail(() => reachable(), () => GDALManager.recoverProjData(), cause)

    private[gbx] def recoverOrFail(isReachable: () => Boolean, recover: () => Seq[String], cause: Throwable): Unit = {
        val dirs = recover()
        if (!isReachable()) throw unreachable(dirs, cause)
        logWarning(s"PROJ's database could not be read on this thread; PROJ was pointed at ${dirs.mkString(":")} again")
    }

    /** `read()`; when `suspect` holds for its answer (one a context without PROJ's database would give) and the database
      * cannot be read, [[recoverOrFail]] and `read()` once more. A suspect answer with the database readable is the
      * data's own (a CRS with no authority code) when this thread had confirmed the database on the same search path
      * before the read; else it is read once more, and that answer stands. The database may have become readable
      * between the read and the check: another thread's [[recoverOrFail]] re-points every thread's PROJ, and this
      * thread's suspect answer then came from the path that was replaced (it gave a NULL srid with no error). */
    def guarded[T](read: () => T)(suspect: T => Boolean): T = {
        val g = generation.get()
        val confirmed = GDALManager.loadedFromNodeCopy && confirmedAt.get() == g
        guardedWith(read, suspect, () => reachableCached(), () => GDALManager.recoverProjData(),
          () => confirmed && generation.get() == g)
    }

    private[gbx] def guardedWith[T](
        read: () => T,
        suspect: T => Boolean,
        isReachable: () => Boolean,
        recover: () => Seq[String],
        confirmedBefore: () => Boolean = () => false
    ): T = {
        val first = read()
        if (!suspect(first)) first
        else if (isReachable()) { if (confirmedBefore()) first else read() }
        else {
            recoverOrFail(isReachable, recover, null)
            read()
        }
    }

    /** `attempt()`; when it fails with a `refusal` (what a context without PROJ's database raises, as an EPSG code
      * that does not resolve) it runs once more, after [[recoverOrFail]] when the database cannot be read now. With the
      * database readable now it runs again as it is: another thread may have re-pointed PROJ after the attempt. */
    def retried[T](attempt: () => T)(refusal: Throwable => Boolean): T =
        retriedWith(attempt, refusal, () => reachable(), () => GDALManager.recoverProjData())

    private[gbx] def retriedWith[T](attempt: () => T, refusal: Throwable => Boolean, isReachable: () => Boolean,
                                    recover: () => Seq[String]): T =
        try attempt()
        catch {
            case e: Throwable if refusal(e) =>
                if (!isReachable()) recoverOrFail(isReachable, recover, e)
                attempt()
        }

}
