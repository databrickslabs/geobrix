package com.databricks.labs.gbx.util

import org.apache.hadoop.fs.{Path => HPath}
import org.apache.spark.internal.Logging
import org.apache.spark.util.SerializableConfiguration

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, LinkOption, NoSuchFileException, Path, Paths}
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import scala.util.control.NonFatal

/** Per-node cache of remote files for GDAL/OGR. Each copy is a refcounted generation in its own
  * directory (`<rootPath>/h<hash>_<gen>/<name>`), reused only while the remote still matches the
  * stat signature it was copied at, and deleted recursively at its last release.
  *
  * Every reader-count and liveness change for one remote runs inside `current.compute(remote, ...)`,
  * so they are serialized; the copy and all file deletion run outside it. A generation's directory
  * is never shared, so retiring one can never touch a newer copy of the same remote.
  */
object NodeFilePathUtil extends Logging {

    private val uuid = this.hashCode().toHexString

    /** Base directory for this JVM's cached files (/tmp/gdal_local_files/&lt;uuid&gt;).
      *
      * Created eagerly: several callers (RST_MapAlgebra, RST_NDVI, NDVI, ClipToGeom,
      * GDAL_RowWriter) write GDAL outputs directly under this path, and GDAL's
      * gdal_translate / gdal_calc silently return a null Dataset when the target
      * directory does not exist, so the directory must exist before any write.
      */
    val rootPath: Path = {
        val p = Paths.get(s"/tmp/gdal_local_files/$uuid")
        scala.util.Try(Files.createDirectories(p))
        p
    }

    private val maxRetries = 3
    private val tinyBackoffMs = 2L
    /** `copyCount` keeps tallies for this many most recently copied remotes. */
    private val maxCopyCounts = 4096

    /** Stat signature of a remote: a file's length and mtime; for a directory dataset (`.gdb`), the
      * sum of lengths and the max mtime over its flat listing. */
    private final case class Sig(len: Long, mtime: Long)

    /** One copy (generation) of a remote. `readers` and `dead` change only inside
      * `current.compute(remote, ...)`. `ready` completes once the copy is in place, or fails. */
    private final class Entry(
        val remote: String,
        val gen: Long,
        val dir: Path,
        val local: Path,
        val sig: Option[Sig],
        val ready: CompletableFuture[Unit]
    ) {
        @volatile var readers: Int = 0
        @volatile var dead: Boolean = false
        val localKey: String = local.toString
    }

    private val generation = new AtomicLong(0L)
    /** remote -> the entry new readers join. */
    private val current = new ConcurrentHashMap[String, Entry]()
    /** local path -> the entry that owns it, a superseded one with readers left included. */
    private val byLocal = new ConcurrentHashMap[String, Entry]()
    /** remote -> copies made; access-ordered and bounded, guarded by its own monitor. */
    private val copyCounts = new java.util.LinkedHashMap[String, Int](64, 0.75f, true) {
        override def removeEldestEntry(eldest: java.util.Map.Entry[String, Int]): Boolean = size() > maxCopyCounts
    }

    /** Last path component (file or dir name) of remote; trailing '/' are ignored, so a directory
      * given as `x.gdb/` caches as `<gen dir>/x.gdb` like `x.gdb`. */
    private def base(remote: String): String = remote.split("://").last.split('/').last

    /** First 16 hex digits (64 bits) of the SHA-256 of `s`. */
    private def hex16(s: String): String =
        MessageDigest
            .getInstance("SHA-256")
            .digest(s.getBytes(StandardCharsets.UTF_8))
            .take(8)
            .map(b => f"${b & 0xff}%02x")
            .mkString

    /** The remote's current signature; `None` when it cannot be read, which never matches. */
    private def signature(remote: String, hconf: SerializableConfiguration): Option[Sig] =
        try {
            val p = new HPath(HadoopUtils.cleanPath(remote))
            val fs = p.getFileSystem(hconf.value)
            val st = fs.getFileStatus(p)
            if (st.isDirectory) {
                val kids = fs.listStatus(p)
                Some(Sig(kids.map(_.getLen).sum, if (kids.isEmpty) 0L else kids.map(_.getModificationTime).max))
            } else Some(Sig(st.getLen, st.getModificationTime))
        } catch { case NonFatal(_) => None }

    /** Ensures a copy of `remotePath` that matches the remote's current signature and holds one reader
      * on it; returns the copy's local path and its reader count. The first reader of a generation
      * copies, concurrent readers of it wait. A failed copy is deleted before anyone sees the failure,
      * and retried up to `maxRetries` times before this throws. */
    def readLock(remotePath: String, hconf: SerializableConfiguration): (String, Int) = {
        val prefix = s"h${hex16(remotePath)}_"
        val name = base(remotePath)
        var attempts = 0
        var lastErr: Throwable = null
        var result: (String, Int) = null
        while (result == null && attempts <= maxRetries) {
            attempts += 1
            val sig = signature(remotePath, hconf)
            var created = false
            var readers = 0
            val e = current.compute(
              remotePath,
              (_, cur) =>
                  if (cur == null || cur.dead || sig.isEmpty || cur.sig != sig) {
                      // Absent, dead or stale: start a new generation; this reader copies. A superseded
                      // live entry stays reachable through byLocal until its own readers release it.
                      val gen = generation.incrementAndGet()
                      val dir = rootPath.resolve(s"$prefix$gen")
                      val fresh = new Entry(remotePath, gen, dir, dir.resolve(name), sig, new CompletableFuture[Unit]())
                      fresh.readers = 1
                      created = true
                      readers = 1
                      fresh
                  } else {
                      cur.readers += 1
                      readers = cur.readers
                      cur
                  }
            )
            if (created) {
                try {
                    copy(e, hconf)
                    result = (e.localKey, readers)
                } catch {
                    case t: Throwable =>
                        // Retire the entry even on a fatal error, or its waiters would block forever.
                        abandon(e, t)
                        if (!NonFatal(t)) throw t
                        lastErr = t
                }
            } else {
                try {
                    e.ready.join() // throws if the copier failed
                    result = (e.localKey, readers)
                } catch {
                    case NonFatal(err) =>
                        lastErr = Option(err.getCause).getOrElse(err)
                        release(e)
                }
            }
            if (result == null) {
                Thread.`yield`()
                Thread.sleep(tinyBackoffMs)
            }
        }
        if (result == null) {
            throw new RuntimeException(s"Failed to materialize $remotePath after $maxRetries retries.", lastErr)
        }
        result
    }

    /** The copier's work, outside `compute`: copy into the fresh generation dir, publish, complete. */
    private def copy(e: Entry, hconf: SerializableConfiguration): Unit = {
        Files.createDirectories(e.dir)
        HadoopUtils.copyToPath(e.remote, e.localKey, hconf) // the file plus same-stem sidecars, or the directory
        if (!Files.exists(e.local)) {
            throw new java.io.FileNotFoundException(s"Copy of ${e.remote} produced nothing at ${e.local}")
        }
        // Published before `ready` completes, so a waiter's releaseLocal always finds its entry.
        byLocal.put(e.localKey, e)
        countCopy(e.remote)
        e.ready.complete(())
    }

    /** Copier failure: retire the entry and drop the copier's reader, delete the partial dir, then
      * fail the waiters (each releases its own reader and retries). */
    private def abandon(e: Entry, err: Throwable): Unit = {
        current.compute(
          e.remote,
          (_, cur) => {
              e.dead = true
              if (e.readers > 0) e.readers -= 1
              if (cur eq e) null else cur
          }
        )
        byLocal.remove(e.localKey, e)
        deleteRecursively(e.dir)
        e.ready.completeExceptionally(err)
    }

    /** Drops one reader of `e`; at zero the entry dies (no reader can join it any more) and its dir
      * is deleted. Returns the readers left; a release beyond the last reader is a no-op. */
    private def release(e: Entry): Int = {
        var left = -1
        current.compute(
          e.remote,
          (_, cur) => {
              if (e.readers > 0) {
                  e.readers -= 1
                  left = e.readers
                  if (left == 0) e.dead = true
              }
              if (left == 0 && (cur eq e)) null else cur
          }
        )
        if (left == 0) {
            byLocal.remove(e.localKey, e)
            deleteRecursively(e.dir)
        }
        math.max(left, 0)
    }

    /** Releases one reader of the CURRENT copy of `remotePath`; returns its readers left. 0 when there
      * is no current copy, or it is still being made (so no caller can be holding it yet). */
    def releaseByRemote(remotePath: String): Int = {
        val e = current.get(remotePath)
        if (e == null || !e.ready.isDone || e.ready.isCompletedExceptionally) 0 else release(e)
    }

    /** Releases one reader of the copy that owns `localPath`, a superseded one included; returns its
      * readers left. An unknown path (sidecar, user-local file, vsimem) is a no-op returning 0. */
    def releaseByLocal(localPath: String): Int = {
        val e = byLocal.get(localPath)
        if (e == null) 0 else release(e)
    }

    /** Test-visible: copies made for this remote (kept for the most recently copied remotes). */
    private[gbx] def copyCount(remotePath: String): Int =
        copyCounts.synchronized(copyCounts.getOrDefault(remotePath, 0))

    private def countCopy(remote: String): Unit =
        copyCounts.synchronized(copyCounts.put(remote, copyCounts.getOrDefault(remote, 0) + 1))

    /** Deletes `p` and everything under it, children first, without following links; never throws. */
    private[util] def deleteRecursively(p: Path): Unit =
        try {
            if (Files.exists(p, LinkOption.NOFOLLOW_LINKS)) {
                val walk = Files.walk(p)
                try {
                    walk.sorted(java.util.Comparator.reverseOrder[Path]()).forEach { q =>
                        try Files.deleteIfExists(q)
                        catch { case NonFatal(err) => logWarning(s"Could not delete node-file cache path $q", err) }
                    }
                } finally walk.close()
            }
        } catch {
            case _: NoSuchFileException => ()
            case NonFatal(err)          => logWarning(s"Could not delete node-file cache dir $p", err)
        }

}
