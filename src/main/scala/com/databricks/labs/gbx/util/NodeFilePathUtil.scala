package com.databricks.labs.gbx.util

import org.apache.hadoop.fs.{Path => HPath}
import org.apache.spark.internal.Logging
import org.apache.spark.util.SerializableConfiguration

import java.io.{FileNotFoundException, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileAlreadyExistsException, FileVisitResult, Files, NoSuchFileException, Path, Paths, SimpleFileVisitor}
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ScheduledExecutorService, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}
import scala.util.control.NonFatal

/** Per-node cache of remote files for GDAL/OGR. Each copy is a refcounted generation in its own
  * directory (`<rootPath>/h<hash>_<gen>/<name>`), reused only while the remote still matches the
  * stat signature it was copied at.
  *
  * At its last release a current copy is kept idle for the retention window (`setRetentionMs`, 0 for
  * none), so the next reader of an unchanged remote reuses it instead of copying the file again. An idle
  * copy is deleted when the window passes with no reader, or at once when a reader finds the remote
  * changed. A copy that is no longer current (the remote changed while it was held) is deleted at its
  * last release.
  *
  * Every reader-count and liveness change for one remote runs inside `current.compute(remote, ...)`,
  * so they are serialized; the copy and all file deletion run outside it, and a generation's directory
  * is deleted only by the one thread that marked it dead. A generation's directory is never shared, so
  * retiring one can never touch a newer copy of the same remote.
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
    /** Generation dir names already on disk that one `readLock` steps past without counting a retry. */
    private val maxTakenDirs = 64

    /** How long an idle copy is kept by default: `spark.databricks.labs.gbx.nodecache.retention.ms`. */
    private[gbx] val DefaultRetentionMs: Long = 60000L
    @volatile private var retention: Long = DefaultRetentionMs

    /** Sets how long a copy is kept idle after its last release; 0 deletes it at once. Applies to copies
      * released from now on. */
    private[gbx] def setRetentionMs(ms: Long): Unit = retention = math.max(0L, ms)

    private[gbx] def retentionMs: Long = retention

    /** Stat signature of a remote: a file's length and mtime; for a directory dataset (`.gdb`), the
      * sum of lengths and the max mtime over its flat listing. */
    private final case class Sig(len: Long, mtime: Long)

    /** One copy (generation) of a remote. `readers`, `dead`, `idleUntil` and `evictionDue` change only
      * inside `current.compute(remote, ...)`. `ready` completes once the copy is in place, or fails. */
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
        /** While idle (no readers and not dead): the `System.nanoTime` at which the copy expires. */
        @volatile var idleUntil: Long = 0L
        /** An expiry check is scheduled for this entry; there is at most one at a time. */
        @volatile var evictionDue: Boolean = false
        val localKey: String = local.toString
        /** The copy is complete in `dir`; only then is `dir` this entry's to delete. */
        def copied: Boolean = ready.isDone && !ready.isCompletedExceptionally
    }

    /** A copy found its generation dir name already on disk. The dir is not this JVM's: it is left alone. */
    private final class TakenDirException(dir: Path) extends FileAlreadyExistsException(dir.toString)

    private val generation = new AtomicLong(0L)
    /** remote -> the entry new readers join, an idle one included. */
    private val current = new ConcurrentHashMap[String, Entry]()
    /** local path -> the entry that owns it, a superseded one with readers left included. */
    private val byLocal = new ConcurrentHashMap[String, Entry]()
    /** remote -> copies made; access-ordered and bounded, guarded by its own monitor. */
    private val copyCounts = new java.util.LinkedHashMap[String, Int](64, 0.75f, true) {
        override def removeEldestEntry(eldest: java.util.Map.Entry[String, Int]): Boolean = size() > maxCopyCounts
    }

    /** One daemon thread that runs the expiry checks of idle copies; started by the first idle copy. */
    private lazy val evictor: ScheduledExecutorService =
        new ScheduledThreadPoolExecutor(
          1,
          new ThreadFactory {
              override def newThread(r: Runnable): Thread = {
                  val t = new Thread(r, "gbx-node-file-cache-evictor")
                  t.setDaemon(true)
                  t
              }
          }
        )

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
      * copies, concurrent readers of it wait; an idle copy that still matches is taken back as is, and
      * one that no longer matches is deleted. A failed copy is deleted before anyone sees the failure,
      * and retried up to `maxRetries` times before this throws. */
    def readLock(remotePath: String, hconf: SerializableConfiguration): (String, Int) = {
        val prefix = s"h${hex16(remotePath)}_"
        val name = base(remotePath)
        var attempts = 0
        var taken = 0
        var lastErr: Throwable = null
        var result: (String, Int) = null
        while (result == null && attempts <= maxRetries) {
            attempts += 1
            val sig = signature(remotePath, hconf)
            var created = false
            var readers = 0
            var superseded: Entry = null
            val e = current.compute(
              remotePath,
              (_, cur) =>
                  if (cur == null || cur.dead || sig.isEmpty || cur.sig != sig) {
                      // Absent, dead or stale: start a new generation; this reader copies. A superseded
                      // copy with readers stays reachable through byLocal until they release it; an idle
                      // one has none, so it dies here and is deleted below.
                      if (cur != null && !cur.dead && cur.readers == 0) {
                          cur.dead = true
                          superseded = cur
                      }
                      val gen = generation.incrementAndGet()
                      val dir = rootPath.resolve(s"$prefix$gen")
                      val fresh = new Entry(remotePath, gen, dir, dir.resolve(name), sig, new CompletableFuture[Unit]())
                      fresh.readers = 1
                      created = true
                      readers = 1
                      fresh
                  } else {
                      // A live copy that matches, an idle one included: its pending expiry check finds it held.
                      cur.readers += 1
                      readers = cur.readers
                      cur
                  }
            )
            if (superseded != null) retire(superseded)
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
                        if (stepsPastTakenDir(t, taken)) { taken += 1; attempts -= 1 }
                }
            } else {
                try {
                    e.ready.join() // throws if the copier failed
                    result = (e.localKey, readers)
                } catch {
                    case NonFatal(err) =>
                        lastErr = Option(err.getCause).getOrElse(err)
                        release(e)
                        if (stepsPastTakenDir(lastErr, taken)) { taken += 1; attempts -= 1 }
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

    /** A generation dir name taken on disk (by another JVM sharing rootPath, or one before a restart) is
      * not a failed copy: the next generation is tried without counting a retry, up to `maxTakenDirs`. */
    private def stepsPastTakenDir(err: Throwable, takenSoFar: Int): Boolean =
        err.isInstanceOf[TakenDirException] && takenSoFar < maxTakenDirs

    /** The copier's work, outside `compute`: claim a fresh generation dir, copy into it, publish,
      * complete. A dir of that name already on disk is not ours: it throws `TakenDirException` without
      * touching it. A copy that fails deletes its own dir before the failure reaches anyone. */
    private def copy(e: Entry, hconf: SerializableConfiguration): Unit = {
        Files.createDirectories(rootPath) // something may have removed it since this JVM started
        try Files.createDirectory(e.dir)
        catch { case _: FileAlreadyExistsException => throw new TakenDirException(e.dir) }
        try {
            HadoopUtils.copyToPath(e.remote, e.localKey, hconf) // the file plus same-stem sidecars, or the directory
            if (!Files.exists(e.local)) {
                throw new FileNotFoundException(s"Copy of ${e.remote} produced nothing at ${e.local}")
            }
            // Published before `ready` completes, so a waiter's releaseLocal always finds its entry.
            byLocal.put(e.localKey, e)
            countCopy(e.remote)
        } catch {
            case t: Throwable =>
                deleteRecursively(e.dir)
                throw t
        }
        e.ready.complete(())
    }

    /** Copier failure: retire the entry and drop the copier's reader, then fail the waiters (each
      * releases its own reader and retries). The waiters are failed even if retiring throws, or they
      * would block in `join` forever. The copy's dir, if it made one, is already deleted. */
    private def abandon(e: Entry, err: Throwable): Unit =
        try {
            current.compute(
              e.remote,
              (_, cur) => {
                  e.dead = true
                  if (e.readers > 0) e.readers -= 1
                  if (cur eq e) null else cur
              }
            )
            byLocal.remove(e.localKey, e)
        } finally e.ready.completeExceptionally(err)

    /** Drops one reader of `e`. At zero a current, complete copy with a known signature is kept idle for
      * the retention window, and an expiry check is scheduled; any other entry dies (no reader can join
      * it any more) and its dir is deleted. Returns the readers left; a release beyond the last reader,
      * or of an idle copy, is a no-op. */
    private def release(e: Entry): Int = {
        var left = -1
        var died = false
        var schedule = false
        val keepMs = retention
        current.compute(
          e.remote,
          (_, cur) => {
              if (e.readers > 0) {
                  e.readers -= 1
                  left = e.readers
                  if (left == 0 && !e.dead) {
                      if (keepMs > 0 && (cur eq e) && e.copied && e.sig.isDefined) {
                          e.idleUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(keepMs)
                          if (!e.evictionDue) {
                              e.evictionDue = true
                              schedule = true
                          }
                      } else {
                          e.dead = true
                          died = true
                      }
                  }
              }
              if (died && (cur eq e)) null else cur
          }
        )
        if (died) retire(e)
        else if (schedule) scheduleExpiry(e, TimeUnit.MILLISECONDS.toNanos(keepMs))
        math.max(left, 0)
    }

    /** Checks `e` for expiry after `delayNanos`; if the check cannot be scheduled, the idle copy is
      * deleted now, so that it is never left with no check pending. */
    private def scheduleExpiry(e: Entry, delayNanos: Long): Unit =
        try {
            evictor.schedule(
              new Runnable {
                  override def run(): Unit = expire(e)
              },
              delayNanos,
              TimeUnit.NANOSECONDS
            )
        } catch {
            case NonFatal(err) =>
                logWarning(s"Could not schedule the expiry of the idle node-file copy ${e.local}; deleting it now", err)
                expire(e, now = true)
        }

    /** The expiry check: an entry still idle past its deadline (or `now`) dies and its dir is deleted; an
      * idle one taken back and released again since is checked again at its new deadline; a held or dead
      * one is left to its holders (its next idle period schedules a new check). */
    private def expire(e: Entry, now: Boolean = false): Unit =
        try {
            var died = false
            var again = 0L
            current.compute(
              e.remote,
              (_, cur) => {
                  if (e.dead || e.readers > 0) {
                      e.evictionDue = false
                  } else {
                      val wait = e.idleUntil - System.nanoTime()
                      if (now || wait <= 0) {
                          e.dead = true
                          e.evictionDue = false
                          died = true
                      } else {
                          again = wait
                      }
                  }
                  if (died && (cur eq e)) null else cur
              }
            )
            if (died) retire(e)
            else if (again > 0) scheduleExpiry(e, again)
        } catch {
            case NonFatal(err) => logWarning(s"Expiry check of the node-file copy ${e.local} failed", err)
        }

    /** After `e` died: unpublish it and delete its dir, if its copy completed (a failed copy deleted its
      * own dir; a taken dir name is not ours). */
    private def retire(e: Entry): Unit = {
        byLocal.remove(e.localKey, e)
        if (e.copied) deleteRecursively(e.dir)
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

    /** Test-visible: readers holding the current copy of `remotePath` (0 when it is idle or absent). */
    private[gbx] def readersOf(remotePath: String): Int = Option(current.get(remotePath)).map(_.readers).getOrElse(0)

    /** Test-visible: copies made for this remote (kept for the most recently copied remotes). */
    private[gbx] def copyCount(remotePath: String): Int =
        copyCounts.synchronized(copyCounts.getOrDefault(remotePath, 0))

    private def countCopy(remote: String): Unit =
        copyCounts.synchronized(copyCounts.put(remote, copyCounts.getOrDefault(remote, 0) + 1))

    /** Deletes `p` and everything under it, children first, without following links; never throws.
      * One walk that visits each entry once and deletes it as it goes: an entry that vanishes under the
      * walk (something else deleting the same tree) is skipped, not an error, so this returns once the
      * whole tree is gone. Failures to delete are logged. */
    private[util] def deleteRecursively(p: Path): Unit =
        try {
            Files.walkFileTree(
              p,
              new SimpleFileVisitor[Path] {
                  override def visitFile(f: Path, attrs: BasicFileAttributes): FileVisitResult = {
                      deleteOne(f)
                      FileVisitResult.CONTINUE
                  }
                  // NoSuchFileException: gone already. Anything else (unreadable): try deleting it as it is.
                  override def visitFileFailed(f: Path, err: IOException): FileVisitResult = {
                      if (!err.isInstanceOf[NoSuchFileException]) deleteOne(f)
                      FileVisitResult.CONTINUE
                  }
                  override def postVisitDirectory(d: Path, err: IOException): FileVisitResult = {
                      deleteOne(d)
                      FileVisitResult.CONTINUE
                  }
              }
            )
        } catch {
            case NonFatal(err) => logWarning(s"Could not delete $p", err)
        }

    private def deleteOne(q: Path): Unit =
        try Files.deleteIfExists(q)
        catch { case NonFatal(err) => logWarning(s"Could not delete $q", err) }

}
