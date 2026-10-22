package com.databricks.labs.gbx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FilterFileSystem, FutureDataInputStreamBuilder, LocalFileSystem, Path => HPath}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatest.BeforeAndAfterEach

import java.io.IOException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.FileTime
import java.util.concurrent.{Callable, ConcurrentHashMap, CountDownLatch, CyclicBarrier, ExecutionException, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

class NodeFileManagerTest extends AnyFunSuite with BeforeAndAfterEach {

    // Note: NodeFileManager is a stateful singleton that interacts with NodeFilePathUtil
    // These tests verify the manager's state management and basic operations

    // Retention off: these tests check that a copy is freed at its last release.
    override def beforeEach(): Unit = {
        super.beforeEach()
        NodeFileManager.setRetentionMs(0L)
    }

    override def afterEach(): Unit =
        try NodeFileManager.setRetentionMs(NodeFilePathUtil.DefaultRetentionMs)
        finally super.afterEach()

    test("init(ExpressionConfig) takes the retention from the conf: default 60 s, 0 for none, else rejected") {
        def retentionAfter(configs: Map[String, String]): Long = {
            NodeFileManager.init(ExpressionConfig(configs, localConf()))
            NodeFilePathUtil.retentionMs
        }
        retentionAfter(Map.empty) shouldBe 60000L
        retentionAfter(Map(NodeFileManager.RetentionKey -> "0")) shouldBe 0L
        retentionAfter(Map(NodeFileManager.RetentionKey -> " 1234 ")) shouldBe 1234L
        for (bad <- Seq("-1", "60s", "")) {
            val e = intercept[IllegalArgumentException](retentionAfter(Map(NodeFileManager.RetentionKey -> bad)))
            withClue(s"the error for '$bad': ") { e.getMessage should include(NodeFileManager.RetentionKey) }
        }
        // init(hadoopConf) sets the Hadoop config only: the retention stays as it was.
        NodeFileManager.setRetentionMs(4321L)
        NodeFileManager.init(localConf())
        NodeFilePathUtil.retentionMs shouldBe 4321L
    }

    test("init should accept SerializableConfiguration") {
        val spark = SparkSession.builder()
            .master("local[1]")
            .appName("NodeFileManagerTest")
            .getOrCreate()

        val hconf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

        noException should be thrownBy NodeFileManager.init(hconf)

        spark.stop()
    }

    test("init should store configuration") {
        val spark = SparkSession.builder()
            .master("local[1]")
            .appName("NodeFileManagerTest")
            .getOrCreate()

        val hconf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
        NodeFileManager.init(hconf)

        // After init, subsequent operations should have access to hconf
        // Verify by checking that manager can be initialized multiple times
        noException should be thrownBy NodeFileManager.init(hconf)

        spark.stop()
    }

    test("NodeFileManager should be a singleton object") {
        // Verify it's an object (singleton pattern)
        NodeFileManager should not be null
        NodeFileManager.getClass.getName should include ("NodeFileManager")
    }

    test("releaseRemote should not throw for empty remote path") {
        val spark = SparkSession.builder()
            .master("local[1]")
            .appName("NodeFileManagerTest")
            .getOrCreate()

        val hconf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
        NodeFileManager.init(hconf)

        // Releasing a non-existent remote path should not crash
        // (though it won't do anything useful)
        noException should be thrownBy NodeFileManager.releaseRemote("")

        spark.stop()
    }

    test("readRemote on a FileGDB directory returns a local directory holding its files") {
        val gdb = FileGDBFixture.unzipBridges()
        try {
            val sourceFiles = gdb.toFile.list().toSet
            NodeFileManager.init(localConf())
            // OGR_DataSource.inferSchema passes HadoopUtils.cleanPath(<load path>): file:/... here, bare /Volumes/...
            // on a cluster. With or without a trailing slash, the copy must be the .gdb directory itself.
            for (remote <- Seq(HadoopUtils.cleanPath(gdb.toString), HadoopUtils.cleanPath(s"$gdb/"))) {
                val local = Paths.get(NodeFileManager.readRemote(remote))
                try {
                    local.startsWith(NodeFilePathUtil.rootPath) shouldBe true
                    local.getFileName.toString shouldBe gdb.getFileName.toString
                    Files.isDirectory(local) shouldBe true
                    val copied = Option(local.toFile.list()).map(_.toSet).getOrElse(Set.empty[String])
                    withClue(s"$remote staged to $local holding $copied: ") {
                        sourceFiles.diff(copied) shouldBe empty
                    }
                } finally NodeFileManager.releaseRemote(remote)
            }
        } finally deleteTree(gdb.getParent)
    }

    // ---- Read / release lifecycle (T1-T8) ---------------------------------------------------------
    // Local-FS config, a fresh remote per test: the cache objects are JVM singletons, so a remote path
    // is never reused across tests.

    test("T1 readRemote then releaseRemote frees the copy and its generation dir") {
        NodeFileManager.init(localConf())
        withRemoteFile("t1.tif", "one".getBytes(StandardCharsets.UTF_8)) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val local = Paths.get(NodeFileManager.readRemote(remote))
            withClue(s"readRemote($remote) returned $local: ") { assert(Files.isRegularFile(local)) }
            NodeFileManager.releaseRemote(remote)
            withClue(s"after releaseRemote($remote), $local and its dir ${local.getParent}: ") {
                assert(!Files.exists(local) && !Files.exists(local.getParent))
            }
        }
    }

    test("T2 two holders: the copy survives the first release and is freed by the second") {
        NodeFileManager.init(localConf())
        withRemoteFile("t2.tif", "two".getBytes(StandardCharsets.UTF_8)) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val a = NodeFileManager.readRemote(remote)
            val b = NodeFileManager.readRemote(remote)
            withClue(s"two holders of $remote: ") { assert(a == b) }
            withClue(s"copies made for $remote: ") { assert(NodeFilePathUtil.copyCount(remote) == 1) }
            NodeFileManager.releaseRemote(remote)
            withClue(s"one holder of $remote left, $a: ") { assert(Files.isRegularFile(Paths.get(a))) }
            NodeFileManager.releaseRemote(remote)
            withClue(s"no holder of $remote left, ${Paths.get(a).getParent}: ") {
                assert(!Files.exists(Paths.get(a).getParent))
            }
        }
    }

    test("T3 releaseLocal of an unknown path is a no-op that leaves the other holders' count intact") {
        NodeFileManager.init(localConf())
        withRemoteFile("t3.tif", "three".getBytes(StandardCharsets.UTF_8)) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val a = NodeFileManager.readRemote(remote)
            NodeFileManager.readRemote(remote)
            // A sidecar GDAL lists next to the copy, a user's own local file, the remote path itself.
            Seq(s"$a.aux.xml", f.toString, remote, "/vsimem/t3.tif").foreach(NodeFileManager.releaseLocal)
            NodeFileManager.releaseLocal(a)
            withClue(s"unknown releases must not count; one holder of $a left: ") {
                assert(Files.isRegularFile(Paths.get(a)))
            }
            NodeFileManager.releaseLocal(a)
            withClue(s"both holders released, ${Paths.get(a).getParent}: ") {
                assert(!Files.exists(Paths.get(a).getParent))
            }
        }
    }

    test("T4 a .gdb is copied as <gen>/x.gdb with its tables directly inside; release frees the gen dir") {
        NodeFileManager.init(localConf())
        val gdb = FileGDBFixture.unzipBridges()
        try {
            val sourceFiles = gdb.toFile.list().toSet
            val remote = HadoopUtils.cleanPath(gdb.toString)
            val local = Paths.get(NodeFileManager.readRemote(remote))
            val gen = local.getParent
            withClue(s"$remote staged to $local: ") {
                assert(local.getFileName.toString == gdb.getFileName.toString && Files.isDirectory(local))
            }
            val direct = local.toFile.listFiles().filter(_.isFile).map(_.getName).toSet
            withClue(s"tables directly inside $local (missing ${sourceFiles.diff(direct)}): ") {
                assert(sourceFiles.subsetOf(direct) && !Files.exists(local.resolve(gdb.getFileName)))
            }
            NodeFileManager.releaseRemote(remote)
            withClue(s"after releaseRemote($remote), generation dir $gen: ") { assert(!Files.exists(gen)) }
            withClue(s"generation dir $gen sits directly under ${NodeFilePathUtil.rootPath}: ") {
                assert(gen.getParent == NodeFilePathUtil.rootPath && gen.getFileName.toString.matches("h[0-9a-f]{16}_[0-9]+"))
            }
        } finally deleteTree(gdb.getParent)
    }

    test("T5 stress: 8 threads x 2 s of readRemote / check / releaseLocal never see a missing copy") {
        NodeFileManager.init(localConf())
        val payload = Array.tabulate[Byte](64 * 1024)(i => (i % 251).toByte)
        withRemoteFile("t5.tif", payload) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val ops = new AtomicInteger()
            val missing = new AtomicInteger()
            val errors = new AtomicInteger()
            val firstError = new java.util.concurrent.atomic.AtomicReference[Throwable]()
            val dirs = ConcurrentHashMap.newKeySet[Path]()
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
            val pool = Executors.newFixedThreadPool(8)
            try {
                val workers = (0 until 8).map { _ =>
                    pool.submit(new Runnable {
                        override def run(): Unit =
                            while (System.nanoTime() < deadline) {
                                try {
                                    val local = NodeFileManager.readRemote(remote)
                                    val p = Paths.get(local)
                                    dirs.add(p.getParent)
                                    try {
                                        if (!Files.isRegularFile(p) || Files.size(p) != payload.length) missing.incrementAndGet()
                                    } catch { case NonFatal(_) => missing.incrementAndGet() }
                                    finally NodeFileManager.releaseLocal(local)
                                    ops.incrementAndGet()
                                } catch {
                                    case NonFatal(e) => errors.incrementAndGet(); firstError.compareAndSet(null, e)
                                }
                            }
                    })
                }
                workers.foreach(_.get(60, TimeUnit.SECONDS))
            } finally pool.shutdownNow()
            withClue(s"${ops.get} ops, ${missing.get} missing, ${errors.get} errors (first: ${firstError.get}): ") {
                assert(ops.get > 0 && missing.get == 0 && errors.get == 0)
            }
            val left = dirs.asScala.filter(Files.exists(_))
            withClue(s"generation dirs of $remote still on disk after every release: $left: ") { assert(left.isEmpty) }
        }
    }

    test("T6 a same-length overwrite with a newer mtime is served fresh; the old holder keeps the old copy") {
        NodeFileManager.init(localConf())
        withRemoteFile("t6.geojson", "AAAA".getBytes(StandardCharsets.UTF_8)) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val a = Paths.get(NodeFileManager.readRemote(remote))
            val t0 = Files.getLastModifiedTime(f).toMillis
            Files.write(f, "BBBB".getBytes(StandardCharsets.UTF_8))
            Files.setLastModifiedTime(f, FileTime.fromMillis(t0 + 2000))
            val b = Paths.get(NodeFileManager.readRemote(remote))
            withClue(s"after the overwrite readRemote($remote) returned $b: ") { assert(text(b) == "BBBB") }
            withClue(s"the old holder's copy $a: ") { assert(a != b && text(a) == "AAAA") }
            withClue(s"copies made for $remote: ") { assert(NodeFilePathUtil.copyCount(remote) == 2) }
            NodeFileManager.releaseLocal(a.toString)
            withClue(s"old generation ${a.getParent} after its last release; new copy $b: ") {
                assert(!Files.exists(a.getParent) && Files.isRegularFile(b))
            }
            NodeFileManager.releaseRemote(remote)
            withClue(s"new generation ${b.getParent} after its last release: ") { assert(!Files.exists(b.getParent)) }
        }
    }

    test("T7 a .gdb table changed in place is served with its new bytes") {
        NodeFileManager.init(localConf())
        val gdb = FileGDBFixture.unzipBridges()
        try {
            val remote = HadoopUtils.cleanPath(gdb.toString)
            val a = Paths.get(NodeFileManager.readRemote(remote))
            val table = Files.list(gdb).iterator().asScala.filter(_.getFileName.toString.endsWith(".gdbtable")).toSeq
                .minBy(_.getFileName.toString)
            val before = Files.readAllBytes(table)
            val after = before.clone()
            after(after.length - 1) = (after.last ^ 0x5a).toByte // same length, new content
            val t0 = Files.getLastModifiedTime(table).toMillis
            Files.write(table, after)
            Files.setLastModifiedTime(table, FileTime.fromMillis(t0 + 2000))
            val b = Paths.get(NodeFileManager.readRemote(remote))
            val name = table.getFileName.toString
            withClue(s"$name in $b after the in-place change: ") {
                assert(java.util.Arrays.equals(Files.readAllBytes(b.resolve(name)), after))
            }
            withClue(s"$name in the old holder's copy $a: ") {
                assert(a != b && java.util.Arrays.equals(Files.readAllBytes(a.resolve(name)), before))
            }
            NodeFileManager.releaseLocal(a.toString)
            NodeFileManager.releaseLocal(b.toString)
            withClue(s"generation dirs ${a.getParent}, ${b.getParent} after release: ") {
                assert(!Files.exists(a.getParent) && !Files.exists(b.getParent))
            }
        } finally deleteTree(gdb.getParent)
    }

    test("T8 a failed copy throws and leaves no partial dir; after the fault clears the copy is complete") {
        val gdb = FileGDBFixture.unzipBridges()
        try {
            Files.write(gdb.resolve(FailingFS.Poison), "x".getBytes(StandardCharsets.UTF_8))
            val sourceFiles = gdb.toFile.list().toSet
            // HadoopUtils.cleanPath rewrites any unknown scheme to file:/..., so the failing FS is
            // registered for file: itself (uncached, so no other test's FileSystem is affected).
            val conf = new Configuration()
            conf.set("fs.file.impl", classOf[FailingFS].getName)
            conf.setBoolean("fs.file.impl.disable.cache", true)
            NodeFileManager.init(new SerializableConfiguration(conf))
            val remote = HadoopUtils.cleanPath(gdb.toString)
            val before = entries(NodeFilePathUtil.rootPath)
            FailingFS.failing = true
            val err = intercept[Exception](NodeFileManager.readRemote(remote))
            val causes = Iterator.iterate[Throwable](err)(_.getCause).takeWhile(_ != null).map(String.valueOf).toList
            withClue(s"readRemote's failure must come from the injected fault: $causes: ") {
                assert(causes.exists(_.contains("injected read failure")))
            }
            val leftovers = entries(NodeFilePathUtil.rootPath).diff(before)
            withClue(s"readRemote failed with $err; new entries under ${NodeFilePathUtil.rootPath}: $leftovers: ") {
                assert(leftovers.isEmpty)
            }
            withClue(s"copies made for $remote while the FS refused ${FailingFS.Poison}: ") {
                assert(NodeFilePathUtil.copyCount(remote) == 0)
            }
            FailingFS.failing = false
            val local = Paths.get(NodeFileManager.readRemote(remote))
            val copied = local.toFile.list().toSet
            withClue(s"after the fault cleared, $local is missing ${sourceFiles.diff(copied)}: ") {
                assert(sourceFiles.subsetOf(copied))
            }
            NodeFileManager.releaseRemote(remote)
            withClue(s"generation dir ${local.getParent} after release: ") { assert(!Files.exists(local.getParent)) }
        } finally {
            FailingFS.failing = false
            NodeFileManager.init(localConf())
            deleteTree(gdb.getParent)
        }
    }

    test("T8b waiters joined to a copy that fails release their hold, retry, and share the next copy") {
        val gdb = FileGDBFixture.unzipBridges()
        val threads = 6
        val pool = Executors.newFixedThreadPool(threads)
        try {
            Files.write(gdb.resolve(FailingFS.Poison), "x".getBytes(StandardCharsets.UTF_8))
            val sourceFiles = gdb.toFile.list().toSet
            val conf = failingConf()
            NodeFileManager.init(conf)
            val remote = HadoopUtils.cleanPath(gdb.toString)
            val before = entries(NodeFilePathUtil.rootPath)
            val allJoined = new AtomicBoolean(false)
            // The copy fails only once every thread holds its generation: the other five wait on it.
            FailingFS.beforeFailure = () => {
                val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
                while (NodeFilePathUtil.readersOf(remote) < threads && System.nanoTime() < deadline) Thread.sleep(5)
                allJoined.set(NodeFilePathUtil.readersOf(remote) == threads)
            }
            FailingFS.failuresLeft = 1
            FailingFS.failing = true
            val start = new CountDownLatch(1)
            val allHold = new CyclicBarrier(threads)
            val results = (0 until threads).map { _ =>
                pool.submit(new Callable[(String, Int, Boolean)] {
                    override def call(): (String, Int, Boolean) = {
                        start.await()
                        val (local, n) = NodeFilePathUtil.readLock(remote, conf)
                        try {
                            val complete = sourceFiles.subsetOf(Paths.get(local).toFile.list().toSet)
                            allHold.await(30, TimeUnit.SECONDS) // every thread holds the copy at once
                            (local, n, complete)
                        } finally NodeFilePathUtil.releaseByLocal(local)
                    }
                })
            }
            start.countDown()
            val got = results.map(_.get(60, TimeUnit.SECONDS))
            withClue("all threads held the failing copy's generation when it failed: ") { assert(allJoined.get) }
            withClue(s"local paths returned: ${got.map(_._1).distinct}: ") { assert(got.map(_._1).distinct.size == 1) }
            withClue(s"holder counts returned: ${got.map(_._2).sorted}: ") { assert(got.map(_._2).sorted == (1 to threads)) }
            withClue(s"complete copies: ${got.map(_._3)}: ") { assert(got.forall(_._3)) }
            withClue(s"copies made for $remote: ") { assert(NodeFilePathUtil.copyCount(remote) == 1) }
            withClue(s"holders of $remote after every release: ") { assert(NodeFilePathUtil.readersOf(remote) == 0) }
            val leftovers = entries(NodeFilePathUtil.rootPath).diff(before)
            withClue(s"new entries under ${NodeFilePathUtil.rootPath}: $leftovers: ") { assert(leftovers.isEmpty) }
        } finally {
            FailingFS.reset()
            pool.shutdownNow()
            NodeFileManager.init(localConf())
            deleteTree(gdb.getParent)
        }
    }

    test("T8c a fatal copy error reaches every waiter instead of leaving it blocked") {
        val gdb = FileGDBFixture.unzipBridges()
        val threads = 4
        val pool = Executors.newFixedThreadPool(threads)
        try {
            Files.write(gdb.resolve(FailingFS.Poison), "x".getBytes(StandardCharsets.UTF_8))
            val conf = failingConf()
            NodeFileManager.init(conf)
            val remote = HadoopUtils.cleanPath(gdb.toString)
            val before = entries(NodeFilePathUtil.rootPath)
            val firstFailure = new AtomicBoolean(true)
            FailingFS.beforeFailure = () => {
                if (firstFailure.getAndSet(false)) {
                    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
                    while (NodeFilePathUtil.readersOf(remote) < threads && System.nanoTime() < deadline) Thread.sleep(5)
                }
            }
            FailingFS.fatal = true
            FailingFS.failing = true
            val start = new CountDownLatch(1)
            val results = (0 until threads).map { _ =>
                pool.submit(new Callable[String] {
                    override def call(): String = {
                        start.await()
                        val (local, _) = NodeFilePathUtil.readLock(remote, conf)
                        NodeFilePathUtil.releaseByLocal(local)
                        local
                    }
                })
            }
            start.countDown()
            results.foreach { r =>
                val e = intercept[ExecutionException](r.get(60, TimeUnit.SECONDS))
                val causes = Iterator.iterate[Throwable](e.getCause)(_.getCause).takeWhile(_ != null).toList
                withClue(s"a waiter's failure must carry the injected fatal error: $causes: ") {
                    assert(causes.exists(c => String.valueOf(c.getMessage).contains("injected fatal read failure")))
                }
            }
            withClue(s"holders of $remote after the failures: ") { assert(NodeFilePathUtil.readersOf(remote) == 0) }
            val leftovers = entries(NodeFilePathUtil.rootPath).diff(before)
            withClue(s"new entries under ${NodeFilePathUtil.rootPath}: $leftovers: ") { assert(leftovers.isEmpty) }
            FailingFS.reset()
            val (local, n) = NodeFilePathUtil.readLock(remote, conf)
            try withClue(s"after the fault cleared, $local holders: ") { assert(n == 1) }
            finally NodeFilePathUtil.releaseByLocal(local)
        } finally {
            FailingFS.reset()
            pool.shutdownNow()
            NodeFileManager.init(localConf())
            deleteTree(gdb.getParent)
        }
    }

    test("a lease releases exactly once, however often it is closed") {
        NodeFileManager.init(localConf())
        withRemoteFile("lease.tif", "lease".getBytes(StandardCharsets.UTF_8)) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val lease = NodeFileManager.acquire(remote)
            val other = NodeFileManager.readRemote(remote)
            withClue(s"lease and readRemote of $remote: ") { assert(lease.localPath == other) }
            lease.close()
            lease.close()
            withClue(s"a double close must release once; the other holder's $other: ") {
                assert(Files.isRegularFile(Paths.get(other)))
            }
            NodeFileManager.releaseLocal(other)
            withClue(s"both released, ${Paths.get(other).getParent}: ") { assert(!Files.exists(Paths.get(other).getParent)) }
        }
    }

    test("releaseRemote of a local copy path releases nothing; releaseLocal of it does") {
        NodeFileManager.init(localConf())
        withRemoteFile("rr_local.tif", "local".getBytes(StandardCharsets.UTF_8)) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val local = NodeFileManager.readRemote(remote)
            NodeFileManager.releaseRemote(local) // a copy's local path is not a remote
            withClue(s"releaseRemote($local) must not release the copy: ") { assert(Files.isRegularFile(Paths.get(local))) }
            NodeFileManager.releaseLocal(local)
            withClue(s"releaseLocal($local), ${Paths.get(local).getParent}: ") {
                assert(!Files.exists(Paths.get(local).getParent))
            }
        }
    }

    // ---- helpers ------------------------------------------------------------------------------------

    private def localConf(): SerializableConfiguration = new SerializableConfiguration(new Configuration())

    /** HadoopUtils.cleanPath rewrites any unknown scheme to file:/..., so the failing FS is registered for file:
      * itself (uncached, so no other test's FileSystem is affected). */
    private def failingConf(): SerializableConfiguration = {
        val conf = new Configuration()
        conf.set("fs.file.impl", classOf[FailingFS].getName)
        conf.setBoolean("fs.file.impl.disable.cache", true)
        new SerializableConfiguration(conf)
    }

    /** A fresh temp dir holding `name` with `bytes`; the dir is deleted recursively after `body`. */
    private def withRemoteFile[T](name: String, bytes: Array[Byte])(body: Path => T): T = {
        val dir = Files.createTempDirectory("gbx_nfm_")
        try {
            val f = dir.resolve(name)
            Files.write(f, bytes)
            body(f)
        } finally deleteTree(dir)
    }

    private def text(p: Path): String = new String(Files.readAllBytes(p), StandardCharsets.UTF_8)

    private def entries(dir: Path): Set[String] = {
        val s = Files.list(dir)
        try s.iterator().asScala.map(_.getFileName.toString).toSet
        finally s.close()
    }

    private def deleteTree(p: Path): Unit =
        if (Files.exists(p)) {
            val s = Files.walk(p)
            try s.sorted(java.util.Comparator.reverseOrder[Path]()).iterator().asScala.foreach(q => Files.deleteIfExists(q))
            finally s.close()
        }

}

/** A local FileSystem whose reads of [[FailingFS.Poison]] fail while [[FailingFS.failing]] is set. */
class FailingFS extends FilterFileSystem(new LocalFileSystem()) {

    override def initialize(name: URI, conf: Configuration): Unit = {
        fs.setConf(conf)
        fs.initialize(name, conf)
        super.initialize(name, conf)
    }

    override def open(f: HPath, bufferSize: Int): FSDataInputStream = {
        FailingFS.check(f)
        super.open(f, bufferSize)
    }

    // FileUtil.copy opens through the openFile builder, which FilterFileSystem hands straight to the
    // wrapped FS, bypassing open().
    override def openFile(path: HPath): FutureDataInputStreamBuilder = {
        FailingFS.check(path)
        super.openFile(path)
    }

}

object FailingFS {

    val Poison = "boom.gdbtable"
    @volatile var failing: Boolean = false
    /** Failures left before the fault clears by itself; negative: until `failing` is unset. */
    @volatile var failuresLeft: Int = -1
    /** Throw a fatal error (a LinkageError, which NonFatal does not match) instead of an IOException. */
    @volatile var fatal: Boolean = false
    /** Runs on the failing read just before it throws (a test waits here for other readers to join). */
    @volatile var beforeFailure: () => Unit = () => ()

    def reset(): Unit = {
        failing = false
        failuresLeft = -1
        fatal = false
        beforeFailure = () => ()
    }

    def check(p: HPath): Unit =
        if (failing && p.getName == Poison) {
            beforeFailure()
            synchronized {
                if (failuresLeft > 0) {
                    failuresLeft -= 1
                    if (failuresLeft == 0) failing = false
                }
            }
            if (fatal) throw new LinkageError(s"injected fatal read failure: $p")
            throw new IOException(s"injected read failure: $p")
        }

}
