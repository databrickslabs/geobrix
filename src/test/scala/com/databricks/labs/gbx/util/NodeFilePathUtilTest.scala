package com.databricks.labs.gbx.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** NodeFilePathUtil's refcounted, generation-scoped node-file cache, driven directly. */
class NodeFilePathUtilTest extends AnyFunSuite {

    private val hconf = new SerializableConfiguration(new Configuration())

    test("readLock counts holders; releaseByLocal and releaseByRemote return the holders left") {
        withRemoteFile("count.tif", "count") { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val (a, n1) = NodeFilePathUtil.readLock(remote, hconf)
            val (b, n2) = NodeFilePathUtil.readLock(remote, hconf)
            withClue(s"two readLocks of $remote returned ($a, $n1), ($b, $n2): ") { assert(a == b && n1 == 1 && n2 == 2) }
            withClue(s"copies made for $remote: ") { assert(NodeFilePathUtil.copyCount(remote) == 1) }
            withClue("releaseByLocal of the copy: ") { assert(NodeFilePathUtil.releaseByLocal(a) == 1) }
            withClue("releaseByRemote of the last holder: ") { assert(NodeFilePathUtil.releaseByRemote(remote) == 0) }
            withClue(s"generation dir ${Paths.get(a).getParent} after the last release: ") {
                assert(!Files.exists(Paths.get(a).getParent))
            }
            withClue("a stale releaseByLocal / releaseByRemote finds nothing: ") {
                assert(NodeFilePathUtil.releaseByLocal(a) == 0 && NodeFilePathUtil.releaseByRemote(remote) == 0)
            }
        }
    }

    test("each generation gets its own h<16 hex>_<gen> dir; a reacquire after the last release copies again") {
        withRemoteFile("gen.tif", "gen") { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val (a, _) = NodeFilePathUtil.readLock(remote, hconf)
            NodeFilePathUtil.releaseByLocal(a)
            val (b, _) = NodeFilePathUtil.readLock(remote, hconf)
            val (da, db) = (Paths.get(a).getParent, Paths.get(b).getParent)
            try {
                withClue(s"generation dirs $da, $db: ") {
                    assert(Seq(da, db).forall(_.getFileName.toString.matches("h[0-9a-f]{16}_[0-9]+")))
                    assert(da != db && prefix(da) == prefix(db) && db.getParent == NodeFilePathUtil.rootPath)
                }
                withClue(s"copies made for $remote: ") { assert(NodeFilePathUtil.copyCount(remote) == 2) }
            } finally NodeFilePathUtil.releaseByLocal(b)
        }
    }

    test("a copy's same-stem sidecars and checksum files are freed with it") {
        val dir = Files.createTempDirectory("gbx_nfpu_")
        try {
            Seq("roads.shp", "roads.shx", "roads.dbf", "roads.prj", "other.txt").foreach(n => Files.write(dir.resolve(n), n.getBytes))
            val remote = HadoopUtils.cleanPath(dir.resolve("roads.shp").toString)
            val (local, _) = NodeFilePathUtil.readLock(remote, hconf)
            val gen = Paths.get(local).getParent
            val copied = gen.toFile.list().toSet
            withClue(s"$gen holds $copied: ") {
                assert(Set("roads.shp", "roads.shx", "roads.dbf", "roads.prj").subsetOf(copied) && !copied("other.txt"))
            }
            NodeFilePathUtil.releaseByRemote(remote)
            withClue(s"generation dir $gen after release: ") { assert(!Files.exists(gen)) }
        } finally deleteTree(dir)
    }

    test("a path with a space is cached and freed") {
        assertRoundTrip("d u")
    }

    test("a path with a space and non-ASCII characters is cached and freed") {
        val jnu = Charset.forName(System.getProperty("sun.jnu.encoding", "UTF-8"))
        assume(jnu.newEncoder().canEncode("d ü"), s"this JVM's file-name encoding ($jnu) has no ü")
        assertRoundTrip("d ü")
    }

    test("a missing remote throws and leaves no dir behind") {
        val dir = Files.createTempDirectory("gbx_nfpu_")
        try {
            val remote = HadoopUtils.cleanPath(dir.resolve("absent.tif").toString)
            val before = entries(NodeFilePathUtil.rootPath)
            intercept[Exception](NodeFilePathUtil.readLock(remote, hconf))
            val leftovers = entries(NodeFilePathUtil.rootPath).diff(before)
            withClue(s"new entries under ${NodeFilePathUtil.rootPath}: $leftovers: ") { assert(leftovers.isEmpty) }
            withClue(s"copies made for $remote: ") { assert(NodeFilePathUtil.copyCount(remote) == 0) }
        } finally deleteTree(dir)
    }

    test("concurrent readLock / releaseByRemote never serve a missing copy and free every generation") {
        withRemoteFile("stress.tif", "s" * 4096) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val ops = new AtomicInteger()
            val bad = new AtomicInteger()
            val dirs = ConcurrentHashMap.newKeySet[Path]()
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
            val pool = Executors.newFixedThreadPool(4)
            try {
                (0 until 4).map { _ =>
                    pool.submit(new Runnable {
                        override def run(): Unit =
                            while (System.nanoTime() < deadline) {
                                try {
                                    val (local, n) = NodeFilePathUtil.readLock(remote, hconf)
                                    val p = Paths.get(local)
                                    dirs.add(p.getParent)
                                    try if (n < 1 || Files.size(p) != 4096) bad.incrementAndGet()
                                    catch { case NonFatal(_) => bad.incrementAndGet() }
                                    finally NodeFilePathUtil.releaseByRemote(remote)
                                    ops.incrementAndGet()
                                } catch { case NonFatal(_) => bad.incrementAndGet() }
                            }
                    })
                }.foreach(_.get(60, TimeUnit.SECONDS))
            } finally pool.shutdownNow()
            withClue(s"${ops.get} ops, ${bad.get} bad: ") { assert(ops.get > 0 && bad.get == 0) }
            val left = dirs.asScala.filter(Files.exists(_))
            withClue(s"generation dirs still on disk: $left: ") { assert(left.isEmpty) }
        }
    }

    // ---- helpers ------------------------------------------------------------------------------------

    /** Caches `<tmp>/<dirName>/x.geojson` and checks the copy's name, content and release. */
    private def assertRoundTrip(dirName: String): Unit = {
        val root = Files.createTempDirectory("gbx_nfpu_")
        try {
            val f = Files.createDirectories(root.resolve(dirName)).resolve("x.geojson")
            Files.write(f, """{"type":"FeatureCollection","features":[]}""".getBytes(StandardCharsets.UTF_8))
            for (remote <- Seq(f.toString, HadoopUtils.cleanPath(f.toString))) {
                val (local, _) = NodeFilePathUtil.readLock(remote, hconf)
                val p = Paths.get(local)
                withClue(s"$remote cached at $p: ") {
                    assert(p.getFileName.toString == "x.geojson")
                    assert(java.util.Arrays.equals(Files.readAllBytes(p), Files.readAllBytes(f)))
                }
                withClue(s"copies made for $remote: ") { assert(NodeFilePathUtil.copyCount(remote) == 1) }
                NodeFilePathUtil.releaseByLocal(local)
                withClue(s"generation dir ${p.getParent} after release: ") { assert(!Files.exists(p.getParent)) }
            }
        } finally deleteTree(root)
    }

    private def prefix(gen: Path): String = gen.getFileName.toString.takeWhile(_ != '_')

    private def withRemoteFile[T](name: String, content: String)(body: Path => T): T = {
        val dir = Files.createTempDirectory("gbx_nfpu_")
        try {
            val f = dir.resolve(name)
            Files.write(f, content.getBytes(StandardCharsets.UTF_8))
            body(f)
        } finally deleteTree(dir)
    }

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
