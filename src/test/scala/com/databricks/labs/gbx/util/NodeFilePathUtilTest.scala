package com.databricks.labs.gbx.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.nio.file.attribute.FileTime
import java.util.concurrent.{Callable, ConcurrentHashMap, CountDownLatch, Executors, ThreadLocalRandom, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** NodeFilePathUtil's refcounted, generation-scoped node-file cache, driven directly. */
class NodeFilePathUtilTest extends AnyFunSuite with BeforeAndAfterEach {

    private val hconf = new SerializableConfiguration(new Configuration())

    // Every test starts with retention off, so a copy is freed at its last release; the retention tests turn it on.
    override def beforeEach(): Unit = {
        super.beforeEach()
        NodeFileManager.init(hconf)
        NodeFilePathUtil.setRetentionMs(0L)
    }

    override def afterEach(): Unit =
        try NodeFilePathUtil.setRetentionMs(NodeFilePathUtil.DefaultRetentionMs)
        finally super.afterEach()

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
        // Retention 1 ms and 20 ms race the eviction check against readers taking the idle copy back.
        for (retention <- Seq(0L, 1L, 20L)) {
            NodeFilePathUtil.setRetentionMs(retention)
            withRemoteFile(s"stress_$retention.tif", "s" * 4096) { f =>
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
                                        if (retention > 0) Thread.sleep(ThreadLocalRandom.current().nextLong(retention * 2))
                                    } catch { case NonFatal(_) => bad.incrementAndGet() }
                                }
                        })
                    }.foreach(_.get(60, TimeUnit.SECONDS))
                } finally pool.shutdownNow()
                withClue(s"retention $retention ms: ${ops.get} ops, ${bad.get} bad: ") { assert(ops.get > 0 && bad.get == 0) }
                withClue(s"retention $retention ms: copies made against generation dirs handed out ${dirs.size}: ") {
                    assert(NodeFilePathUtil.copyCount(remote) == dirs.size)
                }
                withClue(s"retention $retention ms: generation dirs still on disk: ") {
                    assert(eventually(5000)(dirs.asScala.forall(!Files.exists(_))), dirs.asScala.filter(Files.exists(_)))
                }
            }
        }
    }

    test("a generation dir name already on disk (another JVM, a restart) is never written to or deleted") {
        withRemoteFile("taken.tif", "fresh") { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val (probe, _) = NodeFilePathUtil.readLock(remote, hconf)
            NodeFilePathUtil.releaseByLocal(probe)
            val name = Paths.get(probe).getParent.getFileName.toString // h<16 hex>_<gen>
            val prefix = name.take(name.lastIndexOf('_') + 1)
            val gen = name.drop(prefix.length).toLong
            // The next generation names hold another process's copy of the same remote: same name, same length.
            val taken = (gen + 1 to gen + 8).map { g =>
                val d = Files.createDirectory(NodeFilePathUtil.rootPath.resolve(s"$prefix$g"))
                Files.write(d.resolve("taken.tif"), "stale".getBytes(StandardCharsets.UTF_8))
                d
            }
            try {
                val (local, n) = NodeFilePathUtil.readLock(remote, hconf)
                val dir = Paths.get(local).getParent
                try {
                    withClue(s"readLock returned $local with taken dirs $taken: ") { assert(!taken.contains(dir) && n == 1) }
                    withClue(s"content of $local: ") { assert(text(Paths.get(local)) == "fresh") }
                } finally NodeFilePathUtil.releaseByLocal(local)
                withClue(s"generation dir $dir after release: ") { assert(!Files.exists(dir)) }
                taken.foreach { d =>
                    withClue(s"the other process's dir $d: ") {
                        assert(d.toFile.list().toSeq == Seq("taken.tif") && text(d.resolve("taken.tif")) == "stale")
                    }
                }
            } finally taken.foreach(deleteTree)
        }
    }

    test("deleteRecursively removes the whole tree while something else deletes parts of it") {
        (1 to 5).foreach { round =>
            val root = Files.createTempDirectory("gbx_nfpu_del_")
            val leaves = (0 until 1000).map { i =>
                val d = Files.createDirectories(root.resolve(s"d$i/e"))
                (0 until 3).foreach(j => Files.write(d.resolve(s"f$j"), Array[Byte](1)))
                d
            }
            // Another deleter works slowly through the same tree, so the walk meets dirs that vanish under it.
            val started = new CountDownLatch(1)
            val other = new Thread(new Runnable {
                override def run(): Unit = {
                    started.countDown()
                    leaves.reverse.zipWithIndex.foreach { case (d, i) =>
                        scala.util.Try {
                            (0 until 3).foreach(j => Files.deleteIfExists(d.resolve(s"f$j")))
                            Files.deleteIfExists(d)
                            Files.deleteIfExists(d.getParent)
                        }
                        if (i % 20 == 0) Thread.sleep(1)
                    }
                }
            })
            other.start()
            started.await()
            Thread.sleep(2)
            NodeFilePathUtil.deleteRecursively(root)
            val left = Files.exists(root)
            other.join(60000)
            try withClue(s"round $round: $root when deleteRecursively returned: ") { assert(!left) }
            finally deleteTree(root)
        }
    }

    test("readers of a remote replaced over and over always get a complete copy, and every generation is freed") {
        // Retention 0 frees a superseded copy at its last release; with retention on, an idle copy is superseded
        // (deleted at once) or expires, while readers keep taking the current one back.
        for (retention <- Seq(0L, 25L)) {
            NodeFilePathUtil.setRetentionMs(retention)
            val root = Files.createTempDirectory("gbx_nfpu_sup_")
            val staging = Files.createTempDirectory("gbx_nfpu_sup_stage_")
            try {
                val f = root.resolve("sup.tif")
                // Version k: one repeated byte, its length a function of the byte, so a copy can check itself.
                def version(k: Int): Array[Byte] = Array.fill(4096 + (k % 26) * 7)(('A' + k % 26).toByte)
                val base = System.currentTimeMillis() - 3600000L
                // Each version lands whole (an atomic rename), with its own mtime.
                def publish(k: Int): Unit = {
                    val tmp = staging.resolve(s"v$k")
                    Files.write(tmp, version(k))
                    Files.setLastModifiedTime(tmp, FileTime.fromMillis(base + k * 2000L))
                    Files.move(tmp, f, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
                }
                publish(0)
                val remote = HadoopUtils.cleanPath(f.toString)
                val ops = new AtomicInteger()
                val bad = new AtomicInteger()
                val firstBad = new AtomicReference[String]()
                val dirs = ConcurrentHashMap.newKeySet[Path]()
                val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
                val pool = Executors.newFixedThreadPool(5)
                try {
                    val writer = pool.submit(new Callable[Int] {
                        override def call(): Int = {
                            var k = 1
                            while (System.nanoTime() < deadline) {
                                publish(k)
                                k += 1
                                Thread.sleep(20)
                            }
                            k
                        }
                    })
                    val readers = (0 until 4).map { _ =>
                        pool.submit(new Runnable {
                            override def run(): Unit =
                                while (System.nanoTime() < deadline) {
                                    try {
                                        val (local, n) = NodeFilePathUtil.readLock(remote, hconf)
                                        val p = Paths.get(local)
                                        dirs.add(p.getParent)
                                        try {
                                            val b = Files.readAllBytes(p)
                                            val whole = b.nonEmpty && b.forall(_ == b(0)) && b.length == 4096 + (b(0) - 'A') * 7
                                            if (n < 1 || !whole) {
                                                bad.incrementAndGet()
                                                firstBad.compareAndSet(null, s"$p: ${b.length} bytes, holders $n")
                                            }
                                        } catch {
                                            case NonFatal(e) => bad.incrementAndGet(); firstBad.compareAndSet(null, s"$p: $e")
                                        } finally NodeFilePathUtil.releaseByLocal(local)
                                        ops.incrementAndGet()
                                        Thread.sleep(ThreadLocalRandom.current().nextLong(40))
                                    } catch {
                                        case NonFatal(e) => bad.incrementAndGet(); firstBad.compareAndSet(null, e.toString)
                                    }
                                }
                        })
                    }
                    val versions = writer.get(60, TimeUnit.SECONDS)
                    readers.foreach(_.get(60, TimeUnit.SECONDS))
                    withClue(s"retention $retention ms: $versions versions, ${ops.get} reads, ${bad.get} bad (first: ${firstBad.get}): ") {
                        assert(versions > 10 && ops.get > 0 && bad.get == 0)
                    }
                } finally pool.shutdownNow()
                withClue(s"retention $retention ms: generations handed out: ") { assert(dirs.size > 1) }
                withClue(s"retention $retention ms: copies made against generation dirs handed out ${dirs.size}: ") {
                    assert(NodeFilePathUtil.copyCount(remote) == dirs.size)
                }
                withClue(s"retention $retention ms: generation dirs still on disk: ") {
                    assert(eventually(5000)(dirs.asScala.forall(!Files.exists(_))), dirs.asScala.filter(Files.exists(_)))
                }
            } finally {
                deleteTree(root)
                deleteTree(staging)
            }
        }
    }

    // ---- idle retention -----------------------------------------------------------------------------

    test("retention 0: five acquire / close cycles of one remote copy it five times") {
        withRemoteFile("ret_a.tif", "a" * 1024) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            (1 to 5).foreach(_ => NodeFileManager.acquire(remote).close())
            withClue(s"copies made for $remote: ") { assert(NodeFilePathUtil.copyCount(remote) == 5) }
        }
    }

    test("retention on: five acquire / close cycles of one remote copy it once, and the idle copy stays") {
        NodeFilePathUtil.setRetentionMs(5000L)
        withRemoteFile("ret_b.tif", "b" * 1024) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val locals = (1 to 5).map { _ =>
                val lease = NodeFileManager.acquire(remote)
                try lease.localPath
                finally lease.close()
            }
            withClue(s"local paths of five cycles: ${locals.distinct}: ") { assert(locals.distinct.size == 1) }
            withClue(s"copies made for $remote: ") { assert(NodeFilePathUtil.copyCount(remote) == 1) }
            withClue(s"the idle copy ${locals.head}: ") {
                assert(Files.isRegularFile(Paths.get(locals.head)) && NodeFilePathUtil.readersOf(remote) == 0)
            }
            withClue("a by-remote release of an idle copy releases nothing: ") {
                assert(NodeFilePathUtil.releaseByRemote(remote) == 0 && Files.isRegularFile(Paths.get(locals.head)))
            }
            val again = NodeFileManager.acquire(remote)
            try {
                withClue(s"the reacquired copy ${again.localPath}: ") {
                    assert(again.localPath == locals.head && NodeFilePathUtil.readersOf(remote) == 1)
                }
            } finally again.close()
        }
    }

    test("retention on: an idle copy is deleted once the retention window passes") {
        NodeFilePathUtil.setRetentionMs(400L)
        withRemoteFile("ret_c.tif", "c" * 1024) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val lease = NodeFileManager.acquire(remote)
            val gen = Paths.get(lease.localPath).getParent
            lease.close()
            withClue(s"$gen right after the last release: ") { assert(Files.isDirectory(gen)) }
            withClue(s"$gen after the window: ") { assert(eventually(5000)(!Files.exists(gen))) }
            withClue(s"copies made for $remote, holders: ") {
                assert(NodeFilePathUtil.copyCount(remote) == 1 && NodeFilePathUtil.readersOf(remote) == 0)
            }
            val next = NodeFileManager.acquire(remote)
            try withClue(s"after expiry, ${next.localPath}: ") {
                assert(Paths.get(next.localPath).getParent != gen && NodeFilePathUtil.copyCount(remote) == 2)
            }
            finally next.close()
        }
    }

    test("retention on: a reused idle copy is kept while held and expires a full window after its last release") {
        NodeFilePathUtil.setRetentionMs(600L)
        withRemoteFile("ret_g.tif", "g" * 1024) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val first = NodeFileManager.acquire(remote)
            val local = Paths.get(first.localPath)
            first.close()
            Thread.sleep(400)
            val held = NodeFileManager.acquire(remote)
            try {
                withClue(s"the reacquired copy ${held.localPath}: ") {
                    assert(held.localPath == local.toString && NodeFilePathUtil.copyCount(remote) == 1)
                }
                Thread.sleep(500) // past the first idle period's deadline
                withClue(s"a held copy $local past the first deadline: ") { assert(Files.isRegularFile(local)) }
            } finally held.close()
            Thread.sleep(200)
            withClue(s"$local 200 ms into a fresh 600 ms window: ") { assert(Files.isRegularFile(local)) }
            withClue(s"${local.getParent} after the fresh window: ") { assert(eventually(5000)(!Files.exists(local.getParent))) }
        }
    }

    test("retention on: an idle copy of a remote rewritten in place is replaced, and the old copy deleted at once") {
        NodeFilePathUtil.setRetentionMs(60000L)
        withRemoteFile("ret_d.tif", "AAAA") { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val first = NodeFileManager.acquire(remote)
            val old = Paths.get(first.localPath)
            first.close()
            val t0 = Files.getLastModifiedTime(f).toMillis
            Files.write(f, "BBBB".getBytes(StandardCharsets.UTF_8)) // same length, newer mtime
            Files.setLastModifiedTime(f, FileTime.fromMillis(t0 + 2000))
            val next = NodeFileManager.acquire(remote)
            try {
                withClue(s"after the rewrite, ${next.localPath}: ") { assert(text(Paths.get(next.localPath)) == "BBBB") }
                withClue(s"copies made for $remote: ") { assert(NodeFilePathUtil.copyCount(remote) == 2) }
                withClue(s"the superseded idle generation ${old.getParent}: ") { assert(!Files.exists(old.getParent)) }
            } finally next.close()
        }
    }

    test("retention on: a remote replaced by a smaller file with the same mtime is served fresh") {
        NodeFilePathUtil.setRetentionMs(60000L)
        withRemoteFile("ret_e.geojson", "x" * 2048) { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val first = NodeFileManager.acquire(remote)
            val old = Paths.get(first.localPath)
            first.close()
            val t0 = Files.getLastModifiedTime(f)
            Files.write(f, "small".getBytes(StandardCharsets.UTF_8))
            Files.setLastModifiedTime(f, t0) // only the length tells the two apart
            val next = NodeFileManager.acquire(remote)
            try {
                withClue(s"after the smaller replacement, ${next.localPath}: ") { assert(text(Paths.get(next.localPath)) == "small") }
                withClue(s"the superseded idle generation ${old.getParent}: ") { assert(!Files.exists(old.getParent)) }
            } finally next.close()
        }
    }

    test("retention on: a superseded copy is deleted at its holder's last release, not kept idle") {
        NodeFilePathUtil.setRetentionMs(60000L)
        withRemoteFile("ret_h.tif", "AAAA") { f =>
            val remote = HadoopUtils.cleanPath(f.toString)
            val held = NodeFileManager.acquire(remote)
            val t0 = Files.getLastModifiedTime(f).toMillis
            Files.write(f, "BBBB".getBytes(StandardCharsets.UTF_8))
            Files.setLastModifiedTime(f, FileTime.fromMillis(t0 + 2000))
            val fresh = NodeFileManager.acquire(remote)
            val (a, b) = (Paths.get(held.localPath), Paths.get(fresh.localPath))
            withClue(s"the old holder's copy $a, the new copy $b: ") { assert(a != b && text(a) == "AAAA" && text(b) == "BBBB") }
            held.close()
            withClue(s"the superseded generation ${a.getParent} after its last release: ") { assert(!Files.exists(a.getParent)) }
            fresh.close()
            withClue(s"the current generation ${b.getParent} after its last release: ") { assert(Files.isRegularFile(b)) }
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

    private def text(p: Path): String = new String(Files.readAllBytes(p), StandardCharsets.UTF_8)

    /** Polls `cond` until it holds or `timeoutMs` passes; returns its last value. */
    private def eventually(timeoutMs: Long)(cond: => Boolean): Boolean = {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
        var ok = cond
        while (!ok && System.nanoTime() < deadline) {
            Thread.sleep(20)
            ok = cond
        }
        ok
    }

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
