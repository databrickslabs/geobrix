package com.databricks.labs.gbx.vectorx.jts

import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, LineString}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class InterpolateElevationTest extends AnyFunSuite {

    /** z = 2*x + 3*y + 5 sampled at the 4 corners of a 100x100 extent. */
    private def planePoints() = Seq(
        JTS.point(new Coordinate(0.0,   0.0,   2 * 0.0   + 3 * 0.0   + 5)),
        JTS.point(new Coordinate(100.0, 0.0,   2 * 100.0 + 3 * 0.0   + 5)),
        JTS.point(new Coordinate(0.0,   100.0, 2 * 0.0   + 3 * 100.0 + 5)),
        JTS.point(new Coordinate(100.0, 100.0, 2 * 100.0 + 3 * 100.0 + 5))
    )

    test("pointGridBBox emits widthPx*heightPx cell centers inside the extent") {
        val grid = InterpolateElevation.pointGridBBox(0.0, 0.0, 100.0, 100.0, 10, 10, 32633)
        grid.getNumGeometries shouldBe 100
        val p0 = grid.getGeometryN(0)
        p0.getCoordinate.x shouldBe 5.0 +- 1e-9
        p0.getCoordinate.y shouldBe 5.0 +- 1e-9
    }

    test("interpolate reproduces a planar surface exactly (linear TIN)") {
        val mp = JTS.multiPoint(planePoints().toArray)
        val grid = InterpolateElevation.pointGridBBox(0.0, 0.0, 100.0, 100.0, 10, 10, 32633)
        val out = InterpolateElevation.interpolate(mp, Seq.empty[LineString], grid, 0.0, 0.0)
        out should not be empty
        out.foreach { p =>
            val expected = 2 * p.getX + 3 * p.getY + 5
            p.getCoordinate.getZ shouldBe expected +- 1e-6
        }
    }

    test("interpolate skips (does not throw on) points outside the convex hull") {
        val mp = JTS.multiPoint(planePoints().toArray)
        val grid = InterpolateElevation.pointGridBBox(-50.0, -50.0, 150.0, 150.0, 20, 20, 32633)
        val out = InterpolateElevation.interpolate(mp, Seq.empty[LineString], grid, 0.0, 0.0)
        out.size should be > 0  // interior (in-hull) cells still interpolate; out-of-hull skipped, not thrown
    }

    // ====== the shared pre-step and the conforming mode ======

    private val gf = new GeometryFactory()

    private def plane(x: Double, y: Double): Double = 2.0 * x + y + 1.0

    /** The corners of a 10 m square and its centre, on the plane z = 2x + y + 1. */
    private def squarePoints() = JTS.multiPoint(
      Seq((0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (5.0, 5.0))
          .map { case (x, y) => JTS.point(new Coordinate(x, y, plane(x, y))) }
          .toArray
    )

    private def line(cs: (Double, Double, Double)*): Geometry =
        gf.createLineString(cs.map { case (x, y, z) => new Coordinate(x, y, z) }.toArray)

    private def line2d(cs: (Double, Double)*): Geometry =
        gf.createLineString(cs.map { case (x, y) => new Coordinate(x, y) }.toArray)

    /** The XY of every triangle vertex that is not an input coordinate. */
    private def steiner(tris: Seq[Geometry], inputs: Set[(Double, Double)]): Set[(Double, Double)] =
        tris.flatMap(_.getCoordinates.map(c => (c.x, c.y))).toSet -- inputs

    private val squareXY = Set((0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (5.0, 5.0))

    test("conforming: split_point_finder takes effect (MIDPOINT and NONENCROACHING place different points)") {
        val bl = Seq(line((0.0, 1.0, plane(0, 1)), (10.0, 9.5, plane(10, 9.5))))
        val inputs = squareXY ++ Set((0.0, 1.0), (10.0, 9.5))
        def run(f: TriangulationSplitPointTypeEnum.Value) =
            InterpolateElevation.triangulate(squarePoints(), bl, 0.0, 0.0, Some(f), "conforming")
        val mid = run(TriangulationSplitPointTypeEnum.MIDPOINT)
        val non = run(TriangulationSplitPointTypeEnum.NONENCROACHING)
        // MIDPOINT halves the segment towards the centre point until no half's circle holds it
        steiner(mid, inputs) shouldBe
            Set((5.0, 5.25), (2.5, 3.125), (3.75, 4.1875), (4.375, 4.71875), (4.6875, 4.984375), (4.84375, 5.1171875))
        steiner(non, inputs) shouldBe Set((4.8766328011611035, 5.145137880986938))
        // the default (no finder) is JTS's own, NONENCROACHING
        steiner(InterpolateElevation.triangulate(squarePoints(), bl, 0.0, 0.0, None, "conforming"), inputs) shouldBe
            steiner(non, inputs)
    }

    test("conforming: a split point takes the Z of the break line it splits") {
        val bl = Seq(line((0.0, 1.0, plane(0, 1)), (10.0, 9.5, plane(10, 9.5))))
        for (f <- Seq(TriangulationSplitPointTypeEnum.MIDPOINT, TriangulationSplitPointTypeEnum.NONENCROACHING)) {
            val tris = InterpolateElevation.triangulate(squarePoints(), bl, 0.0, 0.0, Some(f), "conforming")
            tris.flatMap(_.getCoordinates).foreach(c => c.getZ shouldBe plane(c.x, c.y) +- 1e-12)
        }
    }

    test("conforming: a 2D break line has Z 0, as in the constrained mode") {
        val kite = JTS.multiPoint(
          Seq((0.0, 0.0), (10.0, 0.0), (5.0, 1.0), (5.0, -1.0)).map { case (x, y) => JTS.point(new Coordinate(x, y, 3.0)) }.toArray
        )
        val bl = Seq(line2d((0.0, 0.0), (10.0, 0.0)))
        for (m <- Seq("conforming", "constrained")) {
            val zs = InterpolateElevation.triangulate(kite, bl, 0.0, 0.0, None, m).flatMap(_.getCoordinates).map(_.getZ)
            zs.exists(_.isNaN) shouldBe false
            zs.min shouldBe 0.0
        }
    }

    test("conforming: crossing break lines raise with a Fix") {
        // two break lines crossing at a shallow angle away from any vertex: the split points chase the crossing
        val box = JTS.multiPoint(
          Seq((0.0, -5.0), (10.0, -5.0), (10.0, 5.0), (0.0, 5.0)).map { case (x, y) => JTS.point(new Coordinate(x, y, 0.0)) }.toArray
        )
        val bl = Seq(line((1.0, -0.5, 0.0), (9.0, 0.5, 0.0)), line((2.0, 0.5, 0.0), (8.63, -0.5, 0.0)))
        val e = intercept[IllegalArgumentException] {
            InterpolateElevation.triangulate(box, bl, 0.0, 0.0, None, "conforming")
        }
        e.getMessage should include("Fix: split crossing break lines where they cross")
    }

    /** The case whose old post-triangulation snap made a zero-area triangle in both modes: (5.007, 4.993) is 0.0099
     *  from the diagonal break line, so it was moved onto the line AFTER triangulating. */
    private def sliverPoints() = JTS.multiPoint(
      Seq((0.0, 0.0), (10.0, 10.0), (5.007, 4.993), (4.3, 5.7)).map { case (x, y) => JTS.point(new Coordinate(x, y, 0.0)) }.toArray
    )

    test("snap before triangulating: no zero-area triangle, and the point becomes a break line vertex") {
        val bl = Seq(line((0.0, 0.0, 0.0), (10.0, 10.0, 20.0)))
        for (m <- Seq("constrained", "conforming")) {
            val tris = InterpolateElevation.triangulate(sliverPoints(), bl, 0.0, 0.05, None, m)
            tris should not be empty
            tris.foreach(_.getArea should be > 0.0)
            val vs = tris.flatMap(_.getCoordinates).toSeq
            vs.exists(c => math.abs(c.x - 5.0) < 1e-9 && math.abs(c.y - 5.0) < 1e-9 && math.abs(c.getZ - 10.0) < 1e-9) shouldBe
                true
            vs.exists(_.x == 5.007) shouldBe false
        }
    }

    test("snap: a horizontal and a vertical break line are found (the lookup grows by the tolerance)") {
        val pts = JTS.multiPoint(
          Seq((0.0, -5.0), (10.0, -5.0), (10.0, 5.0), (0.0, 5.0), (5.0, 0.01), (-3.0, 2.0))
              .map { case (x, y) => JTS.point(new Coordinate(x, y, 0.0)) }
              .toArray
        )
        val bl = Seq(line((1.0, 0.0, 7.0), (9.0, 0.0, 7.0)), line((-2.99, -4.0, 9.0), (-2.99, 4.0, 9.0)))
        val (kept, lines) = InterpolateElevation.prepareSites(pts, bl, 0.0, 0.05)
        kept.map(c => (c.x, c.y)).toSet.intersect(Set((5.0, 0.01), (-3.0, 2.0))) shouldBe empty
        lines(0).map(c => (c.x, c.y, c.getZ)) shouldBe Array((1.0, 0.0, 7.0), (5.0, 0.0, 7.0), (9.0, 0.0, 7.0))
        lines(1).map(c => (c.x, c.y, c.getZ)) shouldBe Array((-2.99, -4.0, 9.0), (-2.99, 2.0, 9.0), (-2.99, 4.0, 9.0))
    }

    test("snap: the nearest line wins, and two points landing on one spot merge into the first") {
        val pts = JTS.multiPoint(
          Seq((5.0, 0.03), (5.0, 1.0), (2.0, 0.0), (7.0, 0.02), (7.0, -0.02), (0.01, 0.01))
              .map { case (x, y) => JTS.point(new Coordinate(x, y, 0.0)) }
              .toArray
        )
        val bl = Seq(
          line((0.0, 0.0, 1.0), (10.0, 0.0, 1.0)), // y = 0
          line((0.0, 0.05, 2.0), (10.0, 0.05, 2.0)), // y = 0.05: (5, 0.03) is nearer to it
          line((0.0, 2.0, 3.0), (10.0, 2.0, 3.0))
        )
        val (kept, lines) = InterpolateElevation.prepareSites(pts, bl, 0.001, 0.05)
        // (5, 0.03) is nearer y = 0.05; (5, 1) is out of reach; (2, 0) lies on y = 0 and on the edge of the reach
        // of y = 0.05, so the nearer wins; (7, 0.02) and (7, -0.02) both land on (7, 0), so the second merges into the
        // first; (0.01, 0) is 0.01 from (0, 0), over merge_tolerance, so it is a vertex of its own
        kept.map(c => (c.x, c.y)).toSeq shouldBe Seq((5.0, 1.0))
        lines(0).map(c => (c.x, c.y, c.getZ)).toSeq shouldBe
            Seq((0.0, 0.0, 1.0), (0.01, 0.0, 1.0), (2.0, 0.0, 1.0), (7.0, 0.0, 1.0), (10.0, 0.0, 1.0))
        lines(1).map(c => (c.x, c.y, c.getZ)).toSeq shouldBe Seq((0.0, 0.05, 2.0), (5.0, 0.05, 2.0), (10.0, 0.05, 2.0))
        lines(2).length shouldBe 2
    }

    test("snap: an exact tie between two lines goes to the first in input order") {
        val pts = JTS.multiPoint(Array(JTS.point(new Coordinate(5.0, 0.025, 0.0)), JTS.point(new Coordinate(0.0, 9.0, 0.0)),
          JTS.point(new Coordinate(9.0, 9.0, 0.0))))
        val bl = Seq(line((0.0, 0.05, 2.0), (10.0, 0.05, 2.0)), line((0.0, 0.0, 1.0), (10.0, 0.0, 1.0)))
        val (_, lines) = InterpolateElevation.prepareSites(pts, bl, 0.0, 0.05)
        lines(0).length shouldBe 3
        lines(1).length shouldBe 2
    }

    test("snap: a projection within merge_tolerance of a line vertex merges into it") {
        val pts = JTS.multiPoint(Array(JTS.point(new Coordinate(9.9995, 0.01, 0.0))))
        val bl = Seq(line((0.0, 0.0, 1.0), (10.0, 0.0, 1.0)))
        val (kept, lines) = InterpolateElevation.prepareSites(pts, bl, 0.001, 0.05)
        kept shouldBe empty
        lines(0).length shouldBe 2
        // with merge_tolerance 0 the same point is a new vertex
        InterpolateElevation.prepareSites(pts, bl, 0.0, 0.05)._2(0).length shouldBe 3
    }

    test("prepareSites takes each part of a multi-line break line and each ring of a polygon, in order") {
        val pts = JTS.multiPoint(Array(JTS.point(new Coordinate(50.0, 50.0, 0.0))))
        val mls = gf.createMultiLineString(Array(
          gf.createLineString(Array(new Coordinate(0.0, 0.0, 1.0), new Coordinate(5.0, 0.0, 1.0))),
          gf.createLineString(Array(new Coordinate(0.0, 5.0, 2.0), new Coordinate(5.0, 5.0, 2.0)))
        ))
        val shell = Array((10.0, 10.0), (20.0, 10.0), (20.0, 20.0), (10.0, 20.0), (10.0, 10.0)).map { case (x, y) => new Coordinate(x, y, 3.0) }
        val hole = Array((12.0, 12.0), (12.0, 14.0), (14.0, 14.0), (12.0, 12.0)).map { case (x, y) => new Coordinate(x, y, 4.0) }
        val poly = gf.createPolygon(gf.createLinearRing(shell), Array(gf.createLinearRing(hole)))
        val (_, lines) = InterpolateElevation.prepareSites(pts, Seq(mls, poly), 0.0, 0.0)
        lines.map(_.map(c => (c.x, c.y, c.getZ)).toSeq) shouldBe Seq(
          Seq((0.0, 0.0, 1.0), (5.0, 0.0, 1.0)),
          Seq((0.0, 5.0, 2.0), (5.0, 5.0, 2.0)),
          shell.map(c => (c.x, c.y, 3.0)).toSeq,
          hole.map(c => (c.x, c.y, 4.0)).toSeq
        )
    }

    test("conforming: a point location that does not converge raises with a Fix") {
        // 50 points and 9 crossing break lines on which JTS's incremental triangulation fails to locate a point
        val rows = scala.io.Source.fromResource("text/tin_locate_failure.txt").getLines().filterNot(_.startsWith("#")).toSeq
        def coords(r: String) = r.split(" ").drop(1).map(_.toDouble).grouped(3).map(a => new Coordinate(a(0), a(1), a(2))).toArray
        val pts = JTS.multiPoint(rows.filter(_.startsWith("P ")).map(r => JTS.point(coords(r).head)).toArray)
        val bl = rows.filter(_.startsWith("L ")).map(r => gf.createLineString(coords(r)))
        val e = intercept[IllegalArgumentException] {
            InterpolateElevation.triangulate(pts, bl, 0.0, 0.0, None, "conforming")
        }
        e.getMessage should include("locating a point did not converge")
        e.getMessage should include("Fix: split crossing break lines where they cross")
    }

    test("merge_tolerance 0 merges only the same X and Y, at any magnitude") {
        // a 1e-12 merge grid once overflowed above about 9.2e6, so distant points merged
        val far = JTS.multiPoint(
          Seq((2.0e7, 1.0e7, 1.0), (2.0e7 + 4.0, 1.0e7, 2.0), (2.0e7, 1.0e7, 3.0), (0.0, 1.0, 4.0), (-0.0, 1.0, 5.0))
              .map { case (x, y, z) => JTS.point(new Coordinate(x, y, z)) }
              .toArray
        )
        val bl = Seq(line((5.0e6, 0.0, 0.0), (5.0e6, 1.0, 0.0)))
        val (kept, _) = InterpolateElevation.prepareSites(far, bl, 0.0, 0.0)
        kept.map(_.getZ).toSeq shouldBe Seq(1.0, 2.0, 4.0)
    }

    test("triangulate takes the mode word in any case, as the Python level does") {
        val bl = Seq(line((0.0, 1.0, plane(0, 1)), (10.0, 9.5, plane(10, 9.5))))
        def tris(mode: String) =
            InterpolateElevation.triangulate(squarePoints(), bl, 0.0, 0.0, None, mode).map(_.toText).sorted
        tris("CONFORMING") shouldBe tris("conforming")
        tris("Constrained") shouldBe tris("constrained")
        tris("CONFORMING") should not be tris("constrained")
        val e = intercept[IllegalArgumentException](tris("Sideways"))
        e.getMessage shouldBe "mode must be 'constrained' or 'conforming'; got 'Sideways'"
        // a null mode is the same argument error, not a NullPointerException
        intercept[IllegalArgumentException](tris(null)).getMessage shouldBe
            "mode must be 'constrained' or 'conforming'; got 'null'"
    }

    // ====== the one merge, and the constrained recovery ======

    private def pts(cs: (Double, Double, Double)*) =
        JTS.multiPoint(cs.map { case (x, y, z) => JTS.point(new Coordinate(x, y, z)) }.toArray)

    private def edges(tris: Seq[Geometry]): Set[Set[(Double, Double)]] =
        tris.flatMap { t =>
            val cs = t.getCoordinates.take(3).map(c => (c.x, c.y))
            Seq(Set(cs(0), cs(1)), Set(cs(1), cs(2)), Set(cs(2), cs(0)))
        }.toSet

    /** Each segment of each prepared line is a triangle edge. */
    private def enforced(tris: Seq[Geometry], lines: Seq[Array[Coordinate]]): Boolean =
        lines.forall(l => l.sliding(2).forall(s => edges(tris).contains(Set((s(0).x, s(0).y), (s(1).x, s(1).y)))))

    // The snapped point lands at (0.45, 0.45), 1.27 from the line's first vertex, so it is a vertex of the line. A
    // second, grid-cell merge once dropped it (both points round to the cell (0, 0)), so neither segment was enforced.
    private val diagLine = Seq(line((-0.45, -0.45, 0.0), (10.0, 10.0, 10.0)))
    private def diagPts() = pts((0.46, 0.44, 0.0), (10.0, -5.0, 0.0), (-5.0, 10.0, 0.0), (12.0, 0.0, 0.0), (0.0, 12.0, 0.0))

    test("constrained keeps a snapped vertex and makes both of its segments edges") {
        val (_, lines) = InterpolateElevation.prepareSites(diagPts(), diagLine, 1.0, 0.05)
        lines(0).length shouldBe 3
        val tris = InterpolateElevation.triangulate(diagPts(), diagLine, 1.0, 0.05, None, "constrained")
        enforced(tris, lines) shouldBe true
    }

    test("the merge is first-wins, a line vertex winning over a mass point") {
        // (0.9, 0) merges into the line's (0, 0); (3.6, 0) joins (3, 0), 0.6 away, where 1 m grid cells split them
        val bl = Seq(line((0.0, 0.0, 5.0), (0.0, 10.0, 5.0)))
        val (kept, lines) =
            InterpolateElevation.prepareSites(pts((0.9, 0.0, 1.0), (3.0, 0.0, 1.0), (3.6, 0.0, 1.0), (8.0, 8.0, 1.0)), bl, 1.0, 0.0)
        kept.map(c => (c.x, c.y, c.getZ)).toSeq shouldBe Seq((3.0, 0.0, 1.0), (8.0, 8.0, 1.0))
        lines(0).map(c => (c.x, c.y, c.getZ)).toSeq shouldBe Seq((0.0, 0.0, 5.0), (0.0, 10.0, 5.0))
    }

    test("a line vertex merges into the nearest kept vertex; a line merged into one coordinate is a mass point") {
        val bl = Seq(
          line((0.0, 0.0, 1.0), (5.0, 0.0, 1.0)),
          line((5.3, 0.4, 2.0), (9.0, 4.0, 2.0)),
          line((20.0, 20.0, 3.0), (20.2, 20.0, 3.0))
        )
        val (kept, lines) = InterpolateElevation.prepareSites(pts(), bl, 0.5, 0.0)
        lines.map(_.map(c => (c.x, c.y, c.getZ)).toSeq) shouldBe Seq(
          Seq((0.0, 0.0, 1.0), (5.0, 0.0, 1.0)),
          Seq((5.0, 0.0, 1.0), (9.0, 4.0, 2.0))
        )
        kept.map(c => (c.x, c.y, c.getZ)).toSeq shouldBe Seq((20.0, 20.0, 3.0))
    }

    test("turning snap on changes no merge where nothing snaps, in both modes") {
        val p = pts((0.4, 0.0, 1.0), (-0.4, 0.0, 2.0), (5.0, 5.0, 0.0), (-5.0, 5.0, 0.0), (0.0, -5.0, 0.0))
        val bl = Seq(line((100.0, 0.0, 0.0), (100.0, 10.0, 0.0)))
        for (m <- Seq("constrained", "conforming")) {
            def vs(snap: Double) =
                InterpolateElevation.triangulate(p, bl, 1.0, snap, None, m).flatMap(_.getCoordinates).map(c => (c.x, c.y, c.getZ)).toSet
            vs(0.0) shouldBe vs(1e-9)
            vs(0.0) should contain((0.4, 0.0, 1.0))
            vs(0.0).exists(_._1 == -0.4) shouldBe false
        }
    }

    test("constrained: crossing break lines raise with a Fix, the same text as the Python level") {
        val box = pts((0.0, -5.0, 0.0), (10.0, -5.0, 0.0), (10.0, 5.0, 0.0), (0.0, 5.0, 0.0), (5.0, 3.0, 0.0), (5.0, -3.0, 0.0))
        val bl = Seq(line((1.0, -1.0, 0.0), (9.0, 1.0, 0.0)), line((1.0, 1.0, 0.0), (9.0, -1.0, 0.0)))
        val e = intercept[IllegalArgumentException] {
            InterpolateElevation.triangulate(box, bl, 0.0, 0.0, None, "constrained")
        }
        e.getMessage shouldBe InterpolateElevation.ConstrainedCrossing
        e.getMessage should include("Fix: split the break lines where they cross")
    }

    test("constrained: a point location that does not converge raises with a Fix, the same text as the Python level") {
        // 12 sites within a few units of one line: JTS's DelaunayTriangulationBuilder fails to locate a point
        val near = pts(
          (1.705614180421661, 0.3556616138089565, 0.0),
          (2.1370135135414468, -0.03827484299550774, 0.0),
          (3.6849469793638447, -1.451785198900048, 0.0),
          (4.164986438772758, -1.8901378555866342, 0.0),
          (6.171084233788748, -3.7220253822936145, 0.0),
          (7.783777556195663, -5.194671825084469, 0.0),
          (8.358689901978432, -5.719658571895044, 0.0),
          (9.510282923560982, -6.771246833105947, 0.0),
          (9.552398189041536, -6.809704793485267, 0.0),
          (13.445510021916174, -10.364737366890585, 0.0),
          (15.91439202650987, -12.61922075267712, 0.0),
          (22.65232113531493, -18.77202562635853, 0.0)
        )
        val e = intercept[IllegalArgumentException] {
            InterpolateElevation.triangulate(near, Seq.empty, 0.0, 0.0, None, "constrained")
        }
        e.getMessage shouldBe InterpolateElevation.ConstrainedLocateFailed
        e.getCause shouldBe a[org.locationtech.jts.triangulate.quadedge.LocateFailureException]
    }

    test("constrained: a point exactly on a break line splits it, and every segment is an edge") {
        // a 3 x 3 grid with a break line along its middle row through (5, 5)
        val grid = pts((for (x <- Seq(0.0, 5.0, 10.0); y <- Seq(0.0, 5.0, 10.0)) yield (x, y, 0.0)): _*)
        val bl = Seq(line((0.0, 5.0, 1.0), (10.0, 5.0, 1.0)))
        val tris = InterpolateElevation.triangulate(grid, bl, 0.0, 0.0, None, "constrained")
        edges(tris) should contain(Set((0.0, 5.0), (5.0, 5.0)))
        edges(tris) should contain(Set((5.0, 5.0), (10.0, 5.0)))
        tris.map(_.getArea).sum shouldBe 100.0 +- 1e-9
    }

    test("constrained: every segment of long break lines through many points is an edge") {
        val rnd = new scala.util.Random(7)
        val p = pts((0 until 400).map(_ => (rnd.nextDouble() * 100, rnd.nextDouble() * 100, 0.0)): _*)
        val bl = (0 until 3).map(k => line((0 until 6).map(i => (i * 20.0, 15.0 + 33.0 * k + rnd.nextDouble() * 5, 1.0)): _*))
        val (_, lines) = InterpolateElevation.prepareSites(p, bl, 0.0, 0.0)
        enforced(InterpolateElevation.triangulate(p, bl, 0.0, 0.0, None, "constrained"), lines) shouldBe true
    }

    test("pointToSegment and project on a zero-length segment measure to and return its point") {
        InterpolateElevation.pointToSegment(3.0, 4.0, 0.0, 0.0, 0.0, 0.0) shouldBe 5.0
        val a = new Coordinate(1.0, 1.0, 7.0)
        val (r, q) = InterpolateElevation.project(new Coordinate(4.0, 5.0, 0.0), a, new Coordinate(1.0, 1.0, 9.0))
        r shouldBe 0.0
        (q eq a) shouldBe true
    }

    test("snap inserts a point on a later segment of a polyline, between that segment's ends") {
        val bl = Seq(line((0.0, 0.0, 0.0), (10.0, 0.0, 10.0), (10.0, 10.0, 20.0)))
        val (kept, lines) = InterpolateElevation.prepareSites(pts((10.02, 4.0, 0.0), (20.0, 20.0, 0.0)), bl, 0.0, 0.05)
        kept.map(c => (c.x, c.y)).toSeq shouldBe Seq((20.0, 20.0))
        lines(0).map(c => (c.x, c.y, c.getZ)).toSeq shouldBe
            Seq((0.0, 0.0, 0.0), (10.0, 0.0, 10.0), (10.0, 4.0, 14.0), (10.0, 10.0, 20.0))
    }

    test("a 2D mass point has Z 0") {
        val p = JTS.multiPoint(Seq((0.0, 0.0), (10.0, 0.0), (0.0, 10.0)).map { case (x, y) => JTS.point(new Coordinate(x, y)) }.toArray)
        for (m <- Seq("constrained", "conforming")) {
            InterpolateElevation.triangulate(p, Seq.empty, 0.0, 0.0, None, m).flatMap(_.getCoordinates).map(_.getZ).toSet shouldBe
                Set(0.0)
        }
    }

    test("the merge joins the nearest kept point, and the lesser (x, y) on an exact tie") {
        val bl = Seq(line((0.0, 0.0, 1.0), (0.0, 10.0, 1.0)), line((2.0, 0.0, 2.0), (2.0, 10.0, 2.0)))
        val near = InterpolateElevation.prepareSites(pts(), bl :+ line((1.2, 0.0, 9.0), (1.2, 5.0, 9.0)), 1.5, 0.0)._2
        near(2)(0).getZ shouldBe 2.0 // (1.2, 0) joins (2, 0), the nearer
        val tie = InterpolateElevation.prepareSites(pts(), bl :+ line((1.0, 0.0, 9.0), (1.0, 5.0, 9.0)), 1.5, 0.0)._2
        tie(2)(0).getZ shouldBe 1.0 // (1, 0) is 1 from both and joins (0, 0), the lesser X
    }

    test("a mass point near a snapped vertex merges into it, the line's coordinate winning") {
        val bl = Seq(line((0.0, 0.0, 0.0), (10.0, 0.0, 10.0)))
        val (kept, lines) =
            InterpolateElevation.prepareSites(pts((5.0, 0.05, 0.0), (5.5, 0.3, 0.0), (5.0, 8.0, 0.0)), bl, 1.0, 0.1)
        kept.map(c => (c.x, c.y)).toSeq shouldBe Seq((5.0, 8.0))
        lines(0).map(c => (c.x, c.y)).toSeq shouldBe Seq((0.0, 0.0), (5.0, 0.0), (10.0, 0.0))
    }

    test("each triangle ring is counter-clockwise from its lowest vertex, in both modes") {
        val gf = new GeometryFactory()
        // a clockwise triangle that starts at its highest vertex comes back counter-clockwise from (0, 0)
        val cw = gf.createPolygon(Array(new Coordinate(4.0, 4.0), new Coordinate(4.0, 0.0), new Coordinate(0.0, 0.0),
            new Coordinate(4.0, 4.0)))
        InterpolateElevation.canonicalTriangle(cw, gf).getCoordinates.map(c => (c.x, c.y)).toSeq shouldBe
            Seq((0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 0.0))
        // the exact sign where the floating-point determinant cannot tell
        InterpolateElevation.orientationSign(new Coordinate(0.1, 0.1), new Coordinate(0.2, 0.2),
            new Coordinate(0.30000000000000004, 0.30000000000000004)) shouldBe 0
        val grid = for (x <- 0 until 6; y <- 0 until 6) yield JTS.point(new Coordinate(x.toDouble, y.toDouble, 0.0))
        val bl = Seq(gf.createLineString(Array(new Coordinate(0.5, 0.3), new Coordinate(4.2, 3.7))))
        for (mode <- Seq("constrained", "conforming")) {
            val tris = InterpolateElevation.triangulate(JTS.multiPoint(grid.toArray), bl, 0.0, 0.0, None, mode)
            tris should not be empty
            tris.foreach { t =>
                val c = t.getCoordinates
                c.length shouldBe 4
                c.take(3).foreach(p => (p.x > c(0).x || (p.x == c(0).x && p.y >= c(0).y)) shouldBe true)
                InterpolateElevation.orientationSign(c(0), c(1), c(2)) shouldBe 1
            }
        }
    }
}
