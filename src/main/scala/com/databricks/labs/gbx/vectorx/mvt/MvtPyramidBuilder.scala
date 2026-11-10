package com.databricks.labs.gbx.vectorx.mvt

import com.databricks.labs.gbx.rasterx.tile.TileMath
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory}

import scala.collection.mutable.ArrayBuffer

/** Helper that fans a sequence of `(geom_wkb, attrs)` features out across a zoom range and
 *  encodes one Mapbox Vector Tile (MVT) per intersecting `(z, x, y)`.
 *
 *  The input geometries are assumed to be in EPSG:4326 lon/lat — callers must reproject any
 *  other CRS upstream. Per tile, each feature is clipped against the tile envelope (in lon/lat),
 *  the surviving geometry is affine-transformed to MVT tile-local coordinates (`[0, extent]`,
 *  origin upper-left, Y flipped) and handed to [[MvtWriter.encode]] which wraps the GDAL OGR
 *  MVT driver.
 *
 *  Pairs with `gbx_rst_xyzpyramid` (the Python level) — the raster
 *  sibling that explodes one raster across the same zoom range. Output rows from both feed
 *  directly into the PMTiles encoder for end-to-end vector or raster publishing.
 *
 *  Pure, stateless object — no Spark, no GDAL globals here (GDAL native is loaded lazily by
 *  [[MvtWriter.encode]]).
 */
object MvtPyramidBuilder {

    /** Cap on total emitted tiles across the requested zoom range. Mirrors the tile cap of `gbx_rst_xyzpyramid` —
     *  prevents accidental fan-outs (a tiny extent at z=20 is still fine; a global extent at
     *  z=10+ blows up quickly).
     */
    val MaxTileCount: Long = 1000000L

    /**
     *  Build `(z, x, y, mvt_bytes)` tiles for a sequence of `(geom_wkb, attrs)` features across
     *  the inclusive zoom range `[minZ, maxZ]`.
     *
     *  @param features  Per-feature pairs of `(geom_wkb_bytes, attrs_map)`. Geometries are
     *                   assumed to be in EPSG:4326 lon/lat. Null / empty / unparseable WKBs are
     *                   silently skipped (consistent with `MvtWriter.encode`).
     *  @param minZ      Inclusive minimum zoom level (>= 0).
     *  @param maxZ      Inclusive maximum zoom level (>= minZ, <= [[TileMath.MAX_ZOOM]]).
     *  @param layerName MVT layer name (e.g. "roads").
     *  @param extent    MVT tile extent in pixels; defaults to [[MvtWriter.DefaultExtent]] (4096).
     *  @return Array of `(z, x, y, mvt_bytes)` tuples; tiles with no surviving features after
     *          clipping are omitted (no empty MVT rows emitted).
     */
    def build(
        features: Iterable[(Array[Byte], Map[String, Any])],
        minZ: Int,
        maxZ: Int,
        layerName: String,
        extent: Int = MvtWriter.DefaultExtent
    ): Array[(Int, Int, Int, Array[Byte])] = {
        require(minZ >= 0, s"gbx_st_asmvt_pyramid: min_z must be >= 0; got $minZ")
        require(maxZ >= minZ, s"gbx_st_asmvt_pyramid: max_z ($maxZ) must be >= min_z ($minZ)")
        require(
          maxZ <= TileMath.MAX_ZOOM,
          s"gbx_st_asmvt_pyramid: max_z must be <= ${TileMath.MAX_ZOOM}; got $maxZ"
        )

        // Parse and accumulate the union bbox in lon/lat once.
        val parsed: Seq[(Geometry, Map[String, Any])] = features.toSeq.flatMap { case (wkb, attrs) =>
            if (wkb == null || wkb.isEmpty) None
            else {
                val g = try { JTS.fromWKB(wkb) } catch { case _: Throwable => null }
                if (g == null || g.isEmpty) None else Some((g, attrs))
            }
        }
        if (parsed.isEmpty) return Array.empty

        val unionEnv = new Envelope()
        parsed.foreach { case (g, _) => unionEnv.expandToInclude(g.getEnvelopeInternal) }
        if (unionEnv.isNull) return Array.empty

        // Cell-count guard — same shape as gbx_rst_xyzpyramid's.
        var total: Long = 0L
        var zg = minZ
        while (zg <= maxZ) {
            total += TileMath.intersectingTileCount(
              unionEnv.getMinX, unionEnv.getMinY, unionEnv.getMaxX, unionEnv.getMaxY, zg
            )
            if (total > MaxTileCount) {
                throw new IllegalArgumentException(
                  s"gbx_st_asmvt_pyramid: tile-count across zoom range [$minZ, $maxZ] exceeds " +
                  s"$MaxTileCount (feature extent is too large for that pyramid depth). " +
                  s"Lower max_z, or pre-filter the features before pyramidizing."
                )
            }
            zg += 1
        }

        val factory = new GeometryFactory()
        val out = new ArrayBuffer[(Int, Int, Int, Array[Byte])](math.min(total, Int.MaxValue.toLong).toInt)

        var z = minZ
        while (z <= maxZ) {
            val tiles = TileMath.intersectingTiles(
              unionEnv.getMinX, unionEnv.getMinY, unionEnv.getMaxX, unionEnv.getMaxY, z
            )
            var i = 0
            while (i < tiles.length) {
                val (zi, xi, yi) = tiles(i)
                val (mx0, my0, mx1, my1) = TileMath.tileBboxWebMerc(zi, xi, yi)
                // tileBboxWebMerc returns EPSG:3857 metres; clip in lon/lat so convert corners.
                val (lonMin, latMin) = webMercToLonLat(mx0, my0)
                val (lonMax, latMax) = webMercToLonLat(mx1, my1)
                val tileEnv = factory.toGeometry(new Envelope(lonMin, lonMax, latMin, latMax))

                val clipped = parsed.flatMap { case (g, attrs) =>
                    val inter =
                        try { g.intersection(tileEnv) } catch { case _: Throwable => null }
                    if (inter == null || inter.isEmpty) None
                    else Some((JTS.toWKB(toTileLocal(inter, lonMin, latMin, lonMax, latMax, extent)), attrs))
                }
                if (clipped.nonEmpty) {
                    val bytes = MvtWriter.encode(layerName, extent, clipped)
                    if (bytes != null && bytes.nonEmpty) out += ((zi, xi, yi, bytes))
                }
                i += 1
            }
            z += 1
        }
        out.toArray
    }

    /** Affine transform: the per-tile lon/lat clip is remapped into MVT tile-local pixel space
     *  `[0, extent]` for this tile (origin upper-left, y-down), mirroring the light pyramid's
     *  `_to_tile_local`. [[MvtWriter.encode]] takes that tile-local geometry and handles the
     *  `[0,extent]`→web-mercator world mapping the OGR driver needs internally, so both tiers
     *  share one coordinate contract.
     *
     *    sx = extent / (lonMax - lonMin) ; tile-local x = (lon - lonMin) * sx
     *    sy = extent / (latMax - latMin) ; tile-local y = (latMax - lat) * sy   // y flipped (top=0)
     *
     *  Mutates a defensive copy of the input geometry; the original is left alone.
     */
    private def toTileLocal(
        g: Geometry,
        lonMin: Double,
        latMin: Double,
        lonMax: Double,
        latMax: Double,
        extent: Int
    ): Geometry = {
        val sx = extent.toDouble / (lonMax - lonMin)
        val sy = extent.toDouble / (latMax - latMin)
        val transformed = g.copy()
        val coords = transformed.getCoordinates
        var i = 0
        while (i < coords.length) {
            val c = coords(i)
            val lon = c.x
            val lat = c.y
            c.x = (lon - lonMin) * sx
            c.y = (latMax - lat) * sy
            i += 1
        }
        transformed.geometryChanged()
        transformed
    }

    /** WGS84 semi-major axis in metres (web-mercator sphere radius). */
    private val R: Double = 6378137.0
    private val Rad2Deg: Double = 180.0 / math.Pi

    /** Inverse Pseudo-Mercator transform (EPSG:3857 metres to lon/lat degrees). */
    private def webMercToLonLat(x: Double, y: Double): (Double, Double) = {
        val lon = (x / R) * Rad2Deg
        val lat = (2.0 * math.atan(math.exp(y / R)) - math.Pi / 2.0) * Rad2Deg
        (lon, lat)
    }
}
