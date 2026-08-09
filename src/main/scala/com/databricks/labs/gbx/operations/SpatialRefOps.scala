package com.databricks.labs.gbx.operations

import org.gdal.osr.{CoordinateTransformation, SpatialReference, osrConstants}

import scala.collection.mutable
import scala.util.Try

/** Helpers for OSR SpatialReference: EPSG code extraction and construction from EPSG code.
  * Tier-neutral home so both RasterX and VectorX expressions can depend on it without a
  * cross-package dependency. The `rasterx.operations.SpatialRefOps` object is a thin
  * forwarder to this one — all existing rasterx importers compile unchanged. */
object SpatialRefOps {

    // 120 WGS84 UTM zones + 4326/27700/3857 + headroom. A workload touching every
    // UTM zone plus the common CRSes never evicts.
    private val TRANSFORMER_CACHE_SIZE = 128

    // A CoordinateTransformation is NOT thread-safe for concurrent use, and executors
    // run multiple Spark tasks per JVM. One LRU cache per worker thread — reuse within
    // a thread, zero cross-thread contention (no lock on the hot path).
    private val txCache =
        new ThreadLocal[mutable.LinkedHashMap[String, CoordinateTransformation]] {
            override def initialValue(): mutable.LinkedHashMap[String, CoordinateTransformation] =
                mutable.LinkedHashMap.empty
        }

    /** Returns the EPSG authority code as Int, or 0 if not EPSG (e.g. ESRI). */
    def getEPSGCode(spatialRef: SpatialReference): Int = {
        // Try to get the PROJCS/GEOGCS authority code
        // Returns 0 if no EPSG authority is found (e.g., for ESRI projections like ESRI:54008)
        (spatialRef.GetAuthorityName(null), spatialRef.GetAuthorityCode(null)) match {
            case (name: String, code: String) if name == "EPSG" => code.toInt
            case _                                              => 0  // Default to 0 for non-EPSG projections
        }
    }

    /** Resolves a CRS string to a SpatialReference — the ONE place the heavy-tier
      * int-cast rule lives (mirrors the light `pyrx.core.crs.resolve_crs`):
      *   - an int-castable string (e.g. `"4326"`, `" 32633 "`) → `ImportFromEPSG(int)`;
      *   - otherwise → `SetFromUserInput(value)`, GDAL's universal parser that accepts
      *     `EPSG:x` / `ESRI:x` / WKT / PROJ4 / auth strings.
      * Throws IllegalArgumentException if the value cannot be resolved to a valid CRS. */
    def resolveCrs(value: String): SpatialReference = {
        require(value != null, "resolveCrs: CRS value is null")
        val trimmed = value.trim
        require(trimmed.nonEmpty, "resolveCrs: CRS value is empty")
        val sr = new SpatialReference()
        Try(trimmed.toInt).toOption match {
            case Some(epsg) =>
                // ImportFromEPSG auto-recovers ESRI codes (e.g. 54008 -> ESRI:54008), so an
                // int-castable string classifies as EPSG or ESRI. A code in neither authority
                // returns a non-zero OGRERR — but the GDAL Java binding may ALSO throw a native
                // RuntimeException before returning; normalise both into IllegalArgumentException.
                val rc = Try(sr.ImportFromEPSG(epsg)).recover {
                    case e: Throwable =>
                        sr.delete()
                        throw new IllegalArgumentException(
                          s"resolveCrs: $epsg is not a valid EPSG or ESRI code", e)
                }.get
                if (rc != 0) {
                    sr.delete()
                    throw new IllegalArgumentException(
                      s"resolveCrs: $epsg is not a valid EPSG or ESRI code (OGRERR=$rc)")
                }
            case None =>
                // SetFromUserInput returns a non-zero OGRERR for unparseable input, but
                // the GDAL Java binding may ALSO throw a native RuntimeException before
                // returning; normalise both into a clean IllegalArgumentException.
                val rc = Try(sr.SetFromUserInput(trimmed)).recover {
                    case e: Throwable =>
                        sr.delete()
                        throw new IllegalArgumentException(
                          s"resolveCrs: could not parse CRS '$value'", e)
                }.get
                if (rc != 0) {
                    sr.delete()
                    throw new IllegalArgumentException(
                      s"resolveCrs: could not parse CRS '$value' (OGRERR=$rc)")
                }
        }
        sr
    }

    /** Canonical CRS string for a SpatialReference (mirrors the light
      * `crs_to_canonical`): the authority string `NAME:CODE` (e.g. `EPSG:4326`,
      * `ESRI:54008`) when the CRS carries one, else the full WKT. Returns null for a
      * null SpatialReference. */
    def crsToCanonical(spatialRef: SpatialReference): String = {
        if (spatialRef == null) return null
        (spatialRef.GetAuthorityName(null), spatialRef.GetAuthorityCode(null)) match {
            case (name: String, code: String) if name != null && name.nonEmpty &&
                code != null && code.nonEmpty => s"$name:$code"
            case _ => spatialRef.ExportToWkt()
        }
    }

    /** Thread-local, LRU-bounded CoordinateTransformation keyed by canonical CRS pair.
      * Mirrors the light `crs.get_transformer`: equivalent spellings (`4326` /
      * `"EPSG:4326"`) resolve to the same canonical key and share one transformation.
      *
      * Resource discipline: every SpatialReference allocated here is deleted in a
      * try/finally. The CoordinateTransformation uses OAMS_TRADITIONAL_GIS_ORDER on
      * both SRs so JTS (x=lon, y=lat) input is not axis-flipped by GDAL 3+. */
    def getTransformer(srcKey: String, dstKey: String): CoordinateTransformation = {
        // Resolve canonical keys for cache deduplication, then release the SRs.
        val srcSR_key = resolveCrs(srcKey)
        val srcC = try crsToCanonical(srcSR_key) finally srcSR_key.delete()
        val dstSR_key = resolveCrs(dstKey)
        val dstC = try crsToCanonical(dstSR_key) finally dstSR_key.delete()

        val key = s"$srcC->$dstC"
        val cache = txCache.get()
        cache.get(key) match {
            case Some(tf) =>
                cache.remove(key); cache.put(key, tf) // move-to-end (most-recent)
                tf
            case None =>
                // Build CT with traditional axis order so JTS (x=lon, y=lat) input isn't flipped.
                val srcSR = resolveCrs(srcKey)
                val dstSR = resolveCrs(dstKey)
                srcSR.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
                dstSR.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
                val tf = new CoordinateTransformation(srcSR, dstSR)
                srcSR.delete()
                dstSR.delete()
                cache.put(key, tf)
                if (cache.size > TRANSFORMER_CACHE_SIZE) cache.remove(cache.head._1) // evict oldest
                tf
        }
    }

    /** Get or build a cached CoordinateTransformation from already-canonical CRS strings.
      *
      * Unlike [[getTransformer]], this overload does NOT call `resolveCrs` for cache-key
      * derivation — the caller guarantees the strings are already canonical (e.g. "EPSG:4326",
      * "ESRI:54008", or a WKT string from [[crsToCanonical]]). On a cache hit this is O(1)
      * with no GDAL calls. On a cache miss it calls `resolveCrs` twice to build the CT,
      * forcing OAMS_TRADITIONAL_GIS_ORDER on both, then releases both SRs.
      *
      * @param srcCanonical  already-canonical source CRS string (from [[crsToCanonical]])
      * @param dstCanonical  already-canonical target CRS string (from [[crsToCanonical]])
      */
    def getTransformerByCanonical(srcCanonical: String, dstCanonical: String): CoordinateTransformation = {
        val key = s"$srcCanonical->$dstCanonical"
        val cache = txCache.get()
        cache.get(key) match {
            case Some(tf) =>
                cache.remove(key); cache.put(key, tf) // move-to-end (most-recent)
                tf
            case None =>
                // Build CT with traditional axis order so JTS (x=lon, y=lat) input isn't flipped.
                val srcSR = resolveCrs(srcCanonical)
                val dstSR = resolveCrs(dstCanonical)
                srcSR.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
                dstSR.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
                val tf = new CoordinateTransformation(srcSR, dstSR)
                srcSR.delete()
                dstSR.delete()
                cache.put(key, tf)
                if (cache.size > TRANSFORMER_CACHE_SIZE) cache.remove(cache.head._1) // evict oldest
                tf
        }
    }

    /** Everything a per-row CRS consumer needs about a resolved CRS, with NO live GDAL handle.
      *
      * `canonical` is the [[crsToCanonical]] string (authority `NAME:CODE`, else full WKT) and
      * `authoritySrid` is the integer authority code when the CRS has a numeric one (the value a
      * geometry can carry as its SRID), else None. Both are derived from a single short-lived
      * `SpatialReference` that is released before this record is returned — so a `CrsInfo` is an
      * immutable value that can be cached and reused indefinitely without any resource risk. */
    final case class CrsInfo(canonical: String, authoritySrid: Option[Int])

    /** A resolved src→dst transform: either the identity (CRSes are the same, per OSR `IsSame`)
      * or a cache-owned [[CoordinateTransformation]]. `transformation` is null when
      * `identity` is true. The CT is owned by [[txCache]] and must never be deleted by a caller. */
    final case class TransformPlan(identity: Boolean, transformation: CoordinateTransformation)

    // Same sizing rationale as TRANSFORMER_CACHE_SIZE, doubled: a CrsInfo key is the raw
    // user-supplied CRS spelling, so one CRS can occupy several entries ("4326", "EPSG:4326").
    private val CRS_INFO_CACHE_SIZE = 256

    // CrsInfo holds no native handle, so this cache could be shared across threads. It is kept
    // thread-local anyway so the per-row hot path needs no lock and no atomic, matching txCache.
    // Per-thread duplication of a (String, Option[Int]) record is negligible.
    private val crsInfoCache =
        new ThreadLocal[mutable.LinkedHashMap[String, CrsInfo]] {
            override def initialValue(): mutable.LinkedHashMap[String, CrsInfo] =
                mutable.LinkedHashMap.empty
        }

    // A TransformPlan references a CoordinateTransformation, which is NOT safe to share between
    // threads — so this cache MUST be thread-local, exactly like txCache.
    //
    // Keyed by the (src, dst) canonical pair as a TUPLE rather than a concatenated string: an
    // authority-less CRS canonicalizes to its full WKT, so building "$src->$dst" would allocate
    // and hash a multi-hundred-character string on every row. The canonical strings come from
    // the CrsInfo cache, so each row sees the SAME String instance — Tuple2 reuses those
    // instances' cached hashCodes and String.equals short-circuits on reference equality.
    private val planCache =
        new ThreadLocal[mutable.LinkedHashMap[(String, String), TransformPlan]] {
            override def initialValue(): mutable.LinkedHashMap[(String, String), TransformPlan] =
                mutable.LinkedHashMap.empty
        }

    /** Authority code as an Int when the CRS carries a numeric one (e.g. `EPSG:4326` -> 4326,
      * `ESRI:54008` -> 54008); None for authority-less CRS (raw WKT / PROJ4) and for
      * non-numeric codes such as `OGC:CRS84`.
      *
      * The single home for the "what SRID can a geometry carry for this CRS?" rule — shared by
      * [[crsInfo]] and by `ST_SetCrs`, which must apply exactly the same rule when deciding
      * whether a CRS can be stamped onto a geometry at all. */
    def authoritySridOf(spatialRef: SpatialReference): Option[Int] =
        for {
            name <- Option(spatialRef.GetAuthorityName(null)) if name.nonEmpty
            code <- Option(spatialRef.GetAuthorityCode(null))
            n    <- Try(code.toInt).toOption
        } yield n

    /** Cached [[CrsInfo]] for a raw CRS spelling — the per-row entry point that replaces
      * `resolveCrs` + `crsToCanonical` + authority probing in hot loops.
      *
      * On a cache hit this makes ZERO GDAL calls. On a miss it resolves exactly one
      * `SpatialReference`, reads the canonical string and authority code off it, and releases it
      * in a `finally` before caching the resulting immutable record.
      *
      * Throws the same `IllegalArgumentException` as [[resolveCrs]] for an unresolvable CRS, and
      * caches NOTHING on failure — so a caller that degrades on failure (the never-error
      * invariant) keeps degrading on every subsequent row rather than seeing a poisoned entry. */
    def crsInfo(value: String): CrsInfo = {
        require(value != null, "crsInfo: CRS value is null")
        val key = value.trim
        val cache = crsInfoCache.get()
        cache.get(key) match {
            case Some(info) =>
                cache.remove(key); cache.put(key, info) // move-to-end (most-recent)
                info
            case None =>
                val sr = resolveCrs(key) // may throw; nothing is cached in that case
                val info = try {
                    CrsInfo(crsToCanonical(sr), authoritySridOf(sr))
                } finally {
                    sr.delete()
                }
                cache.put(key, info)
                if (cache.size > CRS_INFO_CACHE_SIZE) cache.remove(cache.head._1) // evict oldest
                info
        }
    }

    /** Cached [[TransformPlan]] for a canonical CRS pair — the per-row entry point for
      * reprojection. On a cache hit this makes ZERO GDAL calls.
      *
      * On a miss it resolves both CRSes once to evaluate OSR `IsSame` (so the identity
      * short-circuit keeps exactly the semantics of comparing two live `SpatialReference`s,
      * not mere canonical-string equality), releases both, and for the non-identity case takes
      * the cache-owned CT from [[getTransformerByCanonical]].
      *
      * @param srcCanonical already-canonical source CRS string (from [[crsInfo]])
      * @param dstCanonical already-canonical target CRS string (from [[crsInfo]])
      */
    def transformPlan(srcCanonical: String, dstCanonical: String): TransformPlan = {
        val key = (srcCanonical, dstCanonical)
        val cache = planCache.get()
        cache.get(key) match {
            case Some(plan) =>
                cache.remove(key); cache.put(key, plan) // move-to-end (most-recent)
                plan
            case None =>
                val srcSR = resolveCrs(srcCanonical)
                val same = try {
                    val dstSR = resolveCrs(dstCanonical)
                    try srcSR.IsSame(dstSR) == 1 finally dstSR.delete()
                } finally {
                    srcSR.delete()
                }
                val plan =
                    if (same) TransformPlan(identity = true, null)
                    else TransformPlan(
                      identity = false, getTransformerByCanonical(srcCanonical, dstCanonical))
                cache.put(key, plan)
                if (cache.size > TRANSFORMER_CACHE_SIZE) cache.remove(cache.head._1) // evict oldest
                plan
        }
    }

    /** Rule 1 (per-geom) source-CRS resolution — mirror of the light `resolve_source_crs`.
      * Embedded SRID (from EWKB/EWKT) always wins; else the single explicit `srid` or
      * `crs` (both set -> error); else None (CRS-less). The explicit param is a per-geom
      * fallback for plain WKB/WKT — a geom carrying an embedded SRID ignores the param
      * (mixed-column safe), no error. */
    def resolveSourceSR(
        embeddedSrid: Int, srid: Option[Int], crs: Option[String]
    ): Option[SpatialReference] = {
        if (embeddedSrid > 0) Some(resolveCrs(embeddedSrid.toString))
        else (srid, crs) match {
            case (Some(_), Some(_)) =>
                throw new IllegalArgumentException("resolveSourceSR: provide srid OR crs, not both")
            case (_, Some(c)) => Some(resolveCrs(c))
            case (Some(s), _) => Some(resolveCrs(s.toString))
            case _            => None
        }
    }

    /** Target CRS's area_of_use bbox in EPSG:4326 (west, south, east, north), or None
      * when the CRS carries no area-of-use metadata (caller skips the domain check).
      *
      * Resource discipline: `AreaOfUse` has `swigCMemOwn=true` and its own native pointer
      * (confirmed from GDAL 3.11 JAR). It is NOT freed when its parent SpatialReference is
      * deleted — the two are independent SWIG objects. Both must be explicitly `delete()`d:
      * the `AreaOfUse` in an inner `finally`, the `SpatialReference` in the outer `finally`. */
    def areaOfUse(canonical: String): Option[(Double, Double, Double, Double)] = {
        val sr = resolveCrs(canonical)
        try {
            val a = sr.GetAreaOfUse()  // GDAL 3.0+: AreaOfUse (owns native memory) or null
            if (a == null) None
            else try {
                Some((a.getWest_lon_degree, a.getSouth_lat_degree,
                      a.getEast_lon_degree, a.getNorth_lat_degree))
            } finally a.delete()
        } finally sr.delete()
    }

    /** True when every (lon, lat) is inside the bbox; straddling the boundary → false. */
    def allInBBox(lonLat: Array[(Double, Double)], b: (Double, Double, Double, Double)): Boolean = {
        val (w, s, e, n) = b
        lonLat.forall { case (lon, lat) => lon >= w && lon <= e && lat >= s && lat <= n }
    }

    /** Builds a SpatialReference from an EPSG code; uses WGS84 if code <= 0. */
    def fromEPSGCode(getSRID: Int): SpatialReference = {
        val sr = new SpatialReference()
        if (getSRID > 0) {
            sr.ImportFromEPSG(getSRID)
        } else {
            sr.SetWellKnownGeogCS("WGS84") // Default to WGS84 if no valid EPSG code
        }
        sr
    }

}
