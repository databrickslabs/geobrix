package tests.docs.scala

/**
 * Sample data base path for doc tests.
 * Uses GBX_SAMPLE_DATA_ROOT when set (e.g. test-data for minimal bundle, or geobrix_samples for full).
 * When unset (e.g. --no-sample-data-root), defaults to full bundle path so tests pass when full bundle is mounted.
 */
object SampleDataPath {
  private val root = Option(System.getenv("GBX_SAMPLE_DATA_ROOT")).getOrElse("/Volumes/main/default/geobrix_samples")
  val base: String = s"$root/geobrix-examples"
  val nycSentinel2: String = s"$base/nyc/sentinel2/nyc_sentinel2_red.tif"
  val nycBoroughs: String = s"$base/nyc/boroughs/nyc_boroughs.geojson"
  val nycBoroughsGeojsonl: String = s"$base/nyc/boroughs/nyc_boroughs.geojsonl"
  val nycSubwayShp: String = s"$base/nyc/subway/nyc_subway.shp.zip"
  val nycGeoPackage: String = s"$base/nyc/geopackage/nyc_complete.gpkg"
}
