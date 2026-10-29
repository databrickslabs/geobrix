import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.SparkSession;

/**
 * A fresh JVM whose first GDAL user has no ExpressionConfig; only the session's spark.databricks.labs.gbx.native.dir
 * (args[1], may be empty) names a payload. Modes (args[0]):
 *   kernel             KernelGdal (the driver: the session's conf)
 *   writer             a vector writer's native load on a task
 *   writer-no-session  the same with no session visible on the driver, as on an executor: only the task's local
 *                      property can carry the conf
 *   ogr                GDALManager.initOgr (also prints GDAL_DATA)
 *   shared             GDALManager.init with spark.databricks.labs.gbx.sharedobjects.extra = args[2]
 * Prints the outcome and every libgdal file the JVM maps.
 */
public class ConfOnlyProbe {
    public static void main(String[] a) throws Exception {
        SparkSession.Builder b = SparkSession.builder().master("local[1]").appName("gbx-conf-only")
                .config("spark.ui.enabled", "false")
                .config("spark.databricks.labs.gbx.native.dir", a[1]);
        if (a[0].equals("shared")) b = b.config("spark.databricks.labs.gbx.sharedobjects.extra", a[2]);
        SparkSession spark = b.getOrCreate();
        String out;
        try {
            switch (a[0]) {
                case "kernel":
                    out = com.databricks.labs.gbx.kernels.KernelGdal.probe();
                    break;
                case "writer":
                case "writer-no-session":
                    if (a[0].equals("writer-no-session")) {
                        SparkSession.clearActiveSession();
                        SparkSession.clearDefaultSession();
                        boolean visible = SparkSession.getActiveSession().isDefined()
                                || SparkSession.getDefaultSession().isDefined();
                        System.out.println("SESSIONS_VISIBLE " + visible);
                    }
                    spark.range(1).foreachPartition((ForeachPartitionFunction<Long>) it ->
                            com.databricks.labs.gbx.vectorx.ds.geojsonl.GeoJSONL_RowWriter$.MODULE$.ensureNativeLoaded());
                    out = "ok:" + org.gdal.gdal.gdal.VersionInfo("RELEASE_NAME");
                    break;
                case "ogr":
                    com.databricks.labs.gbx.rasterx.gdal.GDALManager.initOgr();
                    out = "ok:" + org.gdal.gdal.gdal.VersionInfo("RELEASE_NAME");
                    System.out.println("GDAL_DATA " + org.gdal.gdal.gdal.GetConfigOption("GDAL_DATA"));
                    break;
                case "shared":
                    com.databricks.labs.gbx.rasterx.gdal.GDALManager.init(
                            com.databricks.labs.gbx.expressions.ExpressionConfig.apply(spark));
                    out = "ok:" + org.gdal.gdal.gdal.VersionInfo("RELEASE_NAME");
                    break;
                default:
                    out = "error:unknown mode " + a[0];
            }
        } catch (Exception e) {
            out = "error:" + e;
        }
        System.out.println("RESULT " + out);
        for (String line : java.nio.file.Files.readAllLines(java.nio.file.Paths.get("/proc/self/maps"))) {
            if (line.matches(".*/libgdal(-[0-9a-f]{8})?\\.so(\\.\\d+)+$")) {
                System.out.println("MAPPED " + line.substring(line.indexOf('/')));
            }
        }
        spark.stop();
    }
}
