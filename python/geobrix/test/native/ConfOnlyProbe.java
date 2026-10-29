import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.SparkSession;

/**
 * A fresh JVM whose first GDAL user has no ExpressionConfig: KernelGdal ("kernel"), or a vector writer's native load
 * on a task ("writer"). Only the session's spark.databricks.labs.gbx.native.dir names the payload (args[1]). Prints
 * the outcome and every libgdal file the JVM maps.
 */
public class ConfOnlyProbe {
    public static void main(String[] a) throws Exception {
        SparkSession spark = SparkSession.builder().master("local[1]").appName("gbx-conf-only")
                .config("spark.ui.enabled", "false")
                .config("spark.databricks.labs.gbx.native.dir", a[1])
                .getOrCreate();
        String out;
        try {
            if (a[0].equals("kernel")) {
                out = com.databricks.labs.gbx.kernels.KernelGdal.probe();
            } else {
                spark.range(1).foreachPartition((ForeachPartitionFunction<Long>) it ->
                        com.databricks.labs.gbx.vectorx.ds.geojsonl.GeoJSONL_RowWriter$.MODULE$.ensureNativeLoaded());
                out = "ok:" + org.gdal.gdal.gdal.VersionInfo("RELEASE_NAME");
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
