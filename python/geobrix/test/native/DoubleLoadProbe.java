/** Loads one GDAL's JNI, then asks GDALManager for the payload's: GDALManager must refuse. */
public class DoubleLoadProbe {
    public static void main(String[] a) {
        System.load(a[0]);
        try {
            com.databricks.labs.gbx.rasterx.gdal.GDALManager.loadNativeFrom(a[1]);
            System.out.println("LOADED");
            System.exit(0);
        } catch (IllegalStateException e) {
            System.err.println(e.getMessage());
            System.exit(3);
        }
    }
}
