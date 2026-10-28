// Loads the JNI library built with GDAL by absolute path, then checks GDAL, OGR and OSR from the Java classes.
// Run by verify-native.sh: java -cp gdal-<version>.jar JniSmoke.java <lib dir> <share dir> <GDAL version>
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.ogr.ogr;
import org.gdal.osr.SpatialReference;

public class JniSmoke {
    public static void main(String[] a) {
        String lib = a[0], share = a[1], want = a[2];
        System.load(lib + "/libgdalalljni.so");
        gdal.SetConfigOption("PROJ_DATA", share + "/proj");
        gdal.SetConfigOption("GDAL_DATA", share + "/gdal");
        gdal.AllRegister();
        ogr.RegisterAll();
        String got = gdal.VersionInfo("RELEASE_NAME");
        if (!got.equals(want)) fail("GDAL " + got + " != " + want);
        Dataset ds = gdal.GetDriverByName("MEM").Create("", 388, 385, 1);
        if (ds.getRasterXSize() != 388 || ds.getRasterYSize() != 385) fail("MEM dataset size");
        ds.delete();
        if (ogr.GetDriverByName("OpenFileGDB") == null) fail("no OpenFileGDB driver");
        if (new SpatialReference().ImportFromEPSG(27700) != 0) fail("EPSG:27700 (PROJ data)");
        System.out.println("JNI_SMOKE_OK " + got);
    }

    static void fail(String m) {
        System.err.println("JNI_SMOKE_FAIL " + m);
        System.exit(1);
    }
}
