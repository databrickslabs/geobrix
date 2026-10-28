// Loads the JNI library built with GDAL by absolute path, then checks GDAL, OGR and OSR from the Java classes.
// Run by verify-native.sh, compiled first and started from the class path (the source-file launcher loads the class in a
// separate class loader, and a library loaded there is invisible to the jar):
//   javac -cp gdal-<version>.jar JniSmoke.java; java -cp gdal-<version>.jar:. JniSmoke <lib dir> <share dir> <GDAL version>
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
        // GDAL_DATA is honoured only when set before AllRegister(): the first file lookup fixes the search path
        if (gdal.FindFile("gdal", "osmconf.ini") == null) fail("GDAL_DATA (set before AllRegister) did not reach share/gdal");
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
