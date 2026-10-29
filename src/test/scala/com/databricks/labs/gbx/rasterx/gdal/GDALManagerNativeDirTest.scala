package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.rasterx.gdal.GDALManager.NativeChoice
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Which GDAL a JVM loads: the conf dir, else GBX_NATIVE_ROOT, else the system JNI; one GDAL or a refusal. Pure: no
  * JNI is loaded here. */
class GDALManagerNativeDirTest extends AnyFunSuite {

    private val dir = "/w/payload/_native/aarch64"
    private val jni = s"$dir/lib/libgdalalljni.so"
    private val legacy = "/usr/lib/libgdalalljni.so"

    private def resolve(
        conf: Option[String] = None,
        env: Option[String] = None,
        arch: String = "aarch64",
        files: Set[String] = Set(jni, "/r/_native/x86_64/lib/libgdalalljni.so", legacy),
        real: Map[String, String] = Map.empty,
        mapped: Seq[String] = Nil,
        libraryPath: Seq[String] = Nil
    ) = GDALManager.resolveNative(conf, env, arch, files, p => real.getOrElse(p, p), mapped, libraryPath)

    private def refusal(r: Either[String, NativeChoice]): String = r.left.getOrElse(fail(s"expected a refusal, got $r"))

    test("resolveNative chooses one GDAL or refuses") {
        // the conf dir wins over GBX_NATIVE_ROOT
        resolve(Some(dir), Some("/r")) shouldBe Right(
          NativeChoice(jni, Some(s"$dir/../share/proj"), Some(s"$dir/../share/gdal"))
        )
        // GBX_NATIVE_ROOT on amd64 is the payload's x86_64 build
        resolve(env = Some("/r"), arch = "amd64").map(_.jni) shouldBe Right("/r/_native/x86_64/lib/libgdalalljni.so")
        // a missing JNI is refused, naming the path
        refusal(resolve(Some("/nowhere/_native/aarch64"))) should include("/nowhere/_native/aarch64/lib/libgdalalljni.so")
        // another GDAL already mapped in this process is refused, naming both
        val two = refusal(resolve(Some(dir), mapped = Seq("/usr/local/lib/libgdal.so.37")))
        two should (include("a different GDAL is already loaded") and include("/usr/local/lib/libgdal.so.37")
            and include(s"$dir/lib"))
        // the system JNI linked into a payload (DCS, init script) runs on that payload's data
        val p = "/opt/gbx/_native/aarch64"
        resolve(files = Set(legacy), real = Map(legacy -> s"$p/lib/libgdalalljni.so")) shouldBe Right(
          NativeChoice(legacy, Some(s"$p/../share/proj"), Some(s"$p/../share/gdal"))
        )
        // a plain system JNI runs on the system GDAL's own data
        resolve(files = Set(legacy)) shouldBe Right(NativeChoice(legacy, None, None))
        // GDAL's Java classes load the first libgdalalljni.so on java.library.path by name: another one there is refused
        refusal(resolve(Some(dir), libraryPath = Seq("/usr/local/lib", "/usr/lib"))) should (include(legacy)
            and include(s"$dir/lib"))
        resolve(Some(dir), libraryPath = Seq(s"$dir/lib", "/usr/lib")).isRight shouldBe true
    }

}
