package com.databricks.labs.gbx.kernels

import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.Properties

import scala.util.Try

/** Identity of the running geobrix JAR, for the Python manager's version and sha256 check.
  * Catalyst-free. A run from `target/classes` has no packaged JAR, so both parts read `dev`. */
object BuildInfo {
    private val PomProps = "/META-INF/maven/com.databricks.labs/geobrix/pom.properties"
    private val Dev = "dev"

    lazy val version: String = Option(getClass.getResourceAsStream(PomProps)).map { in =>
        try {
            val p = new Properties()
            p.load(in)
            p.getProperty("version", Dev)
        } finally in.close()
    }.getOrElse(Dev)

    /** Streams the JAR through the digest: the shaded JAR is hundreds of MB, never read it whole. */
    lazy val sha256: String = Try {
        val path = Paths.get(getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
        if (!Files.isRegularFile(path)) {
            Dev
        } else {
            val md = MessageDigest.getInstance("SHA-256")
            val in = Files.newInputStream(path)
            try {
                val buf = new Array[Byte](1 << 20)
                Iterator.continually(in.read(buf)).takeWhile(_ >= 0).foreach(n => md.update(buf, 0, n))
            } finally in.close()
            md.digest().map("%02x".format(_)).mkString
        }
    }.getOrElse(Dev)

    /** `"<version>|<sha256>"`. */
    def identity(): String = s"$version|$sha256"
}
