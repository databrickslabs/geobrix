package com.databricks.labs.gbx.kernels

import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.Properties
import java.util.zip.ZipFile

import scala.util.Try

/** Identity of the running geobrix JAR, for the Python manager's version and sha256 check. Catalyst-free.
  *
  * Both parts come from the ONE file the class was loaded from, so they cannot describe two different JARs.
  * A classes directory is a development run (`dev|dev`). A JAR whose identity cannot be read, or a class
  * with no code source, is `unknown|unknown`: never a silent `dev`, and the manager refuses it. */
object BuildInfo {
    private val PomProps = "META-INF/maven/com.databricks.labs/geobrix/pom.properties"
    private val Dev = "dev"
    private val Unknown = "unknown"

    private lazy val here: String = identityAt(Try(getClass.getProtectionDomain.getCodeSource.getLocation.toURI).getOrElse(null))

    /** `"<version>|<sha256>"` of the running JAR. */
    def identity(): String = here

    /** The identity of the file or directory `location` names (`null` when the code source is unknown). */
    def identityAt(location: URI): String =
        Try {
            val path = Paths.get(location)
            if (Files.isDirectory(path)) s"$Dev|$Dev" else s"${versionIn(path)}|${sha256Of(path)}"
        }.getOrElse(s"$Unknown|$Unknown")

    private def versionIn(jar: Path): String = {
        val zip = new ZipFile(jar.toFile)
        try {
            val entry = zip.getEntry(PomProps)
            if (entry == null) {
                Unknown
            } else {
                val in = zip.getInputStream(entry)
                try {
                    val p = new Properties()
                    p.load(in)
                    p.getProperty("version", Unknown)
                } finally in.close()
            }
        } finally zip.close()
    }

    /** Streams the file through the digest: the shaded JAR is hundreds of MB, never read it whole. */
    private def sha256Of(file: Path): String = {
        val md = MessageDigest.getInstance("SHA-256")
        val in = Files.newInputStream(file)
        try {
            val buf = new Array[Byte](1 << 20)
            Iterator.continually(in.read(buf)).takeWhile(_ >= 0).foreach(n => md.update(buf, 0, n))
        } finally in.close()
        md.digest().map("%02x".format(_)).mkString
    }
}
