package no.nrk.bigquery

import scala.util.Properties
import java.nio.file.{Path, Paths => JPaths}
import java.io.File

object Paths {
  lazy val projectRoot: Path = {
    var f = new File(
      getClass.getProtectionDomain.getCodeSource.getLocation.getFile
    ).getParentFile
    while (!new File(f.getParentFile, "build.sbt").exists())
      f = f.getParentFile
    f.getParentFile.toPath
  }
  val basedir: Path = JPaths.get(Properties.userHome).resolve(".bigquery")
}
