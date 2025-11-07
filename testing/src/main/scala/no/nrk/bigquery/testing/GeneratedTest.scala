/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package testing

import munit.Assertions.{assert, assertEquals}
import munit.Location

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

trait BQDatasetDirectoryProvider {
  lazy val useDatasetDir: Boolean =
    sys.env.contains("USE_DATASET_DIR_FOR_GENERATED_BQ_FILES")

  def BQDatasetName: String = ""
}

trait GeneratedTest extends BQDatasetDirectoryProvider {
  def testType: String

  def assertCurrentGeneratedFiles: Boolean =
    sys.env.contains("ASSERT_CURRENT_GENERATED_FILES")

  def basedir: Path = {
    var f = new File(
      getClass.getProtectionDomain.getCodeSource.getLocation.getFile
    ).getParentFile
    while (!new File(f.getParentFile, "build.sbt").exists())
      f = f.getParentFile
    f.getParentFile.toPath
  }

  def generatedDir: Path = basedir.resolve("generatedMODIFIED4")

  def testFileForName(name: String): Path = {
    val safeFileName = name.replaceAll("\\s", "_")
    if (useDatasetDir) {
      generatedDir.resolve(testType).resolve(BQDatasetName).resolve(safeFileName)
    } else {
      generatedDir.resolve(testType).resolve(safeFileName)
    }
  }

  def writeAndCompare(testFile: Path, value: String)(implicit
      loc: Location
  ): Unit =
    if (assertCurrentGeneratedFiles) {
      assert(
        Files.exists(testFile),
        s"Expected $testFile to exist. Run failing test locally and check in generated files"
      )
      val storedJsonString =
        new String(Files.readAllBytes(testFile), StandardCharsets.UTF_8)
      assertEquals(value, storedJsonString)
    } else {
      Files.createDirectories(testFile.getParent)
      Files.write(testFile, value.getBytes(StandardCharsets.UTF_8))
      assert(true)
    }
}
