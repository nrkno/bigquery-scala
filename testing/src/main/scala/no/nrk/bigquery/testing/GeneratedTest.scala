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

trait GeneratedTest {
  def testType: String
  def datasetDir: String = ""

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

  def generatedDir = basedir.resolve("generated")

  def testFileForName(name: String): Path =
    generatedDir.resolve(testType).resolve(datasetDir).resolve(name.replaceAll("\\s", "_"))

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
