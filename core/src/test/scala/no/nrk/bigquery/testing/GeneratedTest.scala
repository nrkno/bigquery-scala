package no.nrk.bigquery
package testing

import munit.Assertions.{assert, assertEquals}
import munit.Location

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

trait GeneratedTest {
  def testType: String

  val assertCurrentGeneratedFiles: Boolean =
    sys.env.contains("ASSERT_CURRENT_GENERATED_FILES")

  lazy val dir: Path = {
    val ret = Paths.projectRoot.resolve(s"generated/$testType")
    Files.createDirectories(ret)
    ret
  }

  def testFileForName(name: String): Path =
    dir.resolve(name)

  def writeAndCompare(testFile: Path, value: String)(implicit
      loc: Location
  ): Unit =
    if (assertCurrentGeneratedFiles) {
      assert(
        Files.exists(testFile),
        s"Expected $testFile to exist. Run failing test locally and check in generated files"
      )
      val storedJsonString = Files.readString(testFile)
      assertEquals(value, storedJsonString)
    } else {
      Files.createDirectories(testFile.getParent)
      Files.write(testFile, value.getBytes(StandardCharsets.UTF_8))
      assert(true)
    }
}
