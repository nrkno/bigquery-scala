/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package testing

import cats.syntax.all.*
import cats.effect.{IO, Resource}
import cats.effect.kernel.Outcome
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax.*
import munit.catseffect.IOFixture
import munit.{CatsEffectSuite, Location}
import no.nrk.bigquery.syntax.*
import org.typelevel.log4cats.slf4j.*

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

abstract class BQUdfSmokeTest(testClient: Resource[IO, QueryClient[IO]]) extends CatsEffectSuite {
  val bqClient: IOFixture[QueryClient[IO]] = ResourceSuiteLocalFixture(
    "bqClient",
    testClient
  )

  override def munitFixtures = List(bqClient)

  /** Evaluates the call against BQ but caches it. This is only meant to be used with pure UDFs, not those which reads
    * tables.
    *
    * The jsonified result of evaluating the call is compared to the provided json structure.
    */
  protected def bqCheckCall(
      testName: String,
      call: BQSqlFrag.Call,
      expected: Json
  )(implicit loc: Location): Unit = {
    val longerTestName = show"${call.udf.name} - $testName"
    test(s"bqCheck UDF: $longerTestName") {
      BQUdfSmokeTest
        .bqEvaluateCall(longerTestName, call)
        .apply(bqClient())
        .map(actual => assertEquals(actual, expected))
    }
  }

}

object BQUdfSmokeTest {
  private val logger = Slf4jFactory.create[IO].getLogger

  object `udf-results` extends GeneratedTest {
    override def testType: String = "udf-results"
  }

  def bqEvaluateCall(
      testName: String,
      call: BQSqlFrag.Call
  ): QueryClient[IO] => IO[Json] = { bqClient =>
    val temporaryUdfCall = call.udf match {
      case _: UDF.Temporary[?] => call
      case _: UDF.Reference[?] => call
      case udf: UDF.Persistent[?] => call.copy(udf = udf.convertToTemporary)
    }

    val query = bqfr"SELECT TO_JSON_STRING($temporaryUdfCall)"
    val cachedQuery = CachedQuery(query)

    cachedQuery.readRow
      .flatMap {
        case Some(rows) => IO.pure(rows)
        case None =>
          val log = logger.warn(
            s"Running $testName against BQ (could have been cached)"
          )

          val run = bqClient
            .synchronousQuery(BQJobId("smoketest"), BQQuery[Json](query))
            .compile
            .lastOrError
            .guaranteeCase {
              case Outcome.Errored(_) =>
                IO(println(s"failed query: ${query.asStringWithUDFs}"))
              case _ => IO.unit
            }
            .flatTap(cachedQuery.writeRow)

          log *> run
      }
  }

  // this is a user-wide query cache to speed up development/CI
  case class CachedQuery(frag: BQSqlFrag) {
    val cacheFile: Path = BigQueryTestClient.basedir
      .resolve("smoke-test-udf-cache")
      .resolve(s"${frag.asStringWithUDFs.hashCode()}.json")

    def writeRow(row: Json): IO[Path] =
      IO {
        Files.createDirectories(cacheFile.getParent)
        Files.write(
          cacheFile,
          row.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
        )
      }

    val readRow: IO[Option[Json]] = IO {
      if (Files.exists(cacheFile)) {
        decode[Json](
          new String(Files.readAllBytes(cacheFile), StandardCharsets.UTF_8)
        ) match {
          case Left(err) =>
            System.err.println(
              s"Couldn't parse query cache file $cacheFile. Rerunning query. ${err.getMessage}"
            )
            Files.delete(cacheFile)
            None
          case Right(rows) => Some(rows)
        }
      } else None
    }
  }
}
