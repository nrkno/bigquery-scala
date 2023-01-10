package no.nrk.bigquery
package testing

import cats.effect.IO
import cats.effect.kernel.Outcome
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax._
import munit.{CatsEffectSuite, Location}
import no.nrk.bigquery.implicits._
import org.typelevel.log4cats.slf4j._

import java.nio.file.{Files, Path}

class BQUdfSmokeTest extends CatsEffectSuite {
  val bqClient: Fixture[BigQueryClient] = ResourceSuiteLocalFixture(
    "bqClient",
    BigQueryTestClient.testClient
  )
  override def munitFixtures = List(bqClient)


  /** Evaluates the call against BQ but caches it. This is only meant to be used
    * with pure UDFs, not those which reads tables.
    *
    * The jsonified result of evaluating the call is compared to the provided
    * json structure.
    */
  protected def bqCheckCall(
      testName: String,
      call: BQSqlFrag.Call,
      expected: Json
  )(implicit loc: Location): Unit = {
    val longerTestName = s"${call.udf.name.value} - $testName"

    test(s"bqCheck UDF: $longerTestName") {
      BQUdfSmokeTest
        .bqEvaluateCall(longerTestName, call)
        .apply(bqClient())
        .map(actual => assertEquals(actual, expected))
    }
  }
}

object BQUdfSmokeTest {
  private val logger = Slf4jFactory.getLogger[IO]

  object `udf-results` extends GeneratedTest {
    override def testType: String = "udf-results"
  }

  def bqEvaluateCall(
      testName: String,
      call: BQSqlFrag.Call
  ): BigQueryClient => IO[Json] = { bqClient =>
    val query = bqfr"SELECT TO_JSON_STRING($call)"
    val cachedQuery = CachedQuery(query)

    cachedQuery.readRow
      .flatMap {
        case Some(rows) => IO.pure(rows)
        case None =>
          val log = logger.warn(
            s"Running $testName against BQ (could have been cached)"
          )

          val run = bqClient
            .synchronousQuery(BQJobName("smoketest"), BQQuery[Json](query))
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
        Files.writeString(cacheFile, row.asJson.noSpaces)
      }

    val readRow: IO[Option[Json]] = IO {
      if (Files.exists(cacheFile)) {
        decode[Json](Files.readString(cacheFile)) match {
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
