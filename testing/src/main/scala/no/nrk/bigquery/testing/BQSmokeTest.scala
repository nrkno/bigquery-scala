package no.nrk.bigquery
package testing

import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.syntax.alternative._
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.bigquery.{BigQueryException, Field, StandardSQLTypeName}
import io.circe.parser.decode
import io.circe.syntax._
import munit.Assertions.fail
import munit.{CatsEffectSuite, Location}
import no.nrk.bigquery._
import no.nrk.bigquery.testing.BQSmokeTest.{CheckType, bqCheckFragment}
import org.typelevel.log4cats.slf4j._
import no.nrk.bigquery.implicits._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

abstract class BQSmokeTest extends CatsEffectSuite {
  val assertStableTables: List[BQTableLike[Any]] = Nil

  val bqClient: Fixture[BigQueryClient[IO]] = ResourceSuiteLocalFixture(
    "bqClient",
    BigQueryTestClient.testClient
  )

  override def munitFixtures = List(bqClient)

  protected def bqCheckTest[A](
      testName: String
  )(query: BQQuery[A])(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      bqCheckFragment(
        testName,
        query.sql,
        CheckType.Type(query.bqRead.bqType),
        assertStableTables,
        BQSmokeTest.queries
      )(bqClient())
    }

  protected def bqChecksTest[A](
      testName: String
  )(queries: List[BQQuery[A]])(implicit loc: Location): Unit =
    test(s"bqChecks: $testName".tag(TestTags.Generated)) {
      val combinedSql = queries
        .map(_.sql)
        .mkFragment(bqfr"SELECT * FROM (", bqfr") UNION ALL (", bqfr")")
      bqCheckFragment(
        testName,
        combinedSql,
        CheckType.Type(queries.head.bqRead.bqType),
        assertStableTables,
        BQSmokeTest.queries
      )(bqClient())
    }

  protected def bqCheckFragmentNoSchemaTest(
      testName: String
  )(frag: BQSqlFrag)(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      bqCheckFragment(
        testName,
        frag,
        CheckType.Untyped,
        assertStableTables,
        BQSmokeTest.queries
      )(bqClient())
    }

  protected def bqCheckExample(
      testName: String
  )(frag: BQSqlFrag)(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      bqCheckFragment(
        testName,
        frag,
        CheckType.Untyped,
        assertStableTables,
        BQSmokeTest.exampleQueries
      )(bqClient())
    }

  // note that we just compare the generated sql for legacy sql. neither dry runs nor schemas work
  protected def bqCheckLegacyFragmentTest[A](
      testName: String
  )(frag: BQSqlFrag)(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      BQSmokeTest.queries.writeAndCompare(
        BQSmokeTest.queries.testFileForName(s"$testName.sql"),
        frag.asStringWithUDFs
      )
    }

  protected def bqCheckFragmentTest(
      testName: String
  )(tuple: (BQSqlFrag, BQSchema))(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      bqCheckFragment(
        testName,
        tuple._1,
        CheckType.Schema(tuple._2),
        assertStableTables,
        BQSmokeTest.queries
      )(bqClient())
    }

  protected def bqCheckFill(
      testName: String
  )(fill: BQFill)(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      bqCheckFragment(
        testName,
        fill.query,
        CheckType.Schema(fill.tableDef.schema),
        assertStableTables,
        BQSmokeTest.queries
      )(bqClient())
    }

  protected def bqCheckViewTest(
      testName: String,
      view: BQTableDef.ViewLike[Any]
  )(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      bqCheckFragment(
        testName,
        view.query,
        CheckType.Schema(view.schema),
        assertStableTables,
        BQSmokeTest.queries
      )(bqClient())
    }

  protected def bqCheckFragmentTestFailing(
      testName: String,
      errorFragment: String
  )(frag: BQSqlFrag)(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      bqCheckFragment(
        testName,
        frag,
        CheckType.Failing,
        assertStableTables,
        BQSmokeTest.queries
      )(bqClient()).attempt
        .flatMap {
          case Left(e: BigQueryException) if e.getMessage != null =>
            IO(assert(e.getError.getMessage.contains(errorFragment)))
          case Left(other) => IO.raiseError(other)
          case Right(_) =>
            IO(
              fail(
                s"Expected BQ to report failure with the substring '$errorFragment'"
              )
            )
        }
    }
}

private object BQSmokeTest {
  private val logger = Slf4jFactory.getLogger[IO]

  object staticQueries extends GeneratedTest {
    override def testType: String = "bq-query-static"
  }
  object queries extends GeneratedTest {
    override def testType: String = "bq-query"
  }
  object exampleQueries extends GeneratedTest {
    override def testType: String = "bq-example-query"
  }

  def bqCheckFragment(
      testName: String,
      frag: BQSqlFrag,
      checkType: CheckType,
      assertStable: Seq[BQTableLike[Any]],
      target: GeneratedTest
  ): BigQueryClient[IO] => IO[Unit] = { bqClient =>
    val compareAsIs = IO(
      target.writeAndCompare(
        target.testFileForName(s"$testName.sql"),
        frag.asStringWithUDFs
      )
    )

    dependenciesAsStaticData(frag, assertStable) match {
      case Right(staticFrag) =>
        val compareStatic = IO(
          staticQueries.writeAndCompare(
            staticQueries.testFileForName(s"$testName.sql"),
            staticFrag.asStringWithUDFs
          )
        )

        val cachedQuery = CachedQuery(staticFrag, BigQueryTestClient.basedir)
        val runCheck: IO[Unit] = cachedQuery.read
          .flatMap {
            case Some(schema) => IO.pure(schema)
            case None =>
              val log = logger.warn(
                s"Running $testName against BQ (could have been cached)"
              )
              val run = bqClient
                .dryRun(BQJobName("smoketest"), staticFrag)
                .map(job =>
                  BQSchema.fromSchema(
                    job.getStatistics[QueryStatistics]().getSchema
                  )
                )
                .guaranteeCase {
                  case Outcome.Errored(_) if checkType != CheckType.Failing =>
                    IO(println(s"failed query: ${staticFrag.asStringWithUDFs}"))
                  case _ => IO.unit
                }
                .flatTap(cachedQuery.write)

              log *> run
          }
          .map(checkType.checkSchema)

        compareStatic *> compareAsIs *> runCheck

      case Left(notStaticBecause) =>
        val log = logger.warn(s"Running $testName becase $notStaticBecause")

        val runCheck = bqClient
          .dryRun(BQJobName("smoketest"), frag)
          .guaranteeCase {
            case Outcome.Errored(_) if checkType != CheckType.Failing =>
              IO(println(s"failed query: ${frag.asStringWithUDFs}"))
            case _ => IO.unit
          }
          .map(job =>
            BQSchema.fromSchema(job.getStatistics[QueryStatistics]().getSchema)
          )
          .map(checkType.checkSchema)

        log *> compareAsIs *> runCheck
    }
  }

  sealed trait CheckType {
    def checkSchema(actualSchema: BQSchema): Unit =
      this match {
        case CheckType.Schema(expectedSchema) =>
          conforms(actualSchema, expectedSchema) match {
            case Some(reasons) =>
              fail(s"Failed because ${reasons.mkString(", ")}")
            case None => assert(true)
          }
        case CheckType.Type(expectedType) =>
          conforms.onlyTypes(actualSchema, expectedType) match {
            case Some(reasons) =>
              fail(s"Failed because ${reasons.mkString(", ")}")
            case None => assert(true)
          }
        case CheckType.Untyped | CheckType.Failing =>
          assert(true)
      }
  }

  object CheckType {
    case class Schema(value: BQSchema) extends CheckType
    case class Type(value: BQType) extends CheckType
    case object Untyped extends CheckType
    case object Failing extends CheckType
  }

  def dependenciesAsStaticData(
      frag: BQSqlFrag,
      assertStable: Seq[BQTableLike[Any]]
  ): Either[String, BQSqlFrag] = {
    val (newFrag, ctes) = recurse(frag)
    val structured = BQStructuredSql.parse(newFrag)

    val unstableTables: Set[BQTableLike[Unit]] =
      newFrag.allReferencedAsPartitions
        .map(_.wholeTable.unpartitioned)
        .toSet -- assertStable.map(_.unpartitioned)

    if (structured.queryType.toLowerCase != "select") {
      Left(s"Can only cache SELECT queries, not ${structured.queryType}")
    } else if (unstableTables.nonEmpty)
      Left(
        s"References unstable tables ${unstableTables.map(t => formatTableId(t.tableId)).toList.sorted.mkString(", ")}"
      )
    else
      Right(structured.copy(ctes = ctes.distinct ++ structured.ctes).asFragment)
  }

  private def recurse(frag: BQSqlFrag): (BQSqlFrag, List[CTE]) =
    frag match {
      case x @ BQSqlFrag.Frag(_) =>
        (x, Nil)

      case BQSqlFrag.Call(udf, args) =>
        val (newUdfBody, ctesFromUDF) = recurse(udf.body)
        val (newArgs, ctes) = args.toList.map(recurse).separate
        (
          BQSqlFrag.Call(udf.copy(body = newUdfBody), newArgs),
          ctesFromUDF ++ ctes.flatten
        )

      case BQSqlFrag.Combined(frags) =>
        val allDataRefs = frags.forall {
          case BQSqlFrag.PartitionRef(_) => true
          case BQSqlFrag.FillRef(_)      => true
          case _                         => false
        }

        val (newQueries, ctess) = frags.toList.map(recurse).separate
        val newQuery =
          if (allDataRefs)
            newQueries
              .map(frag => bqfr"(select * from $frag)")
              .mkFragment("(", " UNION ALL ", ")")
          else
            BQSqlFrag.Combined(newQueries)

        (newQuery, ctess.reduce(_ ++ _))

      case p @ BQSqlFrag.PartitionRef(pid) =>
        val schemaOpt: Option[BQSchema] =
          pid.wholeTable match {
            case BQTableRef(_, _)   => None
            case x: BQTableDef[Any] => Some(x.schema)
          }

        schemaOpt match {
          case Some(schema) =>
            val cteName = tempTable(pid)
            (
              cteName.bqShow,
              List(CTE(cteName, bqfr"(select as value ${exampleRow(schema)})"))
            )
          case None =>
            (p, Nil)
        }

      case BQSqlFrag.FilledTableRef(filledTable) =>
        recurse(filledTable.tableDef.unpartitioned.assertPartition.bqShow)

      case BQSqlFrag.FillRef(fill) =>
        // we this fill as a CTE in the output query
        val cteName = tempTable(fill.destination)
        (
          cteName.bqShow,
          List(
            CTE(
              cteName,
              bqfr"(select as value ${exampleRow(fill.tableDef.schema)})"
            )
          )
        )
    }

  def exampleRow(schema: BQSchema): BQSqlFrag = {
    object counter {
      var value = 0L
      def next(): Long = {
        val ret = value
        value += 1
        ret
      }
    }

    def valueForType(field: BQField): BQSqlFrag = {
      val base = field.tpe match {
        case StandardSQLTypeName.JSON  => BQSqlFrag("""JSON '{"foo": "bar"}'""")
        case StandardSQLTypeName.BOOL  => true.bqShow
        case StandardSQLTypeName.INT64 => counter.next().bqShow
        case StandardSQLTypeName.FLOAT64    => (counter.next() + 0.5).bqShow
        case StandardSQLTypeName.NUMERIC    => counter.next().bqShow
        case StandardSQLTypeName.BIGNUMERIC => counter.next().bqShow
        case StandardSQLTypeName.STRING     => StringValue(field.name).bqShow
        case StandardSQLTypeName.BYTES =>
          BQSqlFrag(
            "(select HLL_COUNT.INIT(x) from unnest(['a']) x)"
          ).bqShow // how to write a literal?
        case StandardSQLTypeName.STRUCT => struct(field.subFields)
        case StandardSQLTypeName.ARRAY =>
          List
            .range(0, 2)
            .map(_ => valueForType(field.copy(tpe = field.subFields.head.tpe)))
            .mkFragment("[", ", ", "]")
        case StandardSQLTypeName.TIMESTAMP =>
          BQSqlFrag("TIMESTAMP('2020-01-01 00:00:00+00')")
        case StandardSQLTypeName.DATE => BQSqlFrag("DATE(2020, 1, 1)")
        case StandardSQLTypeName.TIME => BQSqlFrag("TIME(12, 0, 0)")
        case StandardSQLTypeName.DATETIME =>
          BQSqlFrag("DATETIME(2020, 1, 1, 00, 00, 00)")
        case StandardSQLTypeName.GEOGRAPHY =>
          BQSqlFrag("ST_GeogFromText('POINT(0 0)')")
        case StandardSQLTypeName.INTERVAL =>
          BQSqlFrag("MAKE_INTERVAL(1, 6, 15)")
      }

      field.mode match {
        case Field.Mode.REPEATED =>
          List.range(0, 2).map(_ => base).mkFragment("[", ", ", "]")
        case _ => base
      }
    }

    def struct(fields: Seq[BQField]): BQSqlFrag =
      fields
        .map(field => bqfr"${valueForType(field)} as ${Ident(field.name)}")
        .mkFragment(bqfr"struct(", bqfr", ", bqfr")")

    struct(schema.fields)
  }

  // this is a user-wide query cache to speed up development/CI
  case class CachedQuery(frag: BQSqlFrag, cacheDir: Path) {
    val cacheFile = cacheDir
      .resolve("smoke-test-cache")
      .resolve(s"${frag.asStringWithUDFs.hashCode()}.json")

    def write(schema: BQSchema): IO[Path] =
      IO {
        Files.createDirectories(cacheFile.getParent)
        Files.write(
          cacheFile,
          schema.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
        )
      }

    val read: IO[Option[BQSchema]] = IO {
      if (Files.exists(cacheFile)) {
        decode[BQSchema](
          new String(Files.readAllBytes(cacheFile), StandardCharsets.UTF_8)
        ) match {
          case Left(err) =>
            System.err.println(
              s"Couldn't parse query cache file $cacheFile. Rerunning query. ${err.getMessage}"
            )
            Files.delete(cacheFile)
            None
          case Right(schema) => Some(schema)
        }
      } else None
    }
  }
}
