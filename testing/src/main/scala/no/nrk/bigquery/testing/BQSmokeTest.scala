/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package testing

import cats.effect.{IO, Resource}
import cats.effect.kernel.Outcome
import cats.syntax.alternative.*
import io.circe.parser.decode
import io.circe.syntax.*
import munit.Assertions.{clues, fail}
import munit.catseffect.IOFixture
import munit.{CatsEffectSuite, Clues, Location}
import no.nrk.bigquery.UDF.Body
import no.nrk.bigquery.*
import no.nrk.bigquery.testing.BQSmokeTest.{CheckType, bqCheckFragment}
import org.typelevel.log4cats.slf4j.*
import no.nrk.bigquery.syntax.*
import no.nrk.bigquery.util.{IndexSeqSizedBuilder, Nat, Sized}
import org.typelevel.log4cats.SelfAwareStructuredLogger

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

abstract class BQSmokeTest(testClient: Resource[IO, QueryClient[IO]]) extends CatsEffectSuite with GeneratedTest {
  self =>

  override def testType: String = "big-query"

  private val builder: IndexSeqSizedBuilder[BQSqlFrag.Magnet] = new IndexSeqSizedBuilder[BQSqlFrag.Magnet]

  val assertStableTables: List[BQTableLike[Any]] = Nil

  object StaticQueries extends GeneratedTest {
    override def basedir: Path = self.basedir
    override def testType: String = "bq-query-static"
    override def BQDatasetName: String = self.BQDatasetName
  }

  object Queries extends GeneratedTest {
    override def basedir: Path = self.basedir
    override def testType: String = "bq-query"
    override def BQDatasetName: String = self.BQDatasetName
  }

  object ExampleQueries extends GeneratedTest {
    override def basedir: Path = self.basedir
    override def testType: String = "bq-example-query"
    override def BQDatasetName: String = self.BQDatasetName
  }

  val bqClient: IOFixture[QueryClient[IO]] = ResourceSuiteLocalFixture(
    "bqClient",
    testClient
  )

  override def munitFixtures = List(bqClient)

  protected def bqTypeCheckTest[A](
      testName: String
  )(query: BQQuery[A])(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      bqCheckFragment(
        testName,
        query.sql,
        CheckType.TypeOnly(query.bqRead.bqType),
        assertStableTables,
        Queries,
        StaticQueries
      )(bqClient())
    }

  @deprecated("use bqTypeChecksTest", "0.4.6")
  protected def bqChecksTest[A](
      testName: String
  )(queries: List[BQQuery[A]])(implicit loc: Location): Unit =
    bqTypeChecksTest[A](testName)(queries)

  protected def bqTypeChecksTest[A](
      testName: String
  )(queries: List[BQQuery[A]])(implicit loc: Location): Unit =
    test(s"bqChecks: $testName".tag(TestTags.Generated)) {
      val combinedSql = queries
        .map(_.sql)
        .mkFragment(bqfr"SELECT * FROM (", bqfr") UNION ALL (", bqfr")")
      bqCheckFragment(
        testName,
        combinedSql,
        CheckType.TypeOnly(queries.head.bqRead.bqType),
        assertStableTables,
        Queries,
        StaticQueries
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
        Queries,
        StaticQueries
      )(bqClient())
    }

  protected def bqTypeWithNameCheckTest[A](
      testName: String
  )(query: BQQuery[A])(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      bqCheckFragment(
        testName,
        query.sql,
        CheckType.TypeAndName(query.bqRead.bqType),
        assertStableTables,
        Queries,
        StaticQueries
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
        ExampleQueries,
        StaticQueries
      )(bqClient())
    }

  // note that we just compare the generated sql for legacy sql. neither dry runs nor schemas work
  protected def bqCheckLegacyFragmentTest[A](
      testName: String
  )(frag: BQSqlFrag)(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      Queries.writeAndCompare(
        Queries.testFileForName(s"$testName.sql"),
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
        Queries,
        StaticQueries
      )(bqClient())
    }

  protected def bqCheckFill(
      testName: String
  )(fill: BQFill[Any])(implicit loc: Location): Unit =
    test(s"bqCheck: $testName".tag(TestTags.Generated)) {
      bqCheckFragment(
        testName,
        fill.query,
        CheckType.Schema(fill.tableDef.schema),
        assertStableTables,
        Queries,
        StaticQueries
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
        Queries,
        StaticQueries
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
        Queries,
        StaticQueries
      )(bqClient()).attempt
        .flatMap {
          case Left(e: BQExecutionException) =>
            IO(assert(e.main.flatMap(_.message).getOrElse("").contains(errorFragment)))
          case Left(e: BQException) if e.message.isDefined =>
            IO(assert(e.message.getOrElse("").contains(errorFragment)))
          case Left(other) => IO.raiseError(other)
          case Right(_) =>
            IO(
              fail(
                s"Expected BQ to report failure with the substring '$errorFragment'"
              )
            )
        }
    }

  protected def bqCheckTableValueFunction[N <: Nat](testName: String, tvf: TVF[?, N])(
      args: IndexSeqSizedBuilder[BQSqlFrag.Magnet] => Sized[IndexedSeq[BQSqlFrag.Magnet], N]
  )(implicit loc: Location): Unit = {
    val values =
      tvf.params.unsized
        .zip(args(builder).unsized)
        .map { case (param, arg) => bqfr"declare ${param.name} default ${arg};" }
        .mkFragment("\n")

    bqCheckFragmentTest(testName)(
      bqfr"""|$values
             |${tvf.query}
          """.stripMargin -> tvf.schema
    )
  }

}

object BQSmokeTest extends BQDatasetDirectoryProvider {
  self =>

  private val logger: SelfAwareStructuredLogger[IO] = Slf4jFactory.create[IO].getLogger

  def bqCheckFragment(
      testName: String,
      frag: BQSqlFrag,
      checkType: CheckType,
      assertStable: Seq[BQTableLike[Any]],
      target: GeneratedTest,
      static: GeneratedTest
  ): QueryClient[IO] => IO[Unit] = { bqClient =>
    val compareAsIs = IO(
      target.writeAndCompare(
        target.testFileForName(s"$testName.sql"),
        frag.asStringWithUDFs
      )
    )

    dependenciesAsStaticData(frag, assertStable) match {
      case Right(staticFrag) =>
        val compareStatic = IO(
          static.writeAndCompare(
            static.testFileForName(s"$testName.sql"),
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
                .dryRun(BQJobId("smoketest"), staticFrag)
                .map(stats => stats.schema.getOrElse(BQSchema.of()))
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
        val log = logger.warn(s"Running $testName because $notStaticBecause")

        val runCheck = bqClient
          .dryRun(BQJobId("smoketest"), frag)
          .guaranteeCase {
            case Outcome.Errored(_) if checkType != CheckType.Failing =>
              IO(println(s"failed query: ${frag.asStringWithUDFs}"))
            case _ => IO.unit
          }
          .map(stats => stats.schema.getOrElse(BQSchema.of()))
          .map(checkType.checkSchema)

        log *> compareAsIs *> runCheck
    }
  }

  sealed trait CheckType {
    def checkSchema(actualSchema: BQSchema): Unit =
      this match {
        case CheckType.Schema(expectedSchema) =>
          conforms.onlyTypes(actualSchema, expectedSchema) match {
            case Some(reasons) =>
              fail(s"Failed because ${reasons.mkString(", ")}", TypeClue(expectedSchema, actualSchema))
            case None => assert(true)
          }
        case CheckType.TypeOnly(expectedType) =>
          conforms.onlyTypes(actualSchema, expectedType) match {
            case Some(reasons) =>
              fail(s"Failed because ${reasons.mkString(", ")}", TypeClue(expectedType, actualSchema))
            case None => assert(true)
          }

        case CheckType.TypeAndName(expectedType) =>
          conforms.types(actualSchema, expectedType) match {
            case Some(reasons) =>
              fail(s"Failed because ${reasons.mkString(", ")}", TypeClue(expectedType, actualSchema))
            case None => assert(true)
          }
        case CheckType.Untyped | CheckType.Failing =>
          assert(true)
      }
  }

  private case class TypeClue(fields: List[TypeClue.Field])

  private object TypeClue {

    case class Field(
        name: String,
        tpe: BQField.Type,
        subFields: List[Field]
    )

    def apply(expectedSchema: BQSchema, actualSchema: BQSchema): Clues = {
      val actualFields = TypeClue.from(actualSchema)
      val expectedFields = TypeClue.from(expectedSchema)
      clues(expectedFields, actualFields)
    }
    def apply(expectedType: BQType, actualSchema: BQSchema): Clues = {
      val actualFields = TypeClue.from(actualSchema)
      val expectedFields = TypeClue.from(expectedType)
      clues(expectedFields, actualFields)
    }

    private def from(s: BQSchema): TypeClue = {
      def toField(f: BQField): Field = Field(f.name, f.tpe, f.subFields.map(toField))
      TypeClue(s.fields.map(toField))
    }

    private def from(t: BQType): TypeClue = {
      def toField(tup: (String, BQType)): Field = Field(tup._1, tup._2.tpe, tup._2.subFields.map(toField))
      if (t.subFields.isEmpty) TypeClue(toField(("_", t)) :: Nil)
      else TypeClue(t.subFields.map(toField))
    }

  }

  object CheckType {
    case class Schema(value: BQSchema) extends CheckType
    case class TypeOnly(value: BQType) extends CheckType
    case class TypeAndName(value: BQType) extends CheckType
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
        s"References unstable tables ${unstableTables.map(_.tableId.asString).toList.sorted.mkString(", ")}"
      )
    else
      Right(
        structured.copy(ctes = CTEList(ctes.distinct ++ structured.ctes.value, structured.ctes.recursive)).asFragment)
  }

  private def recurse(frag: BQSqlFrag): (BQSqlFrag, List[CTE]) =
    frag match {
      case x @ BQSqlFrag.Frag(_) =>
        (x, Nil)

      case BQSqlFrag.Call(udf, args) =>
        val (newArgs, ctes) = args.map(recurse).separate
        udf match {
          case tUdf @ UDF.Temporary(_, _, Body.Sql(body), _) =>
            val (newUdfBody, ctesFromUDF) = recurse(body)
            (
              BQSqlFrag.Call(tUdf.copy(body = UDF.Body.Sql(newUdfBody)), newArgs),
              ctesFromUDF ++ ctes.flatten
            )
          case pUdf @ UDF.Persistent(_, _, Body.Sql(body), _, _) =>
            val (newUdfBody, ctesFromUDF) = recurse(body)
            (
              BQSqlFrag.Call(
                pUdf.copy(body = UDF.Body.Sql(newUdfBody)).convertToTemporary,
                newArgs
              ),
              ctesFromUDF ++ ctes.flatten
            )
          case _ =>
            (BQSqlFrag.Call(udf, newArgs), ctes.flatten)
        }

      case BQSqlFrag.Combined(frags) =>
        val allDataRefs = frags.forall {
          case BQSqlFrag.PartitionRef(_) => true
          case BQSqlFrag.FillRef(_) => true
          case _ => false
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
            case BQTableRef(_, _, _) => None
            case x: BQAppliedTableValuedFunction[Any] => Some(x.schema)
            case x: BQTableDef[Any] => Some(x.schema)
          }

        schemaOpt match {
          case Some(schema) =>
            val cteName = tempTable(pid)
            (
              cteName.bqShow,
              List(CTE(cteName, bqfr"(select ${exampleRow(schema)})"))
            )
          case None =>
            (p, Nil)
        }

      case BQSqlFrag.TableRef(table) =>
        recurse(table.unpartitioned.assertPartition.bqShow)

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
              bqfr"(select ${exampleRow(fill.tableDef.schema)})"
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
        case BQField.Type.JSON => BQSqlFrag("""JSON '{"foo": "bar"}'""")
        case BQField.Type.BOOL => true.bqShow
        case BQField.Type.INT64 => counter.next().bqShow
        case BQField.Type.FLOAT64 => (counter.next() + 0.5).bqShow
        case BQField.Type.NUMERIC => counter.next().bqShow
        case BQField.Type.BIGNUMERIC => counter.next().bqShow
        case BQField.Type.STRING => StringValue(field.name).bqShow
        case BQField.Type.BYTES =>
          BQSqlFrag(
            "(select HLL_COUNT.INIT(x) from unnest(['a']) x)"
          ).bqShow // how to write a literal?
        case BQField.Type.STRUCT => struct(field.subFields)
        case BQField.Type.ARRAY =>
          List
            .range(0, 2)
            .map(_ => valueForType(field.copy(tpe = field.subFields.head.tpe)))
            .mkFragment("[", ", ", "]")
        case BQField.Type.TIMESTAMP =>
          BQSqlFrag("TIMESTAMP('2020-01-01 00:00:00+00')")
        case BQField.Type.DATE => BQSqlFrag("DATE(2020, 1, 1)")
        case BQField.Type.TIME => BQSqlFrag("TIME(12, 0, 0)")
        case BQField.Type.DATETIME =>
          BQSqlFrag("DATETIME(2020, 1, 1, 00, 00, 00)")
        case BQField.Type.GEOGRAPHY =>
          BQSqlFrag("ST_GeogFromText('POINT(0 0)')")
        case BQField.Type.INTERVAL =>
          BQSqlFrag("MAKE_INTERVAL(1, 6, 15)")
        case BQField.Type.RANGE =>
          counter.next().bqShow
      }

      field.mode match {
        case BQField.Mode.REPEATED =>
          List.range(0, 2).map(_ => base).mkFragment("[", ", ", "]")
        case _ => base
      }
    }

    def struct(fields: List[BQField]): BQSqlFrag =
      fields
        .map(field => bqfr"${valueForType(field)} as ${Ident(field.name)}")
        .mkFragment(bqfr"struct(", bqfr", ", bqfr")")

    schema.fields
      .map(field => bqfr"${valueForType(field)} as ${Ident(field.name)}")
      .mkFragment(",")
  }

  // this is a user-wide query cache to speed up development/CI
  case class CachedQuery(frag: BQSqlFrag, cacheDir: Path)(using givenBQDatasetName: String = self.BQDatasetName) {
    val cacheFile =
      if (useDatasetDir) {
        cacheDir
          .resolve("smoke-test-cache")
          .resolve(givenBQDatasetName)
          .resolve(s"${frag.asStringWithUDFs.hashCode()}.json")
      } else {
        cacheDir
          .resolve("smoke-test-cache")
          .resolve(s"${frag.asStringWithUDFs.hashCode()}.json")
      }

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
