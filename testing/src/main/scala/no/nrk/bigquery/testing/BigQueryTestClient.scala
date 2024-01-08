/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package testing

import cats.data.OptionT
import cats.effect.{IO, Resource}
import cats.syntax.all._
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.bigquery.BigQuery.JobOption
import fs2.Stream
import no.nrk.bigquery.metrics.MetricsOps
import org.apache.avro
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jFactory

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Properties

object BigQueryTestClient {
  private implicit val loggerFactory: Slf4jFactory[IO] = Slf4jFactory.create[IO]
  private val logger: SelfAwareStructuredLogger[IO] = loggerFactory.getLogger

  val basedir =
    Paths
      .get(sys.env.getOrElse("BIGQUERY_HOME", Properties.userHome))
      .resolve(".bigquery")
  val queryCachePath = {
    val dir = basedir.resolve("query-cache")
    Files.createDirectories(dir)
    dir
  }

  private def credentialsFromString(
      str: String
  ): IO[ServiceAccountCredentials] =
    IO.blocking(
      ServiceAccountCredentials.fromStream(
        new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8))
      )
    )

  def testClient: Resource[IO, BigQueryClient[IO]] =
    for {
      credentials <- Resource.eval(
        OptionT(IO(sys.env.get("BIGQUERY_SERVICE_ACCOUNT")))
          .semiflatMap(credentialsFromString)
          .getOrElseF(
            IO.blocking(GoogleCredentials.getApplicationDefault)
          )
      )
      underlying <- BigQueryClient.resource(credentials, MetricsOps.noop[IO])
    } yield underlying

  def cachingClient(
      cacheFrom: Resource[IO, BigQueryClient[IO]]
  ): Resource[IO, BigQueryClient[IO]] =
    cacheFrom.map(client =>
      new BigQueryClient(client.underlying, client.reader, client.metricOps, None) {
        override protected def synchronousQueryExecute(
            jobId: BQJobId,
            query: BQSqlFrag,
            legacySql: Boolean,
            jobOptions: Seq[JobOption],
            logStream: Boolean
        ): Resource[IO, (avro.Schema, Stream[IO, GenericRecord])] = {
          val hash =
            java.util.Objects.hash(query, Boolean.box(legacySql), jobOptions)
          val hashedSchemaPath =
            queryCachePath.resolve(s"${jobId.name}__$hash.json")
          val hashedRowsPath =
            queryCachePath.resolve(s"${jobId.name}__$hash.avro")

          def runAndStore: Resource[IO, (avro.Schema, Stream[IO, GenericRecord])] =
            for {
              tuple <- super
                .synchronousQueryExecute(
                  jobId,
                  query,
                  legacySql,
                  jobOptions,
                  logStream
                )
              (schema, rowStream) = tuple
              _ <- Resource.liftK(serializeSchema(hashedSchemaPath, schema))
              rows <- Resource.liftK(rowStream.compile.toVector)
              _ <- Resource.liftK(serializeRows(hashedRowsPath, schema, rows))
            } yield (schema, Stream.emits(rows))

          val deserialize: IO[(avro.Schema, Vector[GenericRecord])] =
            for {
              schema <- deserializeSchema(hashedSchemaPath)
              rows <- deserializeRows(hashedRowsPath, schema).compile.toVector
            } yield (schema, rows)

          for {
            maybeExisting <- Resource.liftK(deserialize.attempt)
            res <- maybeExisting match {
              case Left(th) =>
                Resource.liftK(
                  logger.warn(s"Couldn't decode cached query: ${th.getMessage}")
                ) *> runAndStore
              case Right((schema, vectorRows)) =>
                Resource.pure[IO, (avro.Schema, Stream[IO, GenericRecord])](
                  (schema, Stream.emits(vectorRows))
                )
            }
          } yield res
        }
      })

  def serializeSchema(path: Path, schema: avro.Schema): IO[Unit] =
    IO.blocking {
      Files.createDirectories(path.getParent)
      Files.write(path, schema.toString(true).getBytes(StandardCharsets.UTF_8))
      ()
    }

  def deserializeSchema(path: Path): IO[avro.Schema] =
    IO.blocking(new avro.Schema.Parser().parse(Files.newInputStream(path)))

  def serializeRows(
      path: Path,
      schema: avro.Schema,
      rows: Vector[GenericRecord]
  ): IO[Unit] =
    IO.blocking {
      Files.createDirectories(path.getParent)
      val fileWriter =
        new DataFileWriter(new GenericDatumWriter[GenericRecord](schema))
      fileWriter.create(schema, path.toFile)
      rows.foreach(fileWriter.append)
      fileWriter.close()
    }

  def deserializeRows(
      path: Path,
      schema: avro.Schema
  ): Stream[IO, GenericRecord] = {
    val reader: Resource[IO, DataFileReader[GenericRecord]] =
      Resource.fromAutoCloseable(
        IO.blocking(
          new DataFileReader(
            path.toFile,
            new GenericDatumReader[GenericRecord](schema)
          )
        )
      )

    Stream.resource(reader).flatMap { reader =>
      Stream.fromBlockingIterator[IO].apply(reader.iterator().asScala, 1000)
    }
  }
}
