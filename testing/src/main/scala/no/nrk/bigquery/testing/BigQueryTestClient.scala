/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package testing

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.Stream
import io.circe.Encoder
import org.apache.avro
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jFactory

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
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

  def cachingClient(
      queryCachePath: Path,
      cacheFrom: Resource[IO, QueryClient[IO]]
  ): Resource[IO, QueryClient[IO]] =
    cacheFrom.map(client =>
      new QueryClient[IO] {
        override type LoadStatistics = client.LoadStatistics
        override type Job = client.Job

        override protected[bigquery] def synchronousQueryExecute(
            jobId: BQJobId,
            query: BQSqlFrag,
            legacySql: Boolean,
            logStream: Boolean): Resource[IO, (Schema, Stream[IO, GenericRecord])] = {
          val hash =
            java.util.Objects.hash(query, Boolean.box(legacySql))
          val hashedSchemaPath =
            queryCachePath.resolve(s"${jobId.name}__$hash.json")
          val hashedRowsPath =
            queryCachePath.resolve(s"${jobId.name}__$hash.avro")

          def runAndStore: Resource[IO, (avro.Schema, Stream[IO, GenericRecord])] =
            for {
              tuple <- client
                .synchronousQueryExecute(
                  jobId,
                  query,
                  legacySql,
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

        override def loadToHashedPartition[A](
            jobId: BQJobId,
            table: BQTableDef.Table[Long],
            stream: Stream[IO, A],
            logStream: Boolean,
            chunkSize: Int)(implicit hashedEncoder: HashedPartitionEncoder[A]): IO[Option[LoadStatistics]] =
          client.loadToHashedPartition(jobId, table, stream, logStream, chunkSize)

        override def loadJson[A: Encoder, P: TableOps](
            jobId: BQJobId,
            table: BQTableDef.Table[P],
            partition: P,
            stream: Stream[IO, A],
            writeDisposition: WriteDisposition,
            logStream: Boolean,
            chunkSize: Int): IO[Option[LoadStatistics]] =
          client.loadJson(jobId, table, partition, stream, writeDisposition, logStream, chunkSize)

        override def submitQuery[P](
            id: BQJobId,
            query: BQSqlFrag,
            destination: Option[BQPartitionId[P]],
            writeDisposition: Option[WriteDisposition]): IO[Job] =
          client.submitQuery(id, query, destination, writeDisposition)

        override def dryRun(id: BQJobId, query: BQSqlFrag): IO[(BQSchema, Job)] =
          client.dryRun(id, query)

        override def createTempTable[Param](
            table: BQTableDef.Table[Param],
            tmpDataset: BQDataset.Ref,
            expirationDuration: Option[FiniteDuration]): IO[BQTableDef.Table[Param]] =
          client.createTempTable(table, tmpDataset, expirationDuration)

        override def createTempTableResource[Param](
            table: BQTableDef.Table[Param],
            tmpDataset: BQDataset.Ref): Resource[IO, BQTableDef.Table[Param]] =
          client.createTempTableResource(table, tmpDataset)
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
