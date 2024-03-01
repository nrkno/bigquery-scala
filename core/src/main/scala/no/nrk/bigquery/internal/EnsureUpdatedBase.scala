/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.internal

import cats.syntax.all.*
import cats.{Applicative, MonadThrow}
import no.nrk.bigquery.*
import org.typelevel.log4cats.LoggerFactory

abstract class EnsureUpdatedBase[F[_], R, T](
    bqClient: BQAdminClientWithUnderlying[F, R, T]
)(implicit F: MonadThrow[F], lf: LoggerFactory[F]) {
  private val logger = lf.getLogger

  def check(template: BQTableDef[Any]): F[UpdateOperation[Nothing, T]] =
    bqClient.getTableWithUnderlying(template.tableId).map { maybeExisting =>
      TableUpdateOperation.from[T](template, maybeExisting)
    }

  def check(persistentRoutine: BQPersistentRoutine.Unknown): F[UpdateOperation[R, Nothing]] =
    bqClient.getRoutineWithUnderlying(persistentRoutine.name).map { maybeExisting =>
      RoutineUpdateOperation.from[R](persistentRoutine, maybeExisting)
    }

  def perform(updateOperation: UpdateOperation[R, T]): F[Unit] =
    updateOperation match {
      case UpdateOperation.Noop(_) =>
        Applicative[F].unit

      case UpdateOperation.CreateTable(to, maybePatchedTable) =>
        for {
          _ <- logger.warn(show"Creating ${to.tableId} of type ${to.getClass.getSimpleName}")
          _ <- bqClient.createTable(to)
          _ <- maybePatchedTable match {
            case Some(patchedTable) => bqClient.updateTable(patchedTable).void
            case None => Applicative[F].unit
          }
        } yield ()

      case UpdateOperation.UpdateTable(from, to) =>
        val msg =
          show"Updating ${from.our.tableId} of type ${to.getClass.getSimpleName} from ${from.our.toString}, to ${to.toString}"
        // for some obscure reason we need to create a new ExistingTable for Scala 3
        logger.warn(msg) >> bqClient.updateTableWithExisting(ExistingTable(from.our, from.table), to).void

      case UpdateOperation.CreateTvf(tvf) =>
        for {
          _ <- logger.warn(show"Creating ${tvf.name.asString} of type Tvf")
          _ <- bqClient.createRoutine(tvf)
        } yield ()

      case UpdateOperation.UpdateTvf(existing, tvf) =>
        for {
          _ <- logger.warn(show"Updating ${tvf.name.asString} of type Tvf")
          // for some obscure reason we need to create a new ExistingRoutine for Scala 3
          _ <- bqClient.updateRoutineWithExisting(ExistingRoutine(existing.our, existing.routine), tvf)
        } yield ()

      case UpdateOperation.CreatePersistentUdf(udf) =>
        for {
          _ <- logger.warn(show"Creating ${udf.name} of type PersistentUdf")
          _ <- bqClient.createRoutine(udf)
        } yield ()

      case UpdateOperation.UpdatePersistentUdf(existing, udf) =>
        for {
          _ <- logger.warn(show"Updating ${udf.name} of type PersistentUdf")
          // for some obscure reason we need to create a new ExistingRoutine for Scala 3
          _ <- bqClient.updateRoutineWithExisting(ExistingRoutine(existing.our, existing.routine), udf)
        } yield ()

      case UpdateOperation.RecreateView(from, to, createNew) =>
        val msg =
          show"Recreating ${to.tableId} of type ${to.getClass.getSimpleName} from ${from.toString}, to ${to.toString}"
        for {
          _ <- logger.warn(msg)
          _ <- bqClient.deleteTable(createNew.local.tableId)
          updated <- perform(createNew)
        } yield updated

      case UpdateOperation.Illegal(meta, reason) =>
        MonadThrow[F].raiseError(
          new RuntimeException(show"Illegal update of ${meta.identifier}: $reason")
        )

      case UpdateOperation.UnsupportedPartitioning(meta, reason) =>
        MonadThrow[F].raiseError(
          new RuntimeException(show"Illegal change of partition schema for ${meta.identifier}. $reason"))

      case UpdateOperation.IllegalSchemaExtension(meta, reason) =>
        MonadThrow[F].raiseError(
          new RuntimeException(show"Invalid table update of ${meta.identifier}: $reason")
        )
    }
}
