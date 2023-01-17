package no.nrk.bigquery
package util

import cats.Functor
import cats.effect.Sync
import cats.syntax.all._

import fs2.compression.Compression
import fs2.{Chunk, Pipe}
import io.circe.Encoder
import io.circe.syntax._
import org.typelevel.log4cats.Logger

object StreamUtils {
  val Megabyte = 1024 * 1024

  def toLineSeparatedJsonBytes[F[_]: Sync, A: Encoder](
      chunkSize: Int
  ): Pipe[F, A, Chunk[Byte]] =
    _.map(_.asJson.noSpaces)
      .intersperse("\n")
      .through(fs2.text.utf8.encode)
      .through(Compression[F].gzip())
      .chunkN(chunkSize)

  def logChunks[F[_]: Functor, T](
      logger: Logger[F],
      maybeTotal: Option[Long],
      action: String
  ): Pipe[F, Chunk[T], Chunk[T]] =
    _.evalMapAccumulate(0L) { case (acc, chunk) =>
      def format(l: Long) = l.toString

      val msg = List(
        action,
        format(chunk.size.toLong),
        s"accumulated ${format(acc + chunk.size)}",
        maybeTotal.fold("")(total => s"of $total")
      ).mkString(" ")

      logger.debug(msg).as((acc + chunk.size, chunk))
    }.map(_._2)

  def log[F[_]: Functor, T](
      logger: Logger[F],
      maybeTotal: Option[Long],
      action: T => String
  ): Pipe[F, T, T] =
    _.evalMapAccumulate(0L) { case (acc, t) =>
      def format(l: Long) = l.toString

      val msg = List(
        action(t),
        format(acc + 1),
        maybeTotal.fold("")(total => s"of $total")
      ).mkString(" ")

      logger.info(msg).as((acc + 1, t))
    }.map(_._2)
}
