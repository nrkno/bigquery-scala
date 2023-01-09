package no.nrk.bigquery
package util

import cats.effect.IO
import fs2.compression.Compression
import fs2.{Chunk, Pipe}
import io.circe.Encoder
import io.circe.syntax._
import org.typelevel.log4cats.Logger

import scala.io.Codec

object StreamUtils {
  val Megabyte = 1024 * 1024

  def toLineSeparatedJsonBytes[A: Encoder](
      chunkSize: Int
  ): Pipe[IO, A, Chunk[Byte]] =
    _.map(_.asJson.noSpaces)
      .intersperse("\n")
      .through(fs2.text.utf8.encode)
      .through(Compression[IO].gzip())
      .chunkN(chunkSize)

  def iso8859Decode[F[_]]: Pipe[F, Byte, String] =
    _.through(
      _.chunks.map((bytes: Chunk[Byte]) =>
        new String(bytes.toArray, Codec.ISO8859.charSet)
      )
    )

  def logChunks[T](
      logger: Logger[IO],
      maybeTotal: Option[Long],
      action: String
  ): Pipe[IO, Chunk[T], Chunk[T]] =
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

  def log[T](
      logger: Logger[IO],
      maybeTotal: Option[Long],
      action: T => String
  ): Pipe[IO, T, T] =
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
