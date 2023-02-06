package no.nrk.bigquery

import cats.syntax.show._
import com.google.cloud.bigquery.{Field, StandardSQLTypeName}
import io.circe.{Decoder, Encoder}
import no.nrk.bigquery.implicits.assertIsUsed

package object testing {
  def tempTable(partitionId: BQPartitionId[Any]): Ident =
    Ident(partitionId.show.filter(c => c.isLetterOrDigit || c == '_'))

  implicit val encoderIdent: Encoder[Ident] = Encoder.encodeString.contramap(_.value)

  implicit lazy val encodeField: Encoder[BQField] = {
    implicit val encodesStandardSQLTypeName: Encoder[StandardSQLTypeName] =
      Encoder[String].contramap(_.name())
    implicit val encodesFieldMode: Encoder[Field.Mode] =
      Encoder[String].contramap(_.name())
    assertIsUsed(encodesStandardSQLTypeName, encodesFieldMode)
    io.circe.generic.semiauto.deriveEncoder
  }

  implicit lazy val decodeField: Decoder[BQField] = {
    implicit val decodesStandardSQLTypeName: Decoder[StandardSQLTypeName] =
      Decoder[String].map(StandardSQLTypeName.valueOf)
    implicit val decodesFieldMode: Decoder[Field.Mode] =
      Decoder[String].map(Field.Mode.valueOf)
    assertIsUsed(decodesStandardSQLTypeName, decodesFieldMode)
    io.circe.generic.semiauto.deriveDecoder
  }

  implicit val decodeSchema: Decoder[BQSchema] = Decoder.forProduct1("fields")(BQSchema.apply)
  implicit val encodeSchema: Encoder[BQSchema] = Encoder.forProduct1("fields")(_.fields)
}
