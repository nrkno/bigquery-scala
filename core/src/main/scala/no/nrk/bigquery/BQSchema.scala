package no.nrk.bigquery

import com.google.cloud.bigquery.Schema
import io.circe.{Decoder, Encoder}

import scala.jdk.CollectionConverters.ListHasAsScala

case class BQSchema(fields: List[BQField]) {
  def toSchema: Schema =
    Schema.of(fields.map(_.toField): _*)

  /** This is useful for views, because we're unable to create views with
    * required columns.
    *
    * This is enforced by BQ at create-time because we *have* to create the view
    * without a schema, and then we may optionally update the view to add our
    * schema, and in that case it can only legally extend what was inferred.
    *
    * @return
    *   a version of the BQSchema with all fields set to nullable. repeated
    *   fields are still allowed.
    */
  def recursivelyNullable: BQSchema =
    copy(fields = fields.map(_.recursivelyNullable))

  def extend(additional: BQSchema) = BQSchema(fields ::: additional.fields)
}

object BQSchema {
  implicit val decodes: Decoder[BQSchema] = Decoder.forProduct1("fields")(apply)
  implicit val encodes: Encoder[BQSchema] =
    Encoder.forProduct1("fields")(_.fields)

  def of(fields: BQField*) = new BQSchema(fields.toList)

  def fromSchema(schema: Schema): BQSchema =
    schema match {
      case null => BQSchema(Nil)
      case schema =>
        BQSchema(schema.getFields.asScala.toList.map(BQField.fromField))
    }
}
