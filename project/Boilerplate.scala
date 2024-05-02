import sbt.*

import java.io.File

object Boilerplate {
  import scala.StringContext.*

  implicit class BlockHelper(val sc: StringContext) extends AnyVal {
    def block(args: Any*): String = {
      val interpolated = sc.standardInterpolator(treatEscapes, args)
      val rawLines = interpolated.split('\n')
      val trimmedLines = rawLines.map(_.dropWhile(_.isWhitespace))
      trimmedLines.mkString("\n")
    }
  }
  /**
   * Return a sequence of the generated files.
   *
   * As a side-effect, it actually generates them...
   */
  def gen(dir: File): Seq[File] = {
//    val templates = scalaBinaryVersion match {
//      case "2.12" => templates212
//      case "3"    => templates213
//      case "2.13" => templates213
//    }
//    templates.map { template =>
    val template = GenProductBqRead
    val tgtFile = template.filename(dir)
    IO.write(tgtFile, template.body)
    Seq(tgtFile)
//    }
  }

//  /**
//   * Return a sequence of the generated test files.
//   *
//   * As a side-effect, it actually generates them...
//   */
//  def genTests(dir: File): Seq[File] = testTemplates.map { template =>
//    val tgtFile = template.filename(dir)
//    IO.write(tgtFile, template.body)
//    tgtFile
//  }


  val header = "// auto-generated boilerplate"
  val maxArity = 22

  trait Template {
    def filename(root: File): File

    def content(arity: Int): String

    def range: IndexedSeq[Int] = 1 to maxArity

    def body: String = {
      val headerLines = header.split('\n')
      val raw = range.map(n => content(n).split('\n').filterNot(_.isEmpty))
      val preBody = raw.head.takeWhile(_.startsWith("|")).map(_.tail)
      val instances = raw.flatMap(_.filter(_.startsWith("-")).map(_.tail))
      val postBody = raw.head.dropWhile(_.startsWith("|")).dropWhile(_.startsWith("-")).map(_.tail)
      (headerLines ++ preBody ++ instances ++ postBody).mkString("\n")
    }
  }

  object GenProductBqRead extends Template {
    override def range: IndexedSeq[Int] = 1 to maxArity

    def filename(root: File): File = root / "no" / "nrk" / "bigquery" / "ProductBQRead.scala"

    def content(arity: Int): String = {
      val synTypes = (0 until arity).map(n => s"A$n")
      val `A..N` = synTypes.mkString(", ")
      val instances = synTypes.map(tpe => s"bqRead$tpe: BQRead[$tpe]").mkString(", ")
      val memberNames = synTypes.map(tpe => s"name$tpe: String").mkString(", ")
      val fieldTypes = synTypes.map(tpe => s"name$tpe -> bqRead$tpe.bqType").mkString(", ")
      val getFields = (0 until arity).map(n => s"getField[A$n]($n)").mkString(", ")

      block"""
        |package no.nrk.bigquery
        |
        |import no.nrk.bigquery.BQRead.firstNotNullable
        |import org.apache.avro.Schema
        |import org.apache.avro.generic.GenericRecord
        |
        |private[bigquery] trait ProductBQRead {
        -  /**
        -   * @group Product
        -   */
        -  final def forProduct$arity[Target, ${`A..N`}]($memberNames)(f: (${`A..N`}) => Target)(implicit
        -    $instances
        -  ): BQRead[Target] = new BQRead[Target] {
        -      override val bqType: BQType = BQType(
        -          BQField.Mode.REQUIRED,
        -          BQField.Type.STRUCT,
        -          List($fieldTypes)
        -       )
        -      override def read(transportSchema: Schema, value: Any): Target =
        -        value match {
        -          case coll: GenericRecord =>
        -            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
        -            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
        -            f($getFields)
        -          case other => sys.error(s"Unexpected: $${other.getClass.getSimpleName} $$other . Schema from BQ: $$transportSchema")
        -        }
        -    }
        |}
      """
    }
  }
}
