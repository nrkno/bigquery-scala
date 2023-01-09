package no.nrk.bigquery
package util

import com.google.cloud.bigquery.{Field, StandardSQLTypeName}

object BqSqlProjection {
  case class TypedFragment(fragment: BQSqlFrag, tpe: BQField)

  def apply(field: BQField)(f: BQField => Op): Option[TypedFragment] =
    impl.recurse(Nil, numIndent = 0, field, f).forceField.maybeSelector

  sealed trait Op
  case object Keep extends Op
  case object Drop extends Op
  case class Rename(to: Ident) extends Op
  case class Flatten(prefix: Option[Ident]) extends Op

  private object impl {
    type ReversedQualifiedIndent = List[Ident]

    def recurse(prefix: ReversedQualifiedIndent, numIndent: Int, field: BQField, f: BQField => Op): Selected = {
      val newPrefix = field.ident :: prefix
      val indent = BQSqlFrag(" " * numIndent)

      def baseCase: SelectedField = {
        val selector = bqfr"$indent${newPrefix.reverse.mkFragment(".")}"
        SelectedField(Some(TypedFragment(selector, field)), maybeAlias = None, wasRewritten = false)
      }

      field match {
        case field if field.mode == Field.Mode.REPEATED =>
          // invent a field for the current element in the array
          val arrayElement = field.copy(name = s"${field.name}Elem", mode = Field.Mode.REQUIRED)

          val selectedElement = recurse(Nil, numIndent + 2, arrayElement, f)

          // we'll flatten an array of structs with one field to array of that field
          val isSingletonStruct: Option[TypedFragment] =
            selectedElement match {
              case SelectedObject(selected, _) =>
                selected.flatMap { case (_, selected) => selected.forceField.maybeSelector } match {
                  case List(one) => Some(one)
                  case _         => None
                }
              case _ => None
            }

          isSingletonStruct match {
            case Some(uniqueSelector) =>
              val arraySelector =
                bqsql"""|$indent(SELECT ARRAY_AGG( # start array ${field.ident}
                        |${uniqueSelector.fragment}
                        |$indent) FROM UNNEST(${newPrefix.reverse.mkFragment(".")}) ${arrayElement.ident})""".stripMargin
              val typedArraySelector = TypedFragment(arraySelector, uniqueSelector.tpe.copy(name = field.name, mode = Field.Mode.REPEATED))
              SelectedField(Some(typedArraySelector), Some(field.ident), wasRewritten = true)
            case None =>
              selectedElement.forceField match {
                case SelectedField(Some(elemSelector), _, true) =>
                  val arraySelector =
                    bqsql"""|$indent(SELECT ARRAY_AGG( # start array ${field.ident}
                            |${elemSelector.fragment}
                            |$indent) FROM UNNEST(${newPrefix.reverse.mkFragment(".")}) ${arrayElement.ident})""".stripMargin
                  val typedArraySelector = TypedFragment(arraySelector, elemSelector.tpe.copy(name = field.name, mode = Field.Mode.REPEATED))
                  SelectedField(Some(typedArraySelector), Some(field.ident), wasRewritten = true)
                case _ =>
                  baseCase
              }
          }

        case field if field.tpe == StandardSQLTypeName.STRUCT =>
          val selecteds: List[(Ident, Selected)] =
            field.subFields.flatMap { subField: BQField =>
              f(subField) match {
                case Keep =>
                  val selectedSubField = recurse(newPrefix, numIndent + 2, subField, f)

                  // cosmetic: add alias only if needed
                  val selectedAliasedSubField =
                    if (subField.tpe == StandardSQLTypeName.STRUCT || subField.tpe == StandardSQLTypeName.ARRAY || subField.mode == Field.Mode.REPEATED)
                      selectedSubField.forceField.withAlias(Some(subField.ident))
                    else selectedSubField

                  List((subField.ident, selectedAliasedSubField))
                case Drop =>
                  // return this so we can determine further out that this structure was changed
                  List((subField.ident, Selected.Dropped))

                case Rename(to) =>
                  val selected = recurse(newPrefix, numIndent + 2, subField, f)
                  val renamedSelected = selected.forceField.renamed(to)
                  List((to, renamedSelected))

                case Flatten(maybePrefix) =>
                  recurse(newPrefix, numIndent, subField, f) match {
                    case obj: SelectedObject =>
                      maybePrefix match {
                        case Some(prefix) =>
                          obj.selecteds.map { case (name, selected) =>
                            val newName = Ident(prefix.value + name.value.capitalize)
                            (newName, selected.forceField.renamed(newName))
                          }
                        case None => obj.selecteds
                      }

                    case _: SelectedField =>
                      sys.error(s"Expected $subField to be a struct when flattening")
                  }
              }
            }

          // delay execution of this so implementation of Flatten can unpack it
          def mkField(obj: SelectedObject): SelectedField = {
            val selectedFields: List[SelectedField] = obj.selecteds.map { case (_, selected) => selected.forceField }

            selectedFields.flatMap(_.aliasedSelector) match {
              case Nil => Selected.Dropped
              case nonEmpty =>
                if (selectedFields.exists(_.wasRewritten)) {
                  val droppedComment = obj.selecteds.collect { case (name, Selected.Dropped) => name } match {
                    case Nil      => BQSqlFrag.Empty
                    case nonEmpty => bqfr"(dropped ${nonEmpty.mkFragment(", ")})"
                  }
                  val frag =
                    bqfr"""|$indent(SELECT AS STRUCT # start struct ${field.ident} $droppedComment
                           |${nonEmpty.mkFragment(",\n")}
                           |$indent)""".stripMargin

                  val tpe = field.copy(subFields = selectedFields.flatMap(_.maybeSelector).map(_.tpe))
                  val typedFragment = TypedFragment(frag, tpe)

                  SelectedField(Some(typedFragment), Some(field.ident), wasRewritten = true)
                } else baseCase
            }
          }

          SelectedObject(selecteds, mkField)
        case _ =>
          baseCase
      }
    }

    sealed trait Selected {
      def forceField: SelectedField
    }

    object Selected {
      val Dropped = SelectedField(None, None, wasRewritten = true)
    }

    // we only need this case so that implementation of `Op.Flatten` can unpack it
    case class SelectedObject(selecteds: List[(Ident, Selected)], mkField: SelectedObject => SelectedField) extends Selected {
      def forceField: SelectedField = mkField(this)
    }

    /** @param maybeSelector
      *   an sql fragments which selects the given field from the source structure
      * @param maybeAlias
      *   optionally alias name. sometimes this is needed for renames, or when un-/repacking structs and arrays
      * @param wasRewritten
      *   if we havent changed anything inside this field, we're free to choose a shorter syntax upstream
      */
    case class SelectedField(maybeSelector: Option[TypedFragment], maybeAlias: Option[Ident], wasRewritten: Boolean) extends Selected {
      override def forceField: SelectedField = this

      def withAlias(outFieldName: Option[Ident]): SelectedField =
        SelectedField(maybeSelector, outFieldName, wasRewritten)

      def renamed(outFieldName: Ident): SelectedField =
        copy(
          maybeSelector = maybeSelector.map(typedFragment => typedFragment.copy(tpe = typedFragment.tpe.copy(name = outFieldName.value))),
          maybeAlias = Some(outFieldName),
          wasRewritten = true
        )

      def aliasedSelector: Option[BQSqlFrag] =
        maybeSelector.map { case TypedFragment(frag, _) =>
          maybeAlias match {
            case Some(value) => bqfr"$frag $value"
            case None        => frag
          }
        }
    }
  }
}
