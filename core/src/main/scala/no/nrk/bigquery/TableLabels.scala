package no.nrk.bigquery

import cats.syntax.show._
import com.google.cloud.bigquery.TableInfo

import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters._

/** @param values sorted for consistent behaviour */
case class TableLabels(values: SortedMap[String, String]) {
  // https://cloud.google.com/bigquery/docs/labels-intro#requirements
  def verify(tableId: BQTableId): Unit = {
    def fail(msg: String) =
      System.err.println(show"Table $tableId: requirement failed: $msg")

    values.foreach { case (key, value) =>
      // Keys have a minimum length of 1 character and a maximum length of 63 characters,
      if (key.isEmpty)
        fail("label key is empty")
      if (key.length > 63)
        fail(show"label key $key is longer than 63 chars")

      // Values can be empty, and have a maximum length of 63 characters.
      if (value.length > 63)
        fail(show"label value $value is longer than 63 chars")

      // Keys and values can contain only lowercase letters, numeric characters, underscores, and dashes. All characters must use UTF-8 encoding, and international characters are allowed.
      if (!key.forall(c => c.isLower || c.isDigit || c == '-' || c == '_'))
        fail(
          show"label key $key can contain only lowercase letters, numeric characters, underscores, and dashes"
        )
      if (!value.forall(c => c.isLower || c.isDigit || c == '-' || c == '_'))
        fail(
          show"label value $value can contain only lowercase letters, numeric characters, underscores, and dashes"
        )

      // Keys must start with a lowercase letter or international character.
      if (!key(0).isLower)
        fail(
          show"label key $key must start with lowercase letter or international character"
        )
    }
  }

  def withAll(moreLabels: Iterable[(String, String)]): TableLabels =
    TableLabels(values ++ moreLabels)

  def ++(other: TableLabels): TableLabels =
    withAll(other.values)

  /** This method is needed for the case where we delete a label. It is deleted by setting it to `null`.
    *
    * As such, we need to know the labels of a table in production before we compute the new set of labels to use when
    * updating
    */
  def forUpdate(
      maybeExistingTable: Option[BQTableDef[Any]]
  ): java.util.Map[String, String] = {
    val ret = new java.util.TreeMap[String, String]

    // existing table? set all old label keys to `null`
    for {
      existingTable <- maybeExistingTable.toList
      existingKey <- existingTable.labels.values.keySet
    } ret.put(existingKey, null)

    // and overwrite the ones we want to keep.
    values.foreach { case (key, value) => ret.put(key, value) }

    ret
  }
}

object TableLabels {
  val Empty: TableLabels = new TableLabels(SortedMap.empty)

  /* nicer syntax for creating instances */
  def apply(values: (String, String)*): TableLabels =
    Empty.withAll(values)

  def fromTableInfo(tableInfo: TableInfo): TableLabels =
    Option(tableInfo.getLabels) match {
      case Some(values) => Empty.withAll(values.asScala)
      case None => Empty
    }
}
