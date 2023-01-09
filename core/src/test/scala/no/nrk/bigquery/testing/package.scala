package no.nrk.bigquery

import cats.syntax.show._

package object testing {
  def tempTable(partitionId: BQPartitionId[Any]): Ident =
    Ident(partitionId.show.filter(c => c.isLetterOrDigit || c == '_'))
}
