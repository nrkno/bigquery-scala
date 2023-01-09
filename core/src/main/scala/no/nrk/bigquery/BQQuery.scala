package no.nrk.bigquery

case class BQQuery[T: BQRead](sql: BQSqlFrag) {
  def bqRead: BQRead[T] = implicitly
}
