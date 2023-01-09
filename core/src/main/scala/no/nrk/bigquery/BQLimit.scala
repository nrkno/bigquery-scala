package no.nrk.bigquery

sealed trait BQLimit

object BQLimit {
  case class Limit(value: Int) extends BQLimit
  case object NoLimit extends BQLimit
}
