package no.nrk.bigquery

final case class LocationId private[bigquery] (value: String) extends AnyVal

object LocationId {
  // Multi region
  val US = LocationId("us")
  val EU = LocationId("eu")

  // Americas
  val NorthAmericaNorthEast1 = LocationId("northamerica-northeast1")
  val NorthAmericaNorthEast2 = LocationId("northamerica-northeast2")

  val SouthAmericaEast1 = LocationId("southamerica-east1")
  val SouthAmericaWest1 = LocationId("southamerica-west1")

  val USSouth1 = LocationId("us-south1")

  val USCentral1 = LocationId("us-central1")

  val USEast1 = LocationId("us-east1")
  val USEast4 = LocationId("us-east4")
  val USEast5 = LocationId("us-east5")

  val USWest1 = LocationId("us-west1")
  val USWest2 = LocationId("us-west2")
  val USWest3 = LocationId("us-west3")
  val USWest4 = LocationId("us-west4")

  // Europe
  val EuropeNorth1 = LocationId("europe-north1")

  val EuropeCentral2 = LocationId("europe-central2")

  val EuropeWest1 = LocationId("europe-west1")
  val EuropeWest2 = LocationId("europe-west2")
  val EuropeWest3 = LocationId("europe-west3")
  val EuropeWest4 = LocationId("europe-west4")
  val EuropeWest6 = LocationId("europe-west6")
  val EuropeWest8 = LocationId("europe-west8")
  val EuropeWest9 = LocationId("europe-west9")
  val EuropeWest12 = LocationId("europe-west12")

  val EuropeSouthWest1 = LocationId("europe-southwest1")

  // Middle east
  val MiddleEastWest1 = LocationId("me-west1")

  val MiddleEastCentral1 = LocationId("me-central1")

  // Asia and Oceania
  val AsiaEast1 = LocationId("asia-east1")
  val AsiaEast2 = LocationId("asia-east2")

  val AsiaSouth1 = LocationId("asia-south1")
  val AsiaSouth2 = LocationId("asia-south2")

  val AsiaNorthEast1 = LocationId("asia-northeast1")
  val AsiaNorthEast2 = LocationId("asia-northeast2")
  val AsiaNorthEast3 = LocationId("asia-northeast3")

  val AsiaSouthEast1 = LocationId("asia-southeast1")
  val AsiaSouthEast2 = LocationId("asia-southeast2")

  val AustraliaSouthEast1 = LocationId("australia-southeast1")
  val AustraliaSouthEast2 = LocationId("australia-southeast2")
}
