package no.nrk.bigquery

import org.scalacheck._

class BQDatasetTest extends munit.ScalaCheckSuite {
  val project = ProjectId.unsafeFromString("test-123")
  def shorterThan(n: Int) = Gen.alphaNumStr.map(str => if (str.length < n) str else str.substring(0, n))
  def ofSize(n: Int) = Gen.listOfN(n, Gen.alphaNumStr).map(_.mkString)

  property("valid dataset") {

    val gen = for {
      alpha <- shorterThan(1021).filterNot(_.isEmpty)
      underscore <- Gen.oneOf("", "_")
      alpha2 <- shorterThan(2).filterNot(_.isEmpty)
    } yield alpha + underscore + alpha2

    Prop.forAll(gen) { (ident: String) =>
      BQDataset.fromId(project, ident).isRight
    }
  }

  property("invalid dataset") {
    val gen = for {
      alpha <- shorterThan(1024).filterNot(_.isEmpty)
      sep <- Gen.oneOf("", "-", "$", "@", ".")
      alpha2 <- if (alpha.length < 1024 && sep.nonEmpty) shorterThan(25) else ofSize(1024 - alpha.length)
    } yield alpha + sep + alpha2

    Prop.forAll(gen) { (ident: String) =>
      BQDataset.fromId(project, ident).isLeft
    }

  }
}
