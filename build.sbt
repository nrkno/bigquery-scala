// https://typelevel.org/sbt-typelevel/faq.html#what-is-a-base-version-anyway
ThisBuild / tlBaseVersion := "0.1" // your current series x.y

ThisBuild / organization := "no.nrk"
ThisBuild / organizationName := "NRK"
ThisBuild / startYear := Some(2023)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(
  tlGitHubDev("oyvindberg", "Øyvind Raddum Berg"),
  tlGitHubDev("lysebraate", "Alfred Sandvik Lysebraate"),
  tlGitHubDev("HenningKoller", "Henning Grimeland Koller"),
  tlGitHubDev("hamnis", "Erlend Hamnaberg")
)

// publish to s01.oss.sonatype.org (set to true to publish to oss.sonatype.org instead)
ThisBuild / tlSonatypeUseLegacyHost := false

// publish website from this branch
//ThisBuild / tlSitePublishBranch := Some("main")

val Scala212 = "2.12.17"
val Scala213 = "2.13.10"
ThisBuild / crossScalaVersions := Seq(Scala213, Scala212, "3.2.1")
ThisBuild / scalaVersion := Scala213 // the default Scala

lazy val root = tlCrossRootProject.aggregate(core)

lazy val core = crossProject(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(
    name := "bigquery",
    libraryDependencies ++= Seq(
      "org.typelevel" %%% "cats-core" % "2.9.0",
      "org.typelevel" %%% "cats-effect" % "3.4.4",
      "org.scalameta" %%% "munit" % "0.7.29" % Test,
      "org.typelevel" %%% "munit-cats-effect-3" % "1.0.7" % Test,
      "com.google.cloud" % "google-cloud-bigquery" % "2.20.1",
      "com.google.cloud" % "google-cloud-bigquerystorage" % "2.27.0",
      "com.google.cloud" % "google-cloud-bigquerydatatransfer" % "2.7.0",
      "org.apache.avro" % "avro" % "1.11.1",
      "com.lihaoyi" %% "sourcecode" % "0.3.0",
      "org.typelevel" %% "log4cats-slf4j" % "2.5.0",
      "io.circe" %% "circe-generic" % "0.14.2",
      "io.circe" %% "circe-parser" % "0.14.2",
      "co.fs2" %% "fs2-core" % "3.4.0",
      "co.fs2" %% "fs2-io" % "3.4.0",

      //scala2
      "com.softwaremill.magnolia1_2" %% "magnolia" % "1.1.2",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.9.0"
    )
  )

//lazy val docs = project.in(file("site")).enablePlugins(TypelevelSitePlugin)
