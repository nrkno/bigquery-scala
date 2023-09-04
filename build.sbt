import com.typesafe.tools.mima.core._

// https://typelevel.org/sbt-typelevel/faq.html#what-is-a-base-version-anyway
ThisBuild / tlBaseVersion := "0.10" // your current series x.y

ThisBuild / organization := "no.nrk.bigquery"
ThisBuild / organizationName := "NRK"
ThisBuild / organizationHomepage := Some(new URL("https://nrk.no"))
ThisBuild / startYear := Some(2020)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(
  tlGitHubDev("oyvindberg", "Øyvind Raddum Berg"),
  tlGitHubDev("lysebraate", "Alfred Sandvik Lysebraate"),
  tlGitHubDev("HenningKoller", "Henning Grimeland Koller"),
  tlGitHubDev("hamnis", "Erlend Hamnaberg")
)
ThisBuild / tlCiHeaderCheck := false
ThisBuild / tlCiScalafmtCheck := true

// publish website from this branch
//ThisBuild / tlSitePublishBranch := Some("main")
ThisBuild / githubWorkflowTargetTags ++= Seq("v*")
ThisBuild / githubWorkflowPublishTargetBranches :=
  Seq(RefPredicate.StartsWith(Ref.Tag("v")))

ThisBuild / githubWorkflowPublish := Seq(
  WorkflowStep.Sbt(
    commands = List("+publish"),
    name = Some("Publish project"),
    env = Map(
      "MYGET_USERNAME" -> "${{ secrets.PLATTFORM_MYGET_ENTERPRISE_WRITE_ID }}",
      "MYGET_PASSWORD" -> "${{ secrets.PLATTFORM_MYGET_ENTERPRISE_WRITE_SECRET }}"
    )
  )
)
ThisBuild / githubWorkflowBuild := {
  val list = (ThisBuild / githubWorkflowBuild).value
  list.collect {
    case step: WorkflowStep.Sbt if step.name.contains("Test") =>
      step.copy(env = Map(
        "BIGQUERY_SERVICE_ACCOUNT" -> "${{secrets.BIGQUERY_SERVICE_ACCOUNT}}",
        "ASSERT_CURRENT_GENERATED_FILES" -> "1"
      ))
    case step: WorkflowStep.Sbt if step.name.contains("Check binary compatibility") =>
      step.copy(env = Map(
        "MYGET_USERNAME" -> "${{ secrets.PLATTFORM_MYGET_ENTERPRISE_READ_ID }}",
        "MYGET_PASSWORD" -> "${{ secrets.PLATTFORM_MYGET_ENTERPRISE_READ_SECRET }}"
      ))
    case s => s
  }
}

val Scala212 = "2.12.18"
val Scala213 = "2.13.11"
ThisBuild / crossScalaVersions := Seq(Scala213, Scala212, "3.3.0")
ThisBuild / scalaVersion := Scala213 // the default Scala
ThisBuild / tlVersionIntroduced := Map(
  "2.12" -> "0.1.1",
  "3" -> "0.1.1",
  "2.13" -> "0.1.0"
)
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("11"))

val commonSettings = Seq(
  resolvers += "MyGet - datahub".at(s"https://nrk.myget.org/F/datahub/maven/"),
  Compile / headerSources := Nil,
  Test / headerSources := Nil,
  publishTo := {
    val MyGet = "https://nrk.myget.org/F/datahub/maven/"
    if (isSnapshot.value) None else Some("releases".at(MyGet))
  },
  credentials ++= {
    (sys.env.get("MYGET_USERNAME"), sys.env.get("MYGET_PASSWORD")) match {
      case (Some(username), Some(password)) =>
        List(
          Credentials("MyGet - datahub", "nrk.myget.org", username, password)
        )
      case _ => Nil
    }
  },
  scalacOptions -= "-source:3.0-migration",
  scalacOptions ++= {
    val compilerWarnings =
      List(
        "cat=other-match-analysis:e", // error on exhaustive match
        "cat=other:e" // compare values like 1 == "str"
      ).mkString("-Wconf:", ",", ",any:wv")
    if (scalaVersion.value.startsWith("3")) {
      Seq("-source:3.2-migration", compilerWarnings)
    } else {
      Seq("-feature", "-language:implicitConversions", compilerWarnings)
    }
  }
)

lazy val root = tlCrossRootProject
  .settings(name := "bigquery-scala")
  .aggregate(core, testing, prometheus, zetasql, docs)
  .disablePlugins(TypelevelCiSigningPlugin, Sonatype, SbtGpg)

lazy val core = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "bigquery-core",
    Compile / headerSources := Nil,
    Test / headerSources := Nil,
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % "2.10.0",
      "org.typelevel" %% "cats-effect" % "3.5.1",
      "org.typelevel" %% "literally" % "1.1.0",
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "org.scalameta" %% "munit-scalacheck" % "0.7.29" % Test,
      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7" % Test,
      "com.google.cloud" % "google-cloud-bigquery" % "2.31.1",
      "com.google.cloud" % "google-cloud-bigquerystorage" % "2.41.1",
      "com.google.cloud" % "google-cloud-bigquerydatatransfer" % "2.23.0",
      "org.apache.avro" % "avro" % "1.11.2",
      "com.lihaoyi" %% "sourcecode" % "0.3.0",
      "org.typelevel" %% "log4cats-slf4j" % "2.6.0",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",
      "co.fs2" %% "fs2-core" % "3.8.0",
      "co.fs2" %% "fs2-io" % "3.8.0",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0"
    ),
    libraryDependencies ++= {
      if (scalaVersion.value.startsWith("3")) {
        Seq(
          "com.softwaremill.magnolia1_3" %% "magnolia" % "1.3.3"
        )
      } else {
        // scala2
        Seq(
          "com.softwaremill.magnolia1_2" %% "magnolia" % "1.1.4",
          "org.scala-lang" % "scala-reflect" % scalaVersion.value
        )
      }
    },
    mimaBinaryIssueFilters ++= List(
      ProblemFilters.exclude[DirectMissingMethodProblem]("no.nrk.bigquery.BQDataset.of"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("no.nrk.bigquery.BQDataset.unsafeOf")
    )
  )
  .disablePlugins(TypelevelCiSigningPlugin, Sonatype, SbtGpg)

lazy val prometheus = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("prometheus"))
  .settings(commonSettings)
  .dependsOn(core)
  .settings(
    name := "bigquery-prometheus",
    libraryDependencies ++= Seq(
      "io.prometheus" % "simpleclient" % "0.16.0"
    )
  )
  .disablePlugins(TypelevelCiSigningPlugin, Sonatype, SbtGpg)

lazy val zetasql = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("zetasql"))
  .settings(commonSettings)
  .dependsOn(core)
  .settings(
    name := "bigquery-zetasql",
    tlMimaPreviousVersions := Set.empty,
    libraryDependencies ++= Seq(
      "com.google.zetasql.toolkit" % "zetasql-toolkit-bigquery" % "0.4.0",
      "org.scalameta" %% "munit" % "0.7.29",
      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7"
    )
  )
  .disablePlugins(TypelevelCiSigningPlugin, Sonatype, SbtGpg)

lazy val testing = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("testing"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "bigquery-testing",
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29",
      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7"
    ),
    mimaBinaryIssueFilters ++= Nil
  )
  .disablePlugins(TypelevelCiSigningPlugin, Sonatype, SbtGpg)

lazy val docs = project
  .in(file("site"))
  //  .enablePlugins(TypelevelSitePlugin)
  .enablePlugins(MdocPlugin, NoPublishPlugin)
  .disablePlugins(TypelevelCiSigningPlugin, Sonatype, SbtGpg)
  .dependsOn(core.jvm, testing.jvm)
  .settings(
    compile := {
      val result = (Compile / compile).value
      mdoc.toTask("").value
      result
    }
  )
