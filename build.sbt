import com.typesafe.tools.mima.core._

// https://typelevel.org/sbt-typelevel/faq.html#what-is-a-base-version-anyway
ThisBuild / tlBaseVersion := "0.12" // your current series x.y

ThisBuild / organization := "no.nrk.bigquery"
ThisBuild / organizationName := "NRK"
ThisBuild / organizationHomepage := Some(new URL("https://nrk.no"))
ThisBuild / startYear := Some(2020)
ThisBuild / licenses := Seq(License.MIT)
ThisBuild / developers := List(
  tlGitHubDev("oyvindberg", "Øyvind Raddum Berg"),
  tlGitHubDev("lysebraate", "Alfred Sandvik Lysebraate"),
  tlGitHubDev("HenningKoller", "Henning Grimeland Koller"),
  tlGitHubDev("ingarabr", "Ingar Abrahamsen"),
  tlGitHubDev("hamnis", "Erlend Hamnaberg")
)
ThisBuild / tlCiHeaderCheck := true
ThisBuild / tlCiScalafmtCheck := true
ThisBuild / tlSonatypeUseLegacyHost := true
ThisBuild / Test / fork := true

// publish website from this branch
//ThisBuild / tlSitePublishBranch := Some("main")

ThisBuild / githubWorkflowPublishTargetBranches :=
  Seq(RefPredicate.StartsWith(Ref.Tag("v")))

ThisBuild / githubWorkflowBuild := {
  val list = (ThisBuild / githubWorkflowBuild).value
  list.collect {
    case step: WorkflowStep.Sbt if step.name.contains("Test") =>
      step.copy(env = Map(
        "BIGQUERY_SERVICE_ACCOUNT" -> "${{secrets.BIGQUERY_SERVICE_ACCOUNT}}",
        "ASSERT_CURRENT_GENERATED_FILES" -> "1"
      ))
    case s => s
  }
}

val Scala212 = "2.12.18"
val Scala213 = "2.13.12"
ThisBuild / crossScalaVersions := Seq(Scala213, Scala212, "3.3.1")
ThisBuild / scalaVersion := Scala213 // the default Scala
ThisBuild / tlVersionIntroduced := Map(
  "2.12" -> "0.9.0",
  "3" -> "0.9.0",
  "2.13" -> "0.9.0"
)
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("11"))

val commonSettings = Seq(
  headerLicenseStyle := HeaderLicenseStyle.SpdxSyntax,
  scalacOptions -= "-source:3.0-migration",
  scalacOptions ++= {
    if (scalaVersion.value.startsWith("3")) {
      Seq("-source:3.2-migration")
    } else {
      Seq("-feature", "-language:implicitConversions", "-language:experimental.macros")
    }
  },
  sonatypeProfileName := "no.nrk"
)

lazy val root = tlCrossRootProject
  .settings(name := "bigquery-scala")
  .settings(commonSettings)
  .aggregate(core, testing, prometheus, zetasql, codegen, codegenTests, `transfer-client`, docs)

lazy val core = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "bigquery-core",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % "2.10.0",
      "org.typelevel" %% "cats-effect" % "3.5.2",
      "org.typelevel" %% "literally" % "1.1.0",
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "org.scalameta" %% "munit-scalacheck" % "0.7.29" % Test,
      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7" % Test,
      "com.google.cloud" % "google-cloud-bigquery" % "2.34.2",
      "com.google.cloud" % "google-cloud-bigquerystorage" % "2.46.0",
      "org.apache.avro" % "avro" % "1.11.3",
      "com.lihaoyi" %% "sourcecode" % "0.3.1",
      "org.typelevel" %% "log4cats-slf4j" % "2.6.0",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",
      "co.fs2" %% "fs2-core" % "3.9.3",
      "co.fs2" %% "fs2-io" % "3.9.3",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0"
    ),
    libraryDependencies ++= {
      if (scalaVersion.value.startsWith("3")) {
        Seq(
          "com.softwaremill.magnolia1_3" %% "magnolia" % "1.3.4"
        )
      } else {
        // scala2
        Seq(
          "com.softwaremill.magnolia1_2" %% "magnolia" % "1.1.6",
          "org.scala-lang" % "scala-reflect" % scalaVersion.value
        )
      }
    },
    Compile / doc / scalacOptions ++= Seq(
      "-no-link-warnings" // Suppresses problems with Scaladoc @throws links
    ),
    mimaBinaryIssueFilters := Nil
  )

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

lazy val zetasql = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("zetasql"))
  .settings(commonSettings)
  .dependsOn(core)
  .settings(
    name := "bigquery-zetasql",
    libraryDependencies ++= Seq(
      "com.google.zetasql.toolkit" % "zetasql-toolkit-bigquery" % "0.4.1",
      "org.scalameta" %% "munit" % "0.7.29",
      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7"
    ),
    mimaBinaryIssueFilters := Nil
  )

lazy val `transfer-client` = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("transfer-client"))
  .settings(commonSettings)
  .dependsOn(core)
  .settings(
    name := "bigquery-transfer-client",
    libraryDependencies ++= Seq(
      "com.google.cloud" % "google-cloud-bigquerydatatransfer" % "2.30.0",
      "org.scalameta" %% "munit" % "0.7.29",
      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7"
    ),
    mimaBinaryIssueFilters := Nil,
    tlMimaPreviousVersions := Set.empty
  )

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
    mimaBinaryIssueFilters := Nil
  )

lazy val codegen = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("codegen"))
  .dependsOn(core, testing % Test)
  .settings(commonSettings)
  .settings(
    name := "bigquery-codegen",
    libraryDependencies ++= Seq(
      "org.apache.commons" % "commons-text" % "1.11.0"
    ),
    mimaBinaryIssueFilters := Nil
  )

lazy val codegenTests = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("codegen-tests"))
  .dependsOn(core)
  .enablePlugins(NoPublishPlugin)
  .disablePlugins(HeaderPlugin)
  .settings(
    tlFatalWarnings := false,
    tlMimaPreviousVersions := Set.empty,
    mimaBinaryIssueFilters := Nil
  )

lazy val docs = project
  .in(file("site"))
  .settings(commonSettings)
  //  .enablePlugins(TypelevelSitePlugin)
  .enablePlugins(MdocPlugin, NoPublishPlugin)
  .dependsOn(core.jvm, testing.jvm)
  .settings(
    compile := {
      val result = (Compile / compile).value
      mdoc.toTask("").value
      result
    }
  )
