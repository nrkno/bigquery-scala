import com.typesafe.tools.mima.core.*

// https://typelevel.org/sbt-typelevel/faq.html#what-is-a-base-version-anyway
ThisBuild / tlBaseVersion := "0.19" // your current series x.y

ThisBuild / organization := "no.nrk.bigquery"
ThisBuild / organizationName := "NRK"
ThisBuild / organizationHomepage := Some(url("https://nrk.no"))
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
      step.withEnv(
        Map(
          "BIGQUERY_SERVICE_ACCOUNT" -> "${{secrets.BIGQUERY_SERVICE_ACCOUNT}}",
          "BIGQUERY_DEFAULT_PROJECT" -> "${{secrets.BIGQUERY_DEFAULT_PROJECT}}",
          "BIGQUERY_DEFAULT_LOCATION" -> "${{secrets.BIGQUERY_DEFAULT_LOCATION}}",
          "ASSERT_CURRENT_GENERATED_FILES" -> "1"
        ))
    case s => s
  }
}

val Scala213 = "2.13.15"
ThisBuild / crossScalaVersions := Seq(Scala213, "3.3.4")
ThisBuild / scalaVersion := Scala213 // the default Scala
ThisBuild / tlVersionIntroduced := Map(
  "3" -> "0.9.0",
  "2.13" -> "0.9.0"
)
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("11"))

val commonSettings = Seq(
  headerLicenseStyle := HeaderLicenseStyle.SpdxSyntax,
  scalacOptions -= "-source:3.0-migration",
  scalacOptions ++= {
    if (scalaVersion.value.startsWith("3")) {
      Seq("-source:3.2-migration", "-old-syntax", "-no-indent")
    } else {
      Nil
    }
  },
  sonatypeProfileName := "no.nrk"
)

lazy val root = tlCrossRootProject
  .settings(name := "bigquery-scala")
  .settings(commonSettings)
  .aggregate(
    core,
    `google-client`,
    `http4s-client`,
    testing,
    prometheus,
    zetasql,
    codegen,
    codegenTests,
    `transfer-client`,
    docs)

lazy val core = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "bigquery-core",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % "2.12.0",
      "org.typelevel" %% "cats-effect" % "3.5.7",
      "org.typelevel" %% "literally" % "1.2.0",
      "org.scalameta" %% "munit" % "1.0.4" % Test,
      "org.scalameta" %% "munit-scalacheck" % "1.0.0" % Test,
      "org.typelevel" %% "munit-cats-effect" % "2.0.0" % Test,
      ("org.apache.avro" % "avro" % "1.12.0").exclude("org.apache.commons", "commons-compress"),
      "org.apache.commons" % "commons-compress" % "1.27.1",
      "com.lihaoyi" %% "sourcecode" % "0.4.2",
      "org.typelevel" %% "log4cats-slf4j" % "2.7.0",
      "io.circe" %% "circe-generic" % "0.14.10",
      "io.circe" %% "circe-parser" % "0.14.10",
      "co.fs2" %% "fs2-core" % "3.11.0",
      "co.fs2" %% "fs2-io" % "3.11.0",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0"
    ),
    libraryDependencies ++= {
      if (scalaVersion.value.startsWith("3")) {
        Seq(
          "com.softwaremill.magnolia1_3" %% "magnolia" % "1.3.8"
        )
      } else {
        // scala2
        Seq(
          "com.softwaremill.magnolia1_2" %% "magnolia" % "1.1.10",
          "org.scala-lang" % "scala-reflect" % scalaVersion.value
        )
      }
    },
    Compile / doc / scalacOptions ++= Seq(
      "-no-link-warnings" // Suppresses problems with Scaladoc @throws links
    ),
    mimaBinaryIssueFilters := List(
      ProblemFilters.exclude[ReversedMissingMethodProblem]("no.nrk.bigquery.QueryClient.extract")
    )
  )

def addGoogleDep(module: ModuleID) =
  module
    .exclude("com.google.guava", "guava")
    .exclude("org.json", "json")

lazy val `google-client` = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("google-client"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "bigquery-google-client",
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit-scalacheck" % "1.0.0" % Test,
      "org.typelevel" %% "munit-cats-effect" % "2.0.0" % Test,
      addGoogleDep("com.google.cloud" % "google-cloud-bigquery" % "2.38.1"),
      addGoogleDep("com.google.cloud" % "google-cloud-bigquerystorage" % "3.11.1"),
      "com.google.guava" % "guava" % "33.4.0-jre"
    ),
    Compile / doc / scalacOptions ++= Seq(
      "-no-link-warnings" // Suppresses problems with Scaladoc @throws links
    ),
    mimaBinaryIssueFilters := Nil
  )

lazy val `http4s-client` = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("http4s-client"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    resolvers ++= Resolver.sonatypeOssRepos("snapshots"),
    name := "bigquery-http4s-client",
    libraryDependencies ++= {
      val binaryVersion = scalaBinaryVersion.value
      Seq(
        "org.scalameta" %% "munit-scalacheck" % "1.0.0" % Test,
        "org.typelevel" %% "munit-cats-effect" % "2.0.0" % Test,
        ("io.chrisdavenport" %% "http4s-grpc-google-cloud-bigquerystorage-v1" % "3.6.0+0.0.6")
          .exclude("io.chrisdavenport", s"http4s-grpc_${binaryVersion}"),
        ("io.chrisdavenport" %% "http4s-grpc" % "0.0.4")
          .exclude("org.http4s", s"http4s-ember-server_${binaryVersion}")
          .exclude("org.http4s", s"http4s-ember-client_${binaryVersion}")
          .exclude("org.http4s", s"http4s-dsl_${binaryVersion}"),
        // needed because of hard-link in http4s-grpc
        // https://github.com/davenverse/http4s-grpc/pull/89
        "org.http4s" %% "http4s-ember-core" % "0.23.30",
        "net.hamnaberg.googleapis" %% "googleapis-http4s-bigquery" % "0.6.1-v2-20241111",
        "com.permutive" %% "gcp-auth" % "1.2.0"
      )
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
      ("com.google.zetasql.toolkit" % "zetasql-toolkit-core" % "0.5.2")
        .exclude("com.google.cloud", "google-cloud-spanner"),
      "org.scalameta" %% "munit" % "1.0.4",
      "org.typelevel" %% "munit-cats-effect" % "2.0.0"
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
      "com.google.cloud" % "google-cloud-bigquerydatatransfer" % "2.75.0",
      "org.scalameta" %% "munit" % "1.0.4",
      "org.typelevel" %% "munit-cats-effect" % "2.0.0"
    ),
    mimaBinaryIssueFilters := Nil
  )

lazy val testing = crossProject(JVMPlatform)
  .withoutSuffixFor(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("testing"))
  .dependsOn(core, `google-client` % Test, `http4s-client` % Test)
  .settings(commonSettings)
  .settings(
    name := "bigquery-testing",
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.4",
      "org.typelevel" %% "munit-cats-effect" % "2.0.0",
      "ch.qos.logback" % "logback-classic" % "1.2.13" % Test,
      "org.http4s" %% "http4s-netty-client" % "0.5.22"
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
      "org.apache.commons" % "commons-text" % "1.13.0"
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
