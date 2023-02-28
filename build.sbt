// https://typelevel.org/sbt-typelevel/faq.html#what-is-a-base-version-anyway
ThisBuild / tlBaseVersion := "0.4" // your current series x.y

ThisBuild / organization := "no.nrk.bigquery"
ThisBuild / organizationName := "NRK"
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

val Scala212 = "2.12.17"
val Scala213 = "2.13.10"
ThisBuild / crossScalaVersions := Seq(Scala213, Scala212, "3.2.2")
ThisBuild / scalaVersion := Scala213 // the default Scala
ThisBuild / tlVersionIntroduced := Map(
  "2.12" -> "0.1.1",
  "3" -> "0.1.1",
  "2.13" -> "0.1.0"
)

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
    if (scalaVersion.value.startsWith("3")) {
      Seq("-source:3.2-migration")
    } else {
      Seq()
    }
  }
)

lazy val root = tlCrossRootProject
  .aggregate(core, testing, prometheus)
  .disablePlugins(TypelevelCiSigningPlugin, Sonatype, SbtGpg)

lazy val core = crossProject(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "bigquery-core",
    Compile / headerSources := Nil,
    Test / headerSources := Nil,
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % "2.9.0",
      "org.typelevel" %% "cats-effect" % "3.4.6",
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7" % Test,
      "com.google.cloud" % "google-cloud-bigquery" % "2.23.0",
      "com.google.cloud" % "google-cloud-bigquerystorage" % "2.29.0",
      "com.google.cloud" % "google-cloud-bigquerydatatransfer" % "2.11.0",
      "org.apache.avro" % "avro" % "1.11.1",
      "com.lihaoyi" %% "sourcecode" % "0.3.0",
      "org.typelevel" %% "log4cats-slf4j" % "2.5.0",
      "io.circe" %% "circe-generic" % "0.14.2",
      "io.circe" %% "circe-parser" % "0.14.2",
      "co.fs2" %% "fs2-core" % "3.6.1",
      "co.fs2" %% "fs2-io" % "3.6.1",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.9.0"
    ),
    libraryDependencies ++= {
      if (scalaVersion.value.startsWith("3")) {
        Seq(
          "com.softwaremill.magnolia1_3" %% "magnolia" % "1.1.2"
        )
      } else {
        // scala2
        Seq(
          "com.softwaremill.magnolia1_2" %% "magnolia" % "1.1.2",
          "org.scala-lang" % "scala-reflect" % scalaVersion.value
        )
      }
    }
  )
  .disablePlugins(TypelevelCiSigningPlugin, Sonatype, SbtGpg)

lazy val prometheus = crossProject(JVMPlatform)
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

lazy val testing = crossProject(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("testing"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "bigquery-testing",
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29",
      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7"
    )
  )
  .disablePlugins(TypelevelCiSigningPlugin, Sonatype, SbtGpg)

//lazy val docs = project.in(file("site")).enablePlugins(TypelevelSitePlugin)
