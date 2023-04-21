# Sbt setup

This library is currently available for Scala binary versions 2.12, 2.13 and 3.2.

To use the latest version, include the following in your `build.sbt`:

```sbt
// The core library
libraryDependencies += "no.nrk.bigquery" %% "bigquery-core" % nrkNoBigQueryVersion

// Testing support
libraryDependencies += "no.nrk.bigquery" %% "bigquery-testing" % nrkNoBigQueryVersion % Test
```
