ThisBuild / Test / fork := true

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-Xfatal-warnings")

ThisBuild / organization := "com.socrata"

ThisBuild / resolvers += "socrata" at "https://repo.socrata.com/artifactory/libs-release"

ThisBuild / evictionErrorLevel := Level.Warn

val commonPG = project in file("common-pg")

val storePG = (project in file("store-pg")).
  dependsOn(commonPG % "compile;test->test")

val soqlServerPG = (project in file("soql-server-pg")).
  dependsOn(commonPG, storePG % "test->test")

val redshiftBackend = (project in file("redshift-backend")).
  dependsOn(commonPG, storePG % "test->test")

disablePlugins(AssemblyPlugin)

releaseProcess -= ReleaseTransformations.publishArtifacts
