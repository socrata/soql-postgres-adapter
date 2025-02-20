import Dependencies._

name := "redshift-backend"

libraryDependencies ++= Seq(
  secondarylib,
  socrataHttpCuratorBroker,
  soqlUtils
)

assembly/test := {}

assembly/assemblyJarName := s"${name.value}-assembly.jar"

assembly/assemblyOutputPath := target.value / (assembly/assemblyJarName).value

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)
