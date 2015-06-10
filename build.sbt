name := "aerospiker"

version := "0.0.1"

scalaVersion := "2.11.6"

scalacOptions := Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture",
  "-Yinline-warnings",
  "-Xlint"
)

resolvers += Resolver.sonatypeRepo("snapshots")

lazy val scalazVersion = "7.1.2"

lazy val scalaz = Seq(
  "org.scalaz" %% "scalaz-core" % scalazVersion,
  "org.scalaz" %% "scalaz-scalacheck-binding" % scalazVersion % "test"
)

lazy val others = Seq(
  "com.aerospike" % "aerospike-client" % "3.1.2"
)

lazy val deps = (scalaz ++ others) map (_.withSources())

libraryDependencies ++= deps

// --------------------------------------------------
// Each plugins settings

scalariformSettings

import wartremover._

wartremoverSettings
