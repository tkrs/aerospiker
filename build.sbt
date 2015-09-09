name := "aerospiker"

organization := "com.github.tkrs"

version := "0.4.0-SNAPSHOT"

scalaVersion := "2.11.7"
publishMavenStyle := true

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

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

lazy val scalazVersion = "7.1.3"
lazy val scalacheckVersion = "1.12.3"
lazy val scalatestVersion = "2.2.5"
lazy val catsVersion = "0.1.2"

lazy val scalaz = Seq(
  "org.scalaz" %% "scalaz-core" % scalazVersion,
  "org.scalaz" %% "scalaz-concurrent" % scalazVersion,
  "org.scalaz" %% "scalaz-scalacheck-binding" % scalazVersion % "test"
)

lazy val cats = Seq(
  "org.spire-math" %% "cats" % catsVersion
)

lazy val test = Seq(
  "org.scalatest" %% "scalatest" % scalatestVersion,
  "org.scalacheck" %% "scalacheck" % scalacheckVersion
) map (_ % "test")

lazy val others = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.aerospike" % "aerospike-client" % "3.1.4"
)

lazy val deps = (scalaz ++ others ++ test ++ cats) map (_.withSources())

libraryDependencies ++= deps

scalariformSettings

// wartremoverErrors in (Compile, compile) ++= Warts.all
