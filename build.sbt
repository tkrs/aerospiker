name := "aerospiker"

organization := "com.github.tkrs"

version := "0.1.1-SNAPSHOT"

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

lazy val scalaz = Seq(
  "org.scalaz" %% "scalaz-core" % scalazVersion,
  "org.scalaz" %% "scalaz-concurrent" % scalazVersion,
  "org.scalaz" %% "scalaz-scalacheck-binding" % scalazVersion % "test"
)

lazy val test = Seq(
  "org.scalatest" %% "scalatest" % scalatestVersion,
  "org.scalacheck" %% "scalacheck" % scalacheckVersion
) map (_ % "it,test")

lazy val others = Seq(
  "com.aerospike" % "aerospike-client" % "3.1.3"
)

lazy val deps = (scalaz ++ others ++ test) map (_.withSources())

libraryDependencies ++= deps

lazy val commonSettings = Seq(
  scalaVersion := "2.11.7",
  organization := "com.github.tkrs"
)
lazy val specs2core = "org.specs2" %% "specs2-core" % "2.4.14"

lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(
    libraryDependencies += specs2core % "it,test"
    // other settings here
  )


// --------------------------------------------------
// Each plugins settings

scalariformSettings

// wartremoverErrors in (Compile, compile) ++= Warts.all
