val coreVersion = "0.4.0-SNAPSHOT"
val serviceVersion = "0.1.0-SNAPSHOT"

lazy val root = project.in(file("."))
  .settings(allSettings)
  .settings(noPublishSettings)
  .aggregate(core, service)
  .dependsOn(core, service)

lazy val allSettings = buildSettings ++ baseSettings ++ publishSettings

lazy val buildSettings = Seq(
  organization := "com.github.tkrs",
  scalaVersion := "2.11.7"
)

val aerospikeVersion = "3.1.4"
val scalazVersion = "7.1.3"
val scalacheckVersion = "1.12.3"
val scalatestVersion = "2.2.5"
val catsVersion = "0.1.2"

lazy val baseSettings = Seq(
  scalacOptions ++= compilerOptions,
  scalacOptions in (Compile, console) := compilerOptions,
  scalacOptions in (Compile, test) := compilerOptions,
  libraryDependencies ++= Seq(
    "org.scalaz" %% "scalaz-core" % scalazVersion,
    "org.scalaz" %% "scalaz-concurrent" % scalazVersion
  ),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

lazy val publishSettings = Seq(
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  homepage := Some(url("https://github.com/tkrs/aerospiker")),
  licenses := Seq("MIT License" -> url("http://www.opensource.org/licenses/mit-license.php")),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/aerospiker"),
      "scm:git:git@github.com:tkrs/aerospiker.git"
    )
  ),
  pomExtra := (
    <developers>
      <developer>
        <id>tkrs</id>
        <name>Takeru Sato</name>
        <url>https://github.com/tkrs</url>
      </developer>
      <developer>
        <id>yanana</id>
        <name>Shun Yanaura</name>
        <url>https://github.com/yanana</url>
      </developer>
    </developers>
  )
)

lazy val noPublishSettings = Seq(
  publish := (),
  publishLocal := (),
  publishArtifact := false
)

lazy val core = project.in(file("core"))
  .settings(
    description := "aerospiker core",
    moduleName := "aerospiker-core",
    name := "core",
    version := coreVersion
  )
  .settings(allSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "com.aerospike" % "aerospike-client" % aerospikeVersion
    )
  )

lazy val service = project.in(file("service"))
  .settings(
    description := "aerospiker service",
    moduleName := "aerospiker-service",
    name := "service",
    version := serviceVersion
  )
  .settings(allSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.spire-math" %% "cats" % catsVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
    )
  )
  .dependsOn(core)

lazy val compilerOptions = Seq(
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

//lazy val tests = Seq(
//  "org.scalaz" %% "scalaz-scalacheck-binding" % scalazVersion,
//  "org.scalatest" %% "scalatest" % scalatestVersion,
//  "org.scalacheck" %% "scalacheck" % scalacheckVersion
//) map (_ % "test")

scalariformSettings
// wartremoverErrors in (Compile, compile) ++= Warts.all
