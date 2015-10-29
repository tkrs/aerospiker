lazy val root = project.in(file("."))
  .settings(allSettings)
  .settings(noPublishSettings)
  .aggregate(core, task, msgpack)
  .dependsOn(core, task, msgpack)

lazy val allSettings = buildSettings ++ baseSettings ++ testSettings ++ publishSettings

lazy val buildSettings = Seq(
  organization := "com.github.tkrs",
  scalaVersion := "2.11.7"
)

val aerospikeVersion = "3.1.6"
val circeVersion = "0.1.1"
val scalazVersion = "7.1.3"
// val scalacheckVersion = "1.12.3"
val scalatestVersion = "2.2.5"
val catsVersion = "0.1.2"

lazy val baseSettings = Seq(
  scalacOptions ++= compilerOptions,
  scalacOptions in (Compile, console) := compilerOptions,
  scalacOptions in (Compile, test) := compilerOptions,
  libraryDependencies ++= Seq(
    "com.aerospike" % "aerospike-client" % aerospikeVersion,
    "org.scalaz" %% "scalaz-concurrent" % scalazVersion,
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-jawn" % circeVersion
  ),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

lazy val testSettings = Seq(
  scalacOptions ++= compilerOptions,
  scalacOptions in (Compile, console) := compilerOptions,
  scalacOptions in (Compile, test) := compilerOptions,
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion
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
  pomExtra :=
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

lazy val noPublishSettings = Seq(
  publish := (),
  publishLocal := (),
  publishArtifact := false
)

lazy val core = project.in(file("core"))
  .settings(
    description := "aerospiker core",
    moduleName := "aerospiker-core",
    name := "core"
  )
  .settings(allSettings: _*)
  .dependsOn(msgpack)

lazy val task = project.in(file("task"))
  .settings(
    description := "aerospiker task",
    moduleName := "aerospiker-task",
    name := "task"
  )
  .settings(allSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.spire-math" %% "cats" % catsVersion
    )
  )
  .dependsOn(core)

lazy val msgpack = project.in(file("msgpack"))
  .settings(
    description := "aerospiker msgpack",
    moduleName := "aerospiker-msgpack",
    name := "msgpack"
  )
  .settings(allSettings: _*)

lazy val example = project.in(file("example"))
  .settings(
    description := "aerospiker example",
    moduleName := "aerospiker-example",
    name := "example"
  )
  .settings(allSettings: _*)
  .settings(noPublishSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
      "org.slf4j" % "slf4j-simple" % "1.7.12"
    )
  )
  .dependsOn(core, task, msgpack)

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
wartremoverErrors in (Compile, compile) ++= Warts.all
