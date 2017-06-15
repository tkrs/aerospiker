lazy val root = project.in(file("."))
  .settings(allSettings)
  .settings(noPublishSettings)
  .aggregate(core, task, msgpack, tests)
  .dependsOn(core, task, msgpack)

lazy val allSettings = Seq.concat(
  buildSettings,
  baseSettings,
  publishSettings,
  scalariformSettings
)

lazy val buildSettings = Seq(
  name := "aerospiker",
  organization := "com.github.tkrs",
  scalaVersion := "2.12.2",
  crossScalaVersions := Seq("2.11.11", "2.12.2")
)

val aerospikeVersion = "3.3.2"
val circeVersion = "0.7.0"
val monixVersion = "2.2.3"
val scalacheckVersion = "1.13.4"
val scalatestVersion = "3.0.1"
val catsVersion = "0.9.0"

lazy val baseSettings = Seq(
  scalacOptions ++= compilerOptions,
  scalacOptions in (Compile, console) ~= (_ filterNot (_ == "-Ywarn-unused-import")),
  libraryDependencies ++= Seq(
    "com.aerospike" % "aerospike-client" % aerospikeVersion,
    "org.typelevel" %% "cats" % catsVersion,
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion
    // "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0"
  ),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

lazy val publishSettings = Seq(
  releaseCrossBuild := true,
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
      url("https://github.com/tkrs/aerospiker"),
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
    </developers>,
  pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray)
) ++ credentialSettings

lazy val credentialSettings = Seq(
  credentials ++= (for {
    username <- Option(System.getenv().get("SONATYPE_USERNAME"))
    password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq
)


lazy val noPublishSettings = Seq(
  publish := (),
  publishLocal := (),
  publishArtifact := false
)

lazy val core = project.in(file("core"))
  .settings(allSettings: _*)
  .settings(
    description := "aerospiker core",
    moduleName := "aerospiker-core",
    name := "core",
    libraryDependencies ++= testDeps
  )
  .dependsOn(msgpack)

lazy val task = project.in(file("task"))
  .settings(allSettings: _*)
  .settings(
    description := "aerospiker task",
    moduleName := "aerospiker-task",
    name := "task",
    libraryDependencies ++= Seq(
      "io.monix" %% "monix-eval" % monixVersion,
      "io.monix" %% "monix-cats" % monixVersion
    ) ++ testDeps
  )
  .dependsOn(core)

lazy val msgpack = project.in(file("msgpack"))
  .settings(allSettings: _*)
  .settings(
    description := "aerospiker msgpack",
    moduleName := "aerospiker-msgpack",
    name := "msgpack",
    libraryDependencies ++= testDeps
  )

lazy val examples = project.in(file("examples"))
  .settings(allSettings: _*)
  .settings(noPublishSettings)
  .settings(
    description := "aerospiker examples",
    moduleName := "aerospiker-examples",
    name := "examples",
    crossScalaVersions := Seq("2.12.2"),
    fork := true,
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
      "org.slf4j" % "slf4j-simple" % "1.7.24"
    )
  )
  .dependsOn(core, task, msgpack)

lazy val tests = project.in(file("test"))
  .settings(allSettings: _*)
  .settings(noPublishSettings)
  .settings(
    description := "aerospiker test",
    moduleName := "aerospiker-test",
    name := "test",
    fork := true,
    libraryDependencies ++= testDeps
  )
  .dependsOn(core, task, msgpack)

lazy val testDeps = Seq(
  "org.scalacheck" %% "scalacheck" % scalacheckVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion
).map(_ % "test")

lazy val compilerOptions = Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-unchecked",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-unused-import",
  "-Ywarn-numeric-widen",
  "-Xfuture",
  "-Xlint"
)
