logLevel := Level.Warn

resolvers ++= Seq(
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  Resolver.url("scoverage-bintray", url("https://dl.bintray.com/sksamuel/sbt-plugins/"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.5.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.1")
addSbtPlugin("org.brianmckenna" % "sbt-wartremover" % "0.14")
addSbtPlugin("net.ceedubs" %% "sbt-ctags" % "0.1.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "0.5.0")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
