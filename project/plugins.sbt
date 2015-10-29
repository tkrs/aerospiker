logLevel := Level.Warn

resolvers ++= Seq(
  Classpaths.typesafeReleases,
  Classpaths.sbtPluginReleases,
  Resolver.url("scoverage-bintray", url("https://dl.bintray.com/sksamuel/sbt-plugins/"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.5.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.3")
addSbtPlugin("org.brianmckenna" % "sbt-wartremover" % "0.14")
addSbtPlugin("net.ceedubs" %% "sbt-ctags" % "0.1.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "0.5.0")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")
