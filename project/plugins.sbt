resolvers ++= Seq(
  Classpaths.typesafeResolver,
  "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "jgit-repo"          at "http://download.eclipse.org/jgit/maven"
)

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin"    % "2.1.0")

addSbtPlugin("com.typesafe.sbt"        % "sbt-git"              % "0.5.0")

addSbtPlugin("com.twitter"             % "sbt-package-dist"     % "1.1.0")
