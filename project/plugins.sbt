resolvers ++= Seq(
  Classpaths.typesafeResolver,
  "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "jgit-repo"          at "http://download.eclipse.org/jgit/maven",
  "Twitter Repo"       at "http://maven.twttr.com/"
)

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin"    % "2.1.0")

addSbtPlugin("com.typesafe.sbt"        % "sbt-git"              % "0.5.0")

addSbtPlugin("com.twitter"             % "sbt-package-dist"     % "1.1.0")

addSbtPlugin("com.twitter" %% "scrooge-sbt-plugin" % "3.7.0")
