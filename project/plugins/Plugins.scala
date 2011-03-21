import sbt._

class Plugins(info: sbt.ProjectInfo) extends sbt.PluginDefinition(info) {
  val twitterMaven = "twitter.com" at "http://maven.twttr.com/"
  val defaultProject = "com.twitter" % "standard-project" % "0.11.3"
  val sbtThrift = "com.twitter" % "sbt-thrift" % "1.0.3"

  val t_repo = "t_repo" at "http://tristanhunt.com:8081/content/groups/public/"
  val posterous = "net.databinder" % "posterous-sbt" % "0.1.4"

  val codasRepo = "codahale.com" at "http://repo.codahale.com/"
}
