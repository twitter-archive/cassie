import sbt._

class Plugins(info: sbt.ProjectInfo) extends sbt.PluginDefinition(info) {
   import scala.collection.jcl
   val environment = jcl.Map(System.getenv())
   def isSBTOpenTwitter = environment.get("SBT_OPEN_TWITTER").isDefined
   def isSBTTwitter = environment.get("SBT_TWITTER").isDefined

   override def repositories = if (isSBTOpenTwitter) {
     Set("twitter.artifactory" at "http://artifactory.local.twitter.com/open-source/")
   } else if (isSBTTwitter) {
     Set("twitter.artifactory" at "http://artifactory.local.twitter.com/repo/")
   } else {
     super.repositories ++ Seq("twitter.com" at "http://maven.twttr.com/")
   }
   override def ivyRepositories = Seq(Resolver.defaultLocal(None)) ++ repositories

  val defaultProject = "com.twitter" % "standard-project" % "0.11.3"
  val sbtThrift = "com.twitter" % "sbt-thrift" % "1.1.0"

  val t_repo = "t_repo" at "http://tristanhunt.com:8081/content/groups/public/"
  val posterous = "net.databinder" % "posterous-sbt" % "0.1.4"

  val codasRepo = "codahale.com" at "http://repo.codahale.com/"
}
