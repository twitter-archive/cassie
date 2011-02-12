import sbt._
import com.twitter.sbt._

class Cassie(info: sbt.ProjectInfo) extends StandardProject(info)
  with DefaultRepos with CompileFinagleThrift with SubversionPublisher {
  /**
   * Repositories
   */
  val scalaToolsSnapshots = "scala-tools.org Snapshots" at "http://scala-tools.org/repo-snapshots"
  val codasRepo = "Coda's Repo" at "http://repo.codahale.com"

  val slf4jVersion = "1.5.11"
  val slf4jApi = "org.slf4j" % "slf4j-api" % slf4jVersion withSources() intransitive()
  val slf4jBindings = "org.slf4j" % "slf4j-jdk14" % slf4jVersion withSources() intransitive()

  val codecs = "commons-codec" % "commons-codec" % "1.4" withSources()

  val ostrich = "com.twitter" % "ostrich" % "3.0.4"

  /**
   * Twitter specific deps
   */
  val finagle = "com.twitter" % "finagle-core" % "1.1.15"
  val finagleThrift = "com.twitter" % "finagle-thrift" % "1.1.15"
  val slf4jNop = "org.slf4j" %  "slf4j-nop" % "1.5.2" % "provided"

  override def subversionRepository = Some("http://svn.local.twitter.com/maven/")

  /**
   * Test Dependencies
   */
  val scalaTest =  "org.scalatest" % "scalatest" % "1.2" % "test" withSources() intransitive()
  val mockito = "org.mockito" % "mockito-all" % "1.8.4" % "test" withSources()
  val junitInterface = "com.novocode" % "junit-interface" % "0.5" % "test->default"

  /**
   * Build Options
   */
  override def compileOptions = Deprecation :: Unchecked :: super.compileOptions.toList

  // include test-thrift definitions: see https://github.com/twitter/standard-project/issues#issue/13
  override def thriftSources = super.thriftSources +++ (testSourcePath / "thrift" ##) ** "*.thrift"
}
