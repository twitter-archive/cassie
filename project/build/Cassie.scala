import sbt._
import com.twitter.sbt._

class Cassie(info: sbt.ProjectInfo) extends StandardParentProject(info)
  with DefaultRepos with SubversionPublisher {
  /**
   * Repositories
   */
  val scalaToolsSnapshots = "scala-tools.org Snapshots" at "http://scala-tools.org/repo-snapshots"
  val codasRepo = "Coda's Repo" at "http://repo.codahale.com"

  override def subversionRepository = Some("http://svn.local.twitter.com/maven/")

  val coreProject = project(
    "cassie-core", "cassie-core",
    new CoreProject(_))

  val hadoopProject = project(
    "cassie-hadoop", "cassie-hadoop",
    new HadoopProject(_), coreProject)

  class CoreProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher with AdhocInlines with CompileThriftFinagle {

    val slf4jVersion = "1.5.11"
    val slf4jApi = "org.slf4j" % "slf4j-api" % slf4jVersion withSources() intransitive()
    val slf4jBindings = "org.slf4j" % "slf4j-jdk14" % slf4jVersion withSources() intransitive()

    val codecs = "commons-codec" % "commons-codec" % "1.4" //withSources()

    val jackson     = "org.codehaus.jackson" % "jackson-core-asl" % "1.6.1"
    val jacksonMap  = "org.codehaus.jackson" % "jackson-mapper-asl" % "1.6.1"

    /**
     * Twitter specific deps
     */
    val finagleVersion = "1.2.1"
    val finagle = "com.twitter" % "finagle-core" % finagleVersion
    val finagleThrift = "com.twitter" % "finagle-thrift" % finagleVersion
    val finagleOstrich = "com.twitter" % "finagle-ostrich4" % finagleVersion
    val util = "com.twitter" % "util" % "1.8.3"

    val slf4jNop = "org.slf4j" %  "slf4j-nop" % slf4jVersion % "provided"

    /**
     * Test Dependencies
     */
    val scalaTest =  "org.scalatest" % "scalatest" % "1.2" % "test" withSources() intransitive()
    val mockito = "org.mockito" % "mockito-all" % "1.8.4" % "test" withSources()
    val junitInterface = "com.novocode" % "junit-interface" % "0.5" % "test->default"

    override def compileOptions = Deprecation :: Unchecked :: super.compileOptions.toList

    // include test-thrift definitions: see https://github.com/twitter/standard-project/issues#issue/13
    override def thriftSources = super.thriftSources +++ (testSourcePath / "thrift" ##) ** "*.thrift"
  }

  class HadoopProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher with AdhocInlines {
    val hadoop    = "org.apache.hadoop" % "hadoop-core" % "0.20.2"
  }
}