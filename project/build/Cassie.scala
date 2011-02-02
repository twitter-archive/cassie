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

  val scalaJ = "org.scalaj" %% "scalaj-collection" % "1.0"

  val logula = "com.codahale" %% "logula" % "2.0.0" withSources()

  /**
   * Twitter specific deps
   */
  val finagle = "com.twitter" % "finagle" % "1.1.8" 
  val finagleThrift = "com.twitter" % "finagle-thrift" % "1.1.8"
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

  /**
   * Include docs and source as build artifacts.
   */
  override def packageSrcJar = defaultJarPath("-sources.jar")
  val sourceArtifact = sbt.Artifact(artifactID, "src", "jar", Some("sources"), Nil, None)
  override def packageToPublishActions = super.packageToPublishActions ++ Seq(packageSrc, `package`)

  /**
   * Build a JAR file with class files for Cassie, Cassandra, and Thrift.
   */
  def assemblyExclude(base: sbt.PathFinder) = base / "META-INF" ** "*"
  def assemblyOutputPath = jarPath
  def assemblyTemporaryPath = outputPath / "assembly-libs"
  def assemblyClasspath = fullUnmanagedClasspath(sbt.Configurations.Runtime)

  def assemblyPaths(tempDir: sbt.Path, classpath: sbt.PathFinder, exclude: sbt.PathFinder => sbt.PathFinder) = {
    val (libs, directories) = classpath.get.toList.partition(sbt.ClasspathUtilities.isArchive)
    for(jar <- libs) {
      val jarName = jar.asFile.getName
      log.info("Including %s".format(jarName))
      sbt.FileUtilities.unzip(jar, tempDir, log).left.foreach(error)
    }
    val base = (sbt.Path.lazyPathFinder(tempDir :: directories) ##)
    (descendents(base, "*") --- exclude(base)).get
  }

  def assemblyTask(tempDir: sbt.Path, classpath: sbt.PathFinder, exclude: sbt.PathFinder => sbt.PathFinder) = {
    packageTask(sbt.Path.lazyPathFinder(assemblyPaths(tempDir, classpath, exclude)), assemblyOutputPath, packageOptions)
  }
  override def packageAction = assemblyTask(assemblyTemporaryPath, assemblyClasspath, assemblyExclude).dependsOn(compile, copyResources) describedAs("Builds a JAR with Cassandra and Thrift classes included.")
}
