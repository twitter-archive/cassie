class Cassie(info: sbt.ProjectInfo) extends sbt.DefaultProject(info) with rsync.RsyncPublishing {
  /**
   * Repositories
   */
  val scalaToolsSnapshots = "scala-tools.org Snapshots" at "http://scala-tools.org/repo-snapshots"
  val codasRepo = "Coda's Repo" at "http://repo.codahale.com"

  val slf4jVersion = "1.5.11"
  val slf4jApi = "org.slf4j" % "slf4j-api" % slf4jVersion withSources() intransitive()
  val slf4jBindings = "org.slf4j" % "slf4j-jdk14" % slf4jVersion withSources() intransitive()

  val pool = "commons-pool" % "commons-pool" % "1.5.4" withSources() intransitive()
  val codecs = "commons-codec" % "commons-codec" % "1.4" withSources()

  val scalaJ = "org.scalaj" %% "scalaj-collection" % "1.0.RC1-SNAPSHOT"

  val logula = "com.codahale" %% "logula" % "1.0.1" withSources() intransitive()
  
  /**
   * Test Dependencies
   */
  val scalaTest = buildScalaVersion match {
    case "2.8.0.Beta1" => "org.scalatest" % "scalatest" % "1.0.1-for-scala-2.8.0.Beta1-with-test-interfaces-0.3-SNAPSHOT" % "test" withSources() intransitive()
    case "2.8.0.RC2" => "org.scalatest" % "scalatest" % "1.2-for-scala-2.8.0.RC2-SNAPSHOT" % "test" withSources() intransitive()
    case "2.8.0.RC3" => "org.scalatest" % "scalatest" % "1.2-for-scala-2.8.0.RC3-SNAPSHOT" % "test" withSources() intransitive()
    case "2.8.0.RC5" => "org.scalatest" % "scalatest" % "1.2-for-scala-2.8.0.RC5-SNAPSHOT" % "test" withSources() intransitive()
    case unknown => error("no known scalatest impl for %s".format(unknown))
  }
  val mockito = "org.mockito" % "mockito-all" % "1.8.4" % "test" withSources()

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
   * Publish via rsync.
   */
  def rsyncRepo = "codahale.com:/home/codahale/repo.codahale.com"

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
