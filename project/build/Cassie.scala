class Cassie(info: sbt.ProjectInfo) extends sbt.DefaultProject(info) {
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

  val scalaJ = "org.scalaj" %% "scalaj-collection" % "1.0.Beta2"

  val logula = "com.codahale" %% "logula" % "1.0.0" withSources() intransitive()
  
  /**
   * Test Dependencies
   */
  val scalaTest = "org.scalatest" % "scalatest" % "1.0.1-for-scala-2.8.0.Beta1-with-test-interfaces-0.3-SNAPSHOT" % "test" withSources() intransitive()
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
   * Publish to a local dir, and then rsync with my repo.
   */
  override def managedStyle = sbt.ManagedStyle.Maven
  val publishTo = sbt.Resolver.file("Local Cache", ("." / "target" / "repo").asFile)
  def publishToLocalRepoAction = super.publishAction
  override def publishAction = task {
    log.info("Uploading to repo.codahale.com")
    sbt.Process("rsync", "-avz" :: "target/repo/" :: "codahale.com:/home/codahale/repo.codahale.com" :: Nil) ! log
    None
  } describedAs("Publish binary and source JARs to repo.codahale.com") dependsOn(test, publishToLocalRepoAction)

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
