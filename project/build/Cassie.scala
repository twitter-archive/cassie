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
}
