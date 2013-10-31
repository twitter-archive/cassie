import sbt._
import Keys._
import Tests._
import com.twitter.sbt._
import com.typesafe.sbteclipse.plugin.EclipsePlugin._

object Cassie extends Build {
  
   val finagleVersion = "6.7.4";
    
   val sharedSettings: Seq[sbt.Project.Setting[_]] = Seq(
       organization := "com.twitter",
       version := "0.25.0",
       scalaVersion := "2.10.0",
       resolvers ++= Seq(
        "Sonatype repo"                    at "https://oss.sonatype.org/content/groups/scala-tools/",
        "Sonatype releases"                at "https://oss.sonatype.org/content/repositories/releases",
        "Sonatype snapshots"               at "https://oss.sonatype.org/content/repositories/snapshots",
        "Sonatype staging"                 at "http://oss.sonatype.org/content/repositories/staging",
        "Java.net Maven2 Repository"       at "http://download.java.net/maven/2/",
        "Twitter Repository"               at "http://maven.twttr.com"

       ),
       libraryDependencies ++= Seq(
         "ch.qos.logback"          %  "logback-classic"                   % "1.0.13"
       ),
       unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_)),
       scalacOptions ++= Seq(
           "-language:postfixOps",
           "-language:implicitConversions",
           "-deprecation",
           "-feature",
           "-unchecked"
       ),
       EclipseKeys.withSource := true,
       EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource
    )
    
    
   lazy val cassie = Project(
     id = "cassie",
     base = file("."),
     settings = Project.defaultSettings ++ VersionManagement.newSettings   
   ).aggregate(
     cassieCore,
     cassieHadoop,
     cassieServerSets,
     cassieStress
   )

   lazy val cassieCore = Project(
     id = "cassie-core",
     base = file("cassie-core"),
     settings = Project.defaultSettings ++ sharedSettings
   ).settings(
     name := "cassie-core",
     libraryDependencies ++= Seq(
       "com.novocode"               % "junit-interface"                    % "0.7"            % "test",
       "org.scala-tools.testings"   %% "scalacheck"                        % "1.9"            % "test",
       "org.scalatest"              % "scalatest"                          % "2.0M8"          % "test",
       "commons-codec"              % "commons-codec"                      % "1.5",
       "com.twitter"                %% "finagle"                           % finagleVersion,
       "com.twitter"                %% "finagle-core"                      % finagleVersion,
       "com.twitter"                %% "finagle-thrift"                    % finagleVersion
     )
   )
   
   lazy val cassieHadoop = Project(
     id = "cassie-hadoop",
     base = file("cassie-hadoop"),
     settings = Project.defaultSettings ++ sharedSettings
   ).settings(
     name :="cassie-hadoop",
     libraryDependencies ++= Seq(
        "com.novocode"               % "junit-interface"                    % "0.7"            % "test",
        "org.scalatest"              % "scalatest"                          % "2.0M8"          % "test",
        "org.apache.hadoop"          % "hadoop-core"                        % "0.20.2"    
     )
   ).dependsOn(
      cassieCore    
   )
   
   lazy val cassieServerSets = Project(
     id = "cassie-serversets",
     base = file("cassie-serversets"),
     settings = Project.defaultSettings ++ sharedSettings
   ).settings(
     name := "cassie-serversets",
     libraryDependencies ++= Seq(
       "com.twitter"             %% "finagle-serversets"                % finagleVersion
     )
   ).dependsOn(
     cassieCore
   )
   
   lazy val cassieStress = Project(
     id = "cassie-stress",
     base = file("cassie-stress"),
     settings = Project.defaultSettings ++ sharedSettings
   ).settings(
     name := "cassie-stress",
     libraryDependencies ++= Seq(
       "com.twitter"             %% "finagle-ostrich4"                  % finagleVersion,
       "com.twitter"             %% "finagle-stress"                    % finagleVersion
     )
   ).dependsOn(
     cassieCore    
   )
}
