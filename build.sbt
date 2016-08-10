import sbt.Keys._

organization := "org.tmoerman"

name := "vcf-comp"

homepage := Some(url(s"https://github.com/tmoerman/"+name.value))

scalaVersion := "2.10.4"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),

  "bintray-tmoerman" at "http://dl.bintray.com/tmoerman/maven"
)

val adamVersion = "0.19.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided",
  "org.bdgenomics.adam" % "adam-core_2.10" % adamVersion exclude("org.apache.hadoop", "*"),
  "org.bdgenomics.adam" % "adam-apis_2.10" % adamVersion,
  "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.7.0",
  "org.tmoerman" %% "adam-fx" % "0.6.0",
  "org.scalaz" %% "scalaz-core" % "7.0.6",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

fork in run := true

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

bintrayPackageLabels := Seq("scala", "adam", "genomics", "snpeff", "vcf", "variants", "comparison")

pomExtra :=
  <scm>
    <url>git@github.com:tmoerman/{name.value}.git</url>
    <connection>scm:git:git@github.com:tmoerman/{name.value}.git</connection>
  </scm>
  <developers>
    <developer>
      <id>tmoerman</id>
      <name>tmoerman</name>
      <url>https://github.com/tmoerman</url>
    </developer>
  </developers>

releaseCrossBuild := false
