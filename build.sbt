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

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided",
  "org.bdgenomics.adam" % "adam-core_2.10" % "0.17.+" exclude("org.apache.hadoop", "*"),
  "org.bdgenomics.adam" % "adam-apis_2.10" % "0.17.+",
  "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.4.0",
  "org.tmoerman" %% "adam-fx" % "0.5.3",
  //"com.chuusai" %% "shapeless" % "2.2.5",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",

  compilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
)

// suppresses scala version warning nonsense
// ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

fork in run := true

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

bintrayPackageLabels := Seq("scala", "adam", "genomics", "snpeff", "variants", "comparison")

releaseCrossBuild := false