organization := "org.tmoerman"

name := "vcf-comp"

homepage := Some(url(s"https://github.com/tmoerman/"+name.value))

scalaVersion := "2.10.4"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "bintray-tmoerman" at "http://dl.bintray.com/tmoerman/maven"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided"

libraryDependencies += "org.bdgenomics.adam" % "adam-core_2.10" % "0.17.+" exclude("org.apache.hadoop", "*")

libraryDependencies += "org.bdgenomics.adam" % "adam-apis_2.10" % "0.17.+"

libraryDependencies += "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.4.0"

libraryDependencies += "org.tmoerman" %% "adam-fx" % "0.5.3"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

fork in run := true

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

bintrayPackageLabels := Seq("scala", "adam", "genomics", "snpeff", "variants")

releaseCrossBuild := false