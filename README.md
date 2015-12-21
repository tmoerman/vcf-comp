# VCF-comp

![venn](img/venn.png)

VCF-comp is a [Scala](http://www.scala-lang.org/) library for pairwise comparison of annotated [VCF](http://samtools.github.io/hts-specs/VCFv4.2.pdf) files. Uses [Apache Spark](http://spark.apache.org/), [ADAM](https://github.com/bigdatagenomics/adam) and [adam-fx](https://github.com/tmoerman/adam-fx).

VCF-comp is intended for performing VCF analyses within a [Spark-notebook](https://github.com/andypetrella/spark-notebook) environment.

VCF-comp is open source software, available on both [Github](https://github.com/tmoerman/vcf-comp) and [BitBucket](https://bitbucket.org/vda-lab/vcf-comp). 

VCF-comp artifacts are published to [Bintray](https://bintray.com/tmoerman/maven/vcf-comp). Latest version: `0.3`

## TOC

<!-- http://doctoc.herokuapp.com/ -->

## GETTING STARTED
### Using the Docker image

`TODO`

*(A Docker image containing Spark Notebook and example notebooks with VCF-comp analyses will be available in the near future)*

### Manual setup

We will assume you have a [Spark-notebook](https://github.com/andypetrella/spark-notebook) instance available. If not, have a look at the [launch instructions](https://github.com/andypetrella/spark-notebook#using-a-release).

#### Remote artifact repository

First, we specify the remote Maven [repository](https://bintray.com/tmoerman/maven/vcf-comp) on Bintray from which the VCF-comp library artifact are available.

```
:remote-repo bintray-tmoerman % default % http://dl.bintray.com/tmoerman/maven % maven
```

#### Library dependencies

Next, we specify the library dependencies. We need both the VCF-comp library and the BDGenomics Adam library, but without Hadoop and Spark dependencies, because these are already provided automatically in the Spark Notebook environment. 

```
:dp org.tmoerman % vcf-comp_2.10 % 0.3
:dp org.bdgenomics.adam % adam-core % 0.17.1
- org.apache.hadoop % hadoop-client %   _
- org.apache.spark  %     _         %   _
- org.scala-lang    %     _         %   _
- org.scoverage     %     _         %   _
- joda-time         %     _         %   _
```

#### Initialize the SparkContext

We are now ready to configure the running `SparkContext` in the notebook. VCF-comp uses Adam's rich domain classes, therefore we need to define Kryo as the Spark serializer and the Adam-FX Kryo registrator. Don't worry too much about this, just execute the following snippet in a Spark Notebook cell.

```
reset(lastChanges = _.set("spark.app.name", "My VCF analysis")
                     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                     .set("spark.kryo.registrator", "org.tmoerman.adam.fx.serialization.AdamFxKryoRegistrator")
                     .set("spark.kryoserializer.buffer", "32m")
                     .set("spark.kryoserializer.buffer.max value", "128")
                     .set("spark.kryo.referenceTracking", "true"))
```

#### Import VCF-comp functionality

Almost there! The final setup step is to import the necessary VCF-comp classes and functions. 

```
import org.tmoerman.vcf.comp.VcfComparisonContext._
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.viz.SparkNoteBookDimpleGraphs._

import org.tmoerman.adam.fx.avro.AnnotatedGenotype

implicit def toDimpleChart(chart: DimpleChart) = chart match {
  case DimpleChart(data, js, s) => DiyChart(data, js, maxPoints = chart.maxPoints, sizes = s) 
}
```

#### Test the setup

We can now test the setup of the library by executing the `.help` function on the SparkContext variable (`sc` or `sparkContext`).

```
sc.help
```

This method should return a list of methods we can invoke on the SparkContext:

```
res9: String = 
- getMetaFields
- startQcComparison
- startSnpComparison
```

This is VCF-comp's so-called *discoverable API* in action. We discuss this concept in more detail later.

Well done! We are now ready to perform an actual pairwise VCF comparison analysis! Read on to find out how.

## USAGE

VCF-comp focuses on pairwise comparison of VCF files, so let's get two interesting files ready: `tumor.vcf` and `normal.vcf`. In this example, we read these files from [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html):

```Scala
val workDir = "hdfs://bla:54310/my-data/VCF/" 

val tumor  = workDir + "tumor.vcf"
val normal = workDir + "normal.vcf"
```

We can now start two types of VCF comparison: a quality control (QC) comparison and a SNP comparison. Let's start with a QC comparison.

### Start a QC comparison

In order to start a QC comparison, we invoke the appropriate method on the SparkContext instance `sc`. Remember, as previously mentioned, we can always invoke the `.help` method on different objects in the analysis, to give us a list of methods available at that point. Let's do that one more time:

```Scala
sc.help
```
```
res9: String = 
- getMetaFields
- startQcComparison
- startSnpComparison
```

The method `startQcComparison` is the one we need, let's invoke it while setting some parameters:

```
val qcParams = new ComparisonParams(labels = ("TUMOR", "NORMAL")) // parameters

val qcComparison = sc.startQcComparison(tumor, normal, qcParams) // a Spark RDD
                     .cache()                                    // cache the RDD
```

The `ComparisonParams` object defines labels for our VCF files. The result of invoking `startQcComparison` is a Spark [RDD](http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds), which we cache in memory because it is an intermediate result we will use in future computations. We store this RDD in a value named `qcComparison`.

Let's once again use the `.help` method to discover how we can proceed. This time we don't invoke it on the SparkContext `sc`, but on the `qcComparison` value.

```Scala
qcComparison.help
```
```
res11: String = 
- countByProjection
- countByTraversableProjection
- indelLengthDistribution
- snpCountByContig
- snpQualityDistribution
- snpReadDepthDistribution
- variantTypeCount
```

Looks like we can do some interesting analyses! Let's have a look at the number of SNPs we can find in the different contigs, so we choose `snpCountByContig`.

```Scala
qcComparison.snpCountByContig
```

Oops, now we get some gobbledigook:

```
res16: Iterable[org.tmoerman.vcf.comp.core.Model.QcProjectionCount[String]] = List(QcProjectionCount(NORMAL,9,1361), 
QcProjectionCount(NORMAL,1,3304), QcProjectionCount(TUMOR,20,844), QcProjectionCount(NORMAL,12,1564), 
QcProjectionCount(TUMOR,19,2607), QcProjectionCount(TUMOR,21,476), QcProjectionCount(TUMOR,1,3376), 
QcProjectionCount(TUMOR,13,547), QcProjectionCount(TUMOR,Y,12), QcProjectionCount(NORMAL,11,2300), 
QcProjectionCount(TUMOR,22,733), QcProjectionCount(NORMAL,2,2197), QcProjectionCount(TUMOR,12,1520), 
QcProjectionCount(NORMAL,7,1522), QcProjectionCount(NORMAL,16,1387), QcProjectionCount(NORMAL,18,54...
```

That doesn't look right, we'd prefer to see some graphical output. Let's consult the `.help` function once more, this time we invoke it on the result of the `qcComparison.snpCountByContig` calculation:

```Scala
qcComparison.snpCountByContig.help
```
```
res17: String = 
- groupedBarChart
- lineChart
```

Okay, a histogram is probably the most sensible chart for a SNP count by contig, so let's choose that one:

```Scala
qcComparison.snpCountByContig
            .groupedBarChart(x_title = "SNP count by contig", 
                             x_order = true)
```

We also specified some overriding attributes of the grouped bar chart. The result is automagically turned into a chart.

![chart](img/snpCount.png)

Nice!

This sequence of steps illustrates the primary usage pattern of the VCF-comp library.

Let's now apply this for a SNP comparison.

### Start a SNP comparison

`TODO`

### Discoverable API

`TODO`

## ADVANCED USAGE

`TODO`

## HOW IT WORKS

### Pimp my library

VCF-comp makes use of a Scala idiom called [*"Pimp my library"*](http://www.artima.com/weblogs/viewpost.jsp?thread=179766), through Scala's implicit conversions.

`TODO`

### Dimple.js

`TODO`

