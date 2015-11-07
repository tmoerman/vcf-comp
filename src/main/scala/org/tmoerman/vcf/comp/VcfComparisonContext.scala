package org.tmoerman.vcf.comp

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rich.RichVariant
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.SnpEffContext._
import org.apache.spark.{SparkContext, Logging}
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.{SnpComparisonRDDFunctions, SnpComparison}
import SnpComparison._

/**
 * @author Thomas Moerman
 */
object VcfComparisonContext {

  implicit def toVcfComparisonContext(sc: SparkContext): VcfComparisonContext = new VcfComparisonContext(sc)

  implicit def pimpSnpComparisonRDD(rdd: RDD[(Category, AnnotatedGenotype)]) : SnpComparisonRDDFunctions = new SnpComparisonRDDFunctions(rdd)

}

class VcfComparisonContext(val sc: SparkContext) extends Serializable with Logging {

  /**
   * @param vcfFile Name of the VCF file.
   * @return Returns a multimap of Strings representing the meta information of the VCF file.
   */
  def getMetaFields(vcfFile: String): Map[String, List[String]] =
    sc
      .textFile(vcfFile)
      .toLocalIterator
      .toList
      .takeWhile(line => line.startsWith("##"))
      .map(_.drop(2).split("=", 2) match { case Array(l, r, _*) => (l, r) })
      .groupBy(_._1)
      .mapValues(_.map(_._2))

      //.foldLeft(Map[String, List[String]]()){ case (m, (k, v)) => m + (k -> m.get(k).map(v :: _).getOrElse(v :: Nil)) }

  /**
   * @param vcfFileA
   * @param vcfFileB
   * @return Returns an RDD that acts as the basis for the comparison analysis.
   */
  def startSnpComparison(vcfFileA: String,
                         vcfFileB: String,
                         params: SnpComparisonParams = new SnpComparisonParams()): RDD[(Category, AnnotatedGenotype)] = {

    val aRDD = sc.loadAnnotatedGenotypes(vcfFileA)
    val bRDD = sc.loadAnnotatedGenotypes(vcfFileB)

    compareSnps(params)(aRDD, bRDD)
  }

}