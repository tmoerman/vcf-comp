package org.tmoerman.vcf.comp

import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.snpeff.SnpEffContext._
import org.apache.spark.{SparkContext, Logging}
import org.tmoerman.vcf.comp.VcfComparison._

/**
 * @author Thomas Moerman
 */
object VcfComparisonContext {

  implicit def toVcfComparisonContext(sc: SparkContext): VcfComparisonContext = new VcfComparisonContext(sc)

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
      .takeWhile(line => line.startsWith("##"))
      .map(_.drop(2).split("=", 2) match { case Array(l, r, _*) => (l, r) })
      .foldLeft(Map[String, List[String]]()){ case (m, (k, v)) => m + (k -> m.get(k).map(v :: _).getOrElse(v :: Nil)) }

  /**
   * @param vcfFileA
   * @param vcfFileB
   * @param cache Default true: cache the resulting RDD.
   * @return Returns an RDD that acts as the basis for the comparison analysis.
   */
  def startComparison(vcfFileA: String, vcfFileB: String, cache: Boolean = true): RDD[ComparisonRow] = {
    val aRDD = sc.loadAnnotatedGenotypes(vcfFileA)
    val bRDD = sc.loadAnnotatedGenotypes(vcfFileB)

    val result = compare(aRDD, bRDD)

    if (cache) result.cache() else result
  }

  val help =
    """
      | Documentation goes here.
    """.stripMargin

}
