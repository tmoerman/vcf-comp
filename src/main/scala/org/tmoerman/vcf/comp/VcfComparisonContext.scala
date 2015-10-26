package org.tmoerman.vcf.comp

import org.apache.spark.{SparkContext, Logging}

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



}
