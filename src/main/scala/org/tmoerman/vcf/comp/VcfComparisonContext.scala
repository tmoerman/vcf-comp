package org.tmoerman.vcf.comp

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.SnpEffContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.{SparkContext, Logging}
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.{QcComparisonRDDFunctions, SnpComparisonRDDFunctions, SnpComparison, QcComparison}
import SnpComparison._
import QcComparison._
import org.tmoerman.vcf.comp.util.ApiHelp

/**
 * @author Thomas Moerman
 */
object VcfComparisonContext {

  implicit def pimpSparkContext(sc: SparkContext): VcfComparisonContextFunctions = new VcfComparisonContextFunctions(sc)

  implicit def pimpQcComparisonRDD(rdd: RDD[(Label, VariantContext)]): QcComparisonRDDFunctions = new QcComparisonRDDFunctions(rdd)

  implicit def pimpSnpComparisonRDD(rdd: RDD[OccurrenceRow[AnnotatedGenotype]]): SnpComparisonRDDFunctions = new SnpComparisonRDDFunctions(rdd)

}

class VcfComparisonContextFunctions(private[this] val sc: SparkContext) extends Serializable with ApiHelp {

  /**
   * @param vcfFile Name of the VCF file.
   * @return Returns a multimap of Strings representing the meta information of the VCF file.
   */
  def getMetaFields(vcfFile: String): Map[String, List[String]] = meta(sc.textFile(vcfFile))

  /**
    * @param vcfFileA
    * @param vcfFileB
    * @param params
    * @return Returns an RDD that acts as the basis for the Quality Control analysis.
    */
  def startQcComparison(vcfFileA: String, vcfFileB: String, params: ComparisonParams = ComparisonParams()) =
    qcComparison(params)(sc.loadVcf(vcfFileA, None),
                         sc.loadVcf(vcfFileB, None))

  /**
   * @param vcfFileA
   * @param vcfFileB
   * @param params
   * @return Returns an RDD that acts as the basis for the comparison analysis.
   */
  def startSnpComparison(vcfFileA: String, vcfFileB: String, params: ComparisonParams = ComparisonParams()) =
    snpComparison(params)(sc.loadAnnotatedGenotypes(vcfFileA),
                          sc.loadAnnotatedGenotypes(vcfFileB))

}