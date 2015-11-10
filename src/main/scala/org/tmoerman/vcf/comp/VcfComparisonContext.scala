package org.tmoerman.vcf.comp

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.SnpEffContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.{SparkContext, Logging}
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.{QCRDDFunctions, SnpComparisonRDDFunctions, SnpComparison, QC}
import SnpComparison._
import QC._

/**
 * @author Thomas Moerman
 */
object VcfComparisonContext {

  implicit def toVcfComparisonContext(sc: SparkContext): VcfComparisonContext = new VcfComparisonContext(sc)

  implicit def pimpQCRDD(rdd: RDD[VariantContext]): QCRDDFunctions = new QCRDDFunctions(rdd)

  implicit def pimpSnpComparisonRDD(rdd: RDD[(Category, AnnotatedGenotype)]): SnpComparisonRDDFunctions = new SnpComparisonRDDFunctions(rdd)

}

class VcfComparisonContext(val sc: SparkContext) extends Serializable with Logging {

  /**
   * @param vcfFile Name of the VCF file.
   * @return Returns a multimap of Strings representing the meta information of the VCF file.
   */
  def getMetaFields(vcfFile: String): Map[String, List[String]] = meta(sc.textFile(vcfFile))

  /**
    * @param vcfFile
    * @param params
    * @return Returns an RDD that acts as the basis for the Quality Control analysis.
    */
  def startQC(vcfFile: String,
              params: VcfQCParams = new VcfQCParams()): RDD[VariantContext] = {

    val rdd = sc.loadGenotypes(vcfFile)

    prep(params)(rdd)
  }

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