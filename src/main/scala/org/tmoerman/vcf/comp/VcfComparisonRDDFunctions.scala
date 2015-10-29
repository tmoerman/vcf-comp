package org.tmoerman.vcf.comp

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.VcfComparison._
import org.tmoerman.adam.fx.snpeff.model.RichAnnotated._
import org.tmoerman.vcf.comp.util.Victorinox._

class VcfComparisonRDDFunctions(val rdd: RDD[(Category, AnnotatedGenotype)])(implicit val labels: Option[Labels] = None) extends Serializable with Logging {

  def snpCount =
    rdd
      .filter{ case (_, rep) => isSnp(rep) }
      .map{ case (cat, _) => cat }
      .countByValue
      .toMap

  def variantTypeCount = countByCategory(snpOnly = false)(variantType)(rdd)

  def snpBaseChangeCount = countByCategory(snpOnly = true)(baseChangeString)(rdd)

  def snpBaseChangesPatternCount = countByCategory(snpOnly = true)(baseChangePatternString)(rdd)

  def readDepthDistribution(snpOnly: Boolean = true, bin: Int => Int = identity) =
    countByCategory(snpOnly)(readDepth _ andThen bin)(rdd)

  def qualityDistribution(snpOnly: Boolean = true, bin: Double => Double = identity) =
    countByCategory(snpOnly)(quality _ andThen bin)(rdd)

  def alleleFrequencyDistribution(snpOnly: Boolean = true, bin: Double => Double = identity) =
    countByCategory(snpOnly)(alleleFrequency _ andThen bin)(rdd)

  def clinvarRatio(snpOnly: Boolean = true) = countByCategory(snpOnly)(hasClinvarAnnotations(_))(rdd)

  def dbSnpRatio(snpOnly: Boolean = true) = countByCategory(snpOnly)(hasDbSnpAnnotations(_))(rdd)

  def snpEffRatio(snpOnly: Boolean = true) = countByCategory(snpOnly)(hasSnpEffAnnotations(_))(rdd)

  /**
   * @return Returns a help String.
   */
  def help =
    """
      | Documentation goes here.
    """.stripMargin

}