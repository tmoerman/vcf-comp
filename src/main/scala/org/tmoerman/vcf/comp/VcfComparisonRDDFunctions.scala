package org.tmoerman.vcf.comp

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.VcfComparison._
import org.tmoerman.adam.fx.snpeff.model.RichAnnotated._
import org.tmoerman.vcf.comp.util.Victorinox._

class VcfComparisonRDDFunctions(val rdd: RDD[(Category, AnnotatedGenotype)]) extends Serializable with Logging {

  def snpCount =
    rdd
      .filter{ case (_, rep) => isSnp(rep) }
      .map{ case (cat, _) => cat }
      .countByValue
      .toMap

  def snpBaseChangeCount = countByCategory(snpOnly = true)(baseChangeString)(rdd)

  def snpBaseChangesPatternCount = countByCategory(snpOnly = true)(baseChangePatternString)(rdd)

  // TODO sensible default binning strategy

  def readDepthDistribution(bin: Int => Int = identity) = countByCategory(snpOnly = true)(readDepth _ andThen bin)(rdd)

  def qualityDistribution(bin: Double => Double = identity) = countByCategory(snpOnly = true)(quality _ andThen bin)(rdd)

  def alleleFrequencyDistribution(bin: Double => Double = identity) = countByCategory(snpOnly = true)(alleleFrequency _ andThen bin)(rdd)

  def clinvarRatio = countByCategory(snpOnly = true)(hasClinvarAnnotations(_))(rdd)

  def dbSnpRatio = countByCategory(snpOnly = true)(hasDbSnpAnnotations(_))(rdd)

  def synonymousRatio = ??? // TODO implement

//  def withReadDepth(predicate: (ReadDepth) => Boolean) =
//    rdd.filter{ case (_, genotype) => predicate(readDepth(genotype)) }
//
//  def withQuality(predicate: (Quality) => Boolean)(entry: (Category, AnnotatedGenotype)): Boolean =
//    predicate(quality(entry._2))

  // TODO (perhaps not here: multiallelic site statistic)

  /**
   * @return Returns a help String.
   */
  def help =
    """
      | Documentation goes here.
    """.stripMargin

  import org.tmoerman.adam.fx.snpeff.model.RichAnnotatedGenotype._

  def enrich(labels: Labels): RDD[String] =
    rdd
      .map{ case (cat, rep) => {

        List(
          rep.getGenotype.getSampleId,
          rep.getGenotype.getVariant.getContig.getContigName,
          rep.getGenotype.getVariant.getStart,
          labels.getOrElse(cat, cat),
          baseChangeString(rep),
          baseChangePatternString(rep),
          readDepth(rep),
          roundToDecimals(2)(quality(rep)),
          roundToDecimals(2)(alleleFrequency(rep)),
          hasClinvarAnnotations(rep),
          hasDbSnpAnnotations(rep)).mkString("\t") + "\n"
      }}

}