package org.tmoerman.vcf.comp

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.VcfComparison._
import org.tmoerman.adam.fx.snpeff.model.RichAnnotated._
import org.tmoerman.vcf.comp.util.Victorinox._

class VcfComparisonRDDFunctions(val rdd: RDD[Iterable[(Category, AnnotatedGenotype)]]) extends Serializable with Logging {

  def snpCount =
    rdd
      .flatMap(identity)
      .map{ case (cat, _) => cat }
      .countByValue
      .toMap

  def snpBaseChangeCount = countByCategory(baseChangeString)(rdd)

  def snpBaseChangesPatternCount = countByCategory(baseChangePatternString)(rdd)

  // TODO sensible default binning strategy

  def readDepthCount(bin: Int => Int = identity) = countByCategory(readDepth _ andThen bin)(rdd)

  def qualityCount(bin: Double => Double = identity) = countByCategory(quality _ andThen bin)(rdd)

  def alleleFrequencyCount(bin: Double => Double = identity) = countByCategory(alleleFrequency _ andThen bin)(rdd)

  def zygosityCount = countByCategory(zygosity)(rdd)

  def functionalImpactCount = countByCategory(functionalImpact)(rdd)

  def clinvarRatio = countByCategory(hasClinvarAnnotations(_))(rdd)

  def commonSnpRatio = countByCategory(hasDbSnpAnnotations(_))(rdd)

//  def multiAllelicSnpRatio =
//    rdd
//      .filter()

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

//  def enrich(labels: Labels): RDD[String] =
//    rdd
//      .map{ case (cat, rep) => {
//
//        List(
//          rep.getGenotype.getSampleId,
//          rep.getGenotype.getVariant.getContig.getContigName,
//          rep.getGenotype.getVariant.getStart,
//          labels.getOrElse(cat, cat),
//          baseChangeString(rep),
//          baseChangePatternString(rep),
//          readDepth(rep),
//          roundToDecimals(2)(quality(rep)),
//          roundToDecimals(2)(alleleFrequency(rep)),
//          hasClinvarAnnotations(rep),
//          hasDbSnpAnnotations(rep)).mkString("\t") + "\n"
//      }}

}