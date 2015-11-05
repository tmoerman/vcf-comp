package org.tmoerman.vcf.comp.core

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.model.RichAnnotated._
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.SnpComparison._

class SnpComparisonRDDFunctions(val rdd: RDD[Map[Category, Iterable[AnnotatedGenotype]]]) extends Serializable with Logging {

  // This delegate selection strategy should be compared to simply flattening the data structure

  private def electDelegate(genotypes: Iterable[AnnotatedGenotype]): AnnotatedGenotype = genotypes.maxBy(quality)

  lazy val delegates: RDD[(Category, AnnotatedGenotype)] = rdd.flatMap(_.mapValues(electDelegate))

  def categoryCount =
    delegates
      .map{ case (cat, _) => cat }
      .countByValue
      .toMap

  def baseChangeCount = countByCategory(baseChangeString)

  def baseChangesPatternCount = countByCategory(baseChangePatternString)

  // TODO sensible default binning strategy

  def readDepthCount(bin: Int => Int = identity) = countByCategory(readDepth _ andThen bin)

  def qualityCount(bin: Double => Double = identity) = countByCategory(quality _ andThen bin)

  def alleleFrequencyCount(bin: Double => Double = identity) = countByCategory(alleleFrequency _ andThen bin)

  def zygosityCount = countByCategory(zygosity)

  def functionalImpactCount = countByCategory(functionalImpact)

  def clinvarRatio = countByCategory(hasClinvarAnnotations(_))

  def commonSnpRatio = countByCategory(hasDbSnpAnnotations(_))

  def countByCategory[P](projection: AnnotatedGenotype => P): Map[(Category, P), Count] =
    delegates
      .mapValues(projection)
      .countByValue
      .toMap

  // TODO (perhaps not here: multiallelic site statistic)

  /**
   * @return Returns a help String.
   */
  def help =
    """
      | Documentation goes here.
    """.stripMargin

  //  def withReadDepth(predicate: (ReadDepth) => Boolean) =
  //    rdd.filter{ case (_, genotype) => predicate(readDepth(genotype)) }
  //
  //  def withQuality(predicate: (Quality) => Boolean)(entry: (Category, AnnotatedGenotype)): Boolean =
  //    predicate(quality(entry._2))

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