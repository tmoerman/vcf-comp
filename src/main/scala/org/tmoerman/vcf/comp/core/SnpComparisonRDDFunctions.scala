package org.tmoerman.vcf.comp.core

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.model.RichAnnotated._
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.SnpComparison._

class SnpComparisonRDDFunctions(val rdd: RDD[(Category, AnnotatedGenotype)]) extends Serializable with Logging {

  def viewOnly(occurrences: Occurrence*): RDD[(Category, AnnotatedGenotype)] =
    rdd
      .filter{ case ((_, occurrence), _) => occurrences.contains(occurrence) ||
                                            occurrences.map(_.toLowerCase).contains(occurrence.toLowerCase) }

  def categoryCount: Iterable[CategoryCount] =
    rdd
      .map{ case (cat, _) => name(cat) }
      .countByValue
      .map{ case (cat, count) => CategoryCount(cat, count) }

  def baseChangeCount           = countByProjection(baseChangeString)

  def baseChangePatternCount    = countByProjection(baseChangePatternString)

  def baseChangeTypeCount       = countByProjection(baseChangeType)

  def zygosityCount             = countByProjection(zygosity)

  def functionalImpactCount     = countByProjection(functionalImpact)

  def functionalAnnotationCount = countByProjection(functionalAnnotation)

  def transcriptBiotypeCount    = countByProjection(transcriptBiotype)

  def clinvarRatio              = countByProjection(hasClinvarAnnotations(_))

  def commonSnpRatio            = countByProjection(hasDbSnpAnnotations(_))

  def readDepthDistribution      (bin: ReadDepth => Double = identity)   = countByProjection[Double](g => bin(readDepth(g)))

  def qualityDistribution        (bin: Double => Double = quantize(25))  = countByProjection[Quality](g => bin(quality(g)))

  def alleleFrequencyDistribution(bin: Double => Double = quantize(.01)) = countByProjection[AlleleFrequency](g => bin(alleleFrequency(g)))

  def countByProjection[P](projection: AnnotatedGenotype => P): Iterable[CategoryProjectionCount[P]] =
    rdd
      .map{ case (cat, rep) => (name(cat), rep) }
      .mapValues(projection)
      .countByValue
      .map{ case ((cat, p), count) => CategoryProjectionCount(cat, p, count) }

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