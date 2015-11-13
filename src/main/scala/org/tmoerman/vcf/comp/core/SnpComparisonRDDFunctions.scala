package org.tmoerman.vcf.comp.core

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.model.RichAnnotated._
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.SnpComparison._

object SnpComparisonRDDFunctions {

  private val CLINVAR_LABELS    = Map(true -> "Clinvar",     false -> "Not Clinvar")
  private val COMMON_SNP_LABELS = Map(true -> "Common SNP",  false -> "Not Common SNP")
  private val SYNONYMOUS_LABELS = Map(true -> "Synonoymous", false -> "Non Synonymous")

}

class SnpComparisonRDDFunctions(val rdd: RDD[(Category, AnnotatedGenotype)]) extends Serializable with Logging {
  import SnpComparisonRDDFunctions._

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

  def clinvarRatio               (label: Boolean => String = CLINVAR_LABELS)    = countByProjection(g => label(hasClinvarAnnotations(g)))

  def commonSnpRatio             (label: Boolean => String = COMMON_SNP_LABELS) = countByProjection(g => label(hasDbSnpAnnotations(g)))

  def synonymousRatio            (label: Boolean => String = SYNONYMOUS_LABELS) = countByProjectionOption(g => isSynonymous(g).map(label))

  def readDepthDistribution      (bin: ReadDepth => Double = identity)   = countByProjection(g => bin(readDepth(g)))

  def qualityDistribution        (bin: Double => Double = quantize(25))  = countByProjection(g => bin(quality(g)))

  def alleleFrequencyDistribution(bin: Double => Double = quantize(.01)) = countByProjection(g => bin(alleleFrequency(g)))

  def countByProjection[P](projection: AnnotatedGenotype => P): Iterable[CategoryProjectionCount[P]] =
    rdd
      .map{ case (cat, rep) => (name(cat), rep) }
      .mapValues(projection(_))
      .countByValue
      .map{ case ((cat, p), count) => CategoryProjectionCount(cat, p, count) }

  def countByProjectionOption[P](projection: AnnotatedGenotype => Option[P]): Iterable[CategoryProjectionCount[P]] =
    rdd
      .map{ case (cat, rep) => (name(cat), rep) }
      .flatMapValues(projection(_))
      .countByValue
      .map{ case ((cat, p), count) => CategoryProjectionCount(cat, p, count) }

  /**
   * @return Returns a help String.
   */
  def help =
    """
      | Documentation goes here.
    """.stripMargin

}