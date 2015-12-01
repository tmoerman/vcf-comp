package org.tmoerman.vcf.comp.core

import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.model.RichAnnotated._
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.SnpComparison._
import org.tmoerman.vcf.comp.util.ApiHelp

object SnpComparisonLabels {

  val CLINVAR_LABELS    = Map(true -> "Clinvar",     false -> "Not Clinvar")
  val COMMON_SNP_LABELS = Map(true -> "Common SNP",  false -> "Not Common SNP")
  val SYNONYMOUS_LABELS = Map(true -> "Synonoymous", false -> "Missense")

}

class SnpComparisonRDDFunctions(private[this] val rdd: RDD[OccurrenceRow[AnnotatedGenotype]]) extends Serializable with ApiHelp {
  import SnpComparisonLabels._

  def viewOnly(occurrences: Occurrence*): RDD[OccurrenceRow[AnnotatedGenotype]] =
    rdd.map(row => row.filterKeys(occurrence =>
      occurrences.contains(occurrence) ||
      occurrences.map(_.toLowerCase).contains(occurrence.toLowerCase)))

  def categoryCount: Iterable[CategoryCount] =
    rdd
      .flatMap(flattenToReps())
      .map{ case (cat, _) => name(cat) }
      .countByValue
      .map{ case (cat, count) => CategoryCount(cat, count) }

  def baseChangeCount           = countByProjection(baseChangeString)

  def baseChangePatternCount    = countByProjection(baseChangePatternString)

  def baseChangeTypeCount       = countByProjection(baseChangeType)

  def zygosityCount             = countByTraversableProjection(zygosity)

  def functionalImpactCount     = countByProjection(functionalImpact)

  def functionalAnnotationCount = countByProjection(functionalAnnotation)

  def transcriptBiotypeCount    = countByProjection(transcriptBiotype)

  def clinvarRatio                (label: Boolean => String = CLINVAR_LABELS)    = countByProjection(g => label(hasClinvarAnnotations(g)))

  def commonSnpRatio              (label: Boolean => String = COMMON_SNP_LABELS) = countByProjection(g => label(hasDbSnpAnnotations(g)))

  def synonymousRatio             (label: Boolean => String = SYNONYMOUS_LABELS) = countByTraversableProjection(g => isSynonymous(g).map(label))

  def readDepthDistribution       (step: ReadDepth = DEFAULT_READ_DEPTH_STEP)    = countByProjection(g => quantize(step)(readDepth(g)))

  def qualityDistribution         (step: Quality = DEFAULT_QUALITY_STEP)         = countByProjection(g => quantize(step)(quality(g)))

  def alleleFrequencyDistribution (step: Double = DEFAULT_ALLELE_FREQUENCY_STEP) = countByProjection(g => quantize(step)(alleleFrequency(g)))

  def zygositySwitchCount       = countBySwitch(zygosity)

  def allelesSwitchCount        = countBySwitch(a => Some(genotypeAllelesString(a)))

  def countByProjection[P](projection: AnnotatedGenotype => P): Iterable[CategoryProjectionCount[P]] =
    rdd
      .flatMap(flattenToReps())
      .map{ case (cat, rep) => (name(cat), rep) }
      .mapValues(projection(_))
      .countByValue
      .map{ case ((cat, p), count) => CategoryProjectionCount(cat, p, count) }

  def countByTraversableProjection[P](projection: AnnotatedGenotype => Traversable[P]): Iterable[CategoryProjectionCount[P]] =
    rdd
      .flatMap(flattenToReps())
      .map{ case (cat, rep) => (name(cat), rep) }
      .flatMapValues(projection(_))
      .countByValue
      .map{ case ((cat, p), count) => CategoryProjectionCount(cat, p, count) }

  def countBySwitch[P](projection: AnnotatedGenotype => Option[P]): Iterable[CategoryProjectionCount[String]] =
    rdd
      .flatMap(_
          .filter { case (occ, labeled) => occ == CONCORDANT || occ == DISCORDANT }
          .flatMap { case (occ, labeled) =>
            val arr = labeled.toArray

            for {projectionA <- arr(0)._2.values.flatMap(projection(_))
                 projectionB <- arr(1)._2.values.flatMap(projection(_))
                 if projectionA != projectionB // only switches are interesting
                 switch = s"$projectionA -> $projectionB"} yield (occ, switch)
          })
      .countByValue
      .map{ case ((cat, p), count) => CategoryProjectionCount(cat, p, count) }

}