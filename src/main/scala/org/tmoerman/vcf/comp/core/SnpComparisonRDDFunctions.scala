package org.tmoerman.vcf.comp.core

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.BroadcastRegionJoin
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.model.RichAnnotated._
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.SnpComparison._
import org.tmoerman.vcf.comp.util.ApiHelp
import org.bdgenomics.formats.avro.Feature

object SnpComparisonLabels {

  val CLINVAR_LABELS    = Map(true -> "Clinvar",     false -> "Not Clinvar")
  val COMMON_SNP_LABELS = Map(true -> "Common SNP",  false -> "Not Common SNP")
  val SYNONYMOUS_LABELS = Map(true -> "Synonoymous", false -> "Missense")

}

class SnpComparisonRDDFunctions(private[this] val rdd: RDD[OccurrenceRow[AnnotatedGenotype]]) extends Serializable with ApiHelp {
  import SnpComparisonLabels._

  /**
    * @param occurrences vararg. Accepts one or more Strings from "unique", "concordant", "discordant".
    * @return Returns the RDD, filtered on the specified occurrences.
    */
  def viewOnly(occurrences: String*): RDD[OccurrenceRow[AnnotatedGenotype]] =
    rdd.map(row => row.filterKeys(occurrence =>
      occurrences.contains(occurrence) ||
      occurrences.map(_.toLowerCase).contains(occurrence.toLowerCase)))

  /**
    * @return Returns SNP count by concordance category.
    */
  def categoryCount: Iterable[CategoryCount] =
    rdd
      .flatMap(flattenToReps())
      .map{ case (cat, _) => name(cat) }
      .countByValue
      .map{ case (cat, count) => CategoryCount(cat, count) }

  /**
    * @return Returns SNP count by base change per concordance category.
    *
    *         Base change is a String: "ref->alt" where ref and alt are the variant alleles, e.g. "A->T", "G->C", etc...
    */
  def baseChangeCount = countByProjection(baseChangeString)

  /**
    * @return Returns SNP count by base change pattern per concordance category.
    *
    *         Base change pattern is a String: "ref:alt", analogous to base change, but without taking into account
    *         the order of ref to alt, e.g. "A:T", "C:G", etc...
    */
  def baseChangePatternCount = countByProjection(baseChangePatternString)

  /**
    * @return Returns SNP count by base change type per concordance category.
    *
    *         Base change types are "Ti" (Transition) and "Tv" (Transversion).
    */
  def baseChangeTypeCount = countByProjection(baseChangeType)

  /**
    * @return Returns SNP count by zygosity per concordance category.
    *
    *         Zygosity values are: "HOM_REF", "HET", "HOM_ALT", "NO_CALL".
    */
  def zygosityCount = countByTraversableProjection(zygosity)

  /**
    * @return Returns SNP count by functional impact per concordance category.
    *
    *         Functional impact is a scale value provided by SnpEff: "HIGH", "MODERATE", "LOW" and "MODIFIER".
    *
    *         Cfr. SnpEff http://snpeff.sourceforge.net/VCFannotationformat_v1.0.pdf
    */
  def functionalImpactCount = countByProjection(functionalImpact)

  /**
    * @return Returns SNP count by functional annotation per concordance category.
    *
    *         Functional annotation is a SnpEff annotation, including: "synonymous_variant", "missense_variant",
    *         "stop_gained", "start_lost".
    *
    *         Assumes annotation with SnpEff http://snpeff.sourceforge.net/VCFannotationformat_v1.0.pdf
    */
  def functionalAnnotationCount = countByProjection(functionalAnnotation)

  /**
    * @return Returns SNP count by transcript biotype per concordance category.
    *
    *         Transcript biotype is a SnpEff annotation: including: "protein_coding", "retained_intron"
    *         "nonsense_mediated_decay".
    *
    *         Assumes annotation with SnpEff http://snpeff.sourceforge.net/VCFannotationformat_v1.0.pdf
    */
  def transcriptBiotypeCount = countByProjection(transcriptBiotype)

  /**
    * @param label (optional). Maps the Boolean to a descriptive label.
    * @return Returns the ratio of SNPs with a Clinvar annotation vs. SNPs without.
    *
    *         Assumes VCF annotation with SnpSift http://snpeff.sourceforge.net/SnpSift.html
    */
  def clinvarRatio(label: Boolean => String = CLINVAR_LABELS) = countByProjection(g => label(hasClinvarAnnotations(g)))

  /**
    * @param label (optional). Maps the Boolean to a descriptive label.
    * @return Returns the ratio of SNPs with a DbSNP annotation vs. SNPs without.
    *
    *         Assumes VCF annotation with SnpSift http://snpeff.sourceforge.net/SnpSift.html
    */
  def commonSnpRatio(label: Boolean => String = COMMON_SNP_LABELS) = countByProjection(g => label(hasDbSnpAnnotations(g)))

  /**
    * @param label (optional). Maps the boolean to a descriptive label.
    * @return Returns the ratio of SNPs with "synonymous_variant" vs. "missense_variant" annotation. If a SNP has
    *         neither annotation, it is not taken into account.
    *
    *         Assumes annotation with SnpEff http://snpeff.sourceforge.net/VCFannotationformat_v1.0.pdf
    */
  def synonymousRatio(label: Boolean => String = SYNONYMOUS_LABELS) = countByTraversableProjection(g => isSynonymous(g).map(label))

  /**
    * @param step (optional). Size of the step interval for binning the read depth values.
    * @return Returns the distribution of SNPs by read depth.
    */
  def readDepthDistribution(step: Int = DEFAULT_READ_DEPTH_STEP) = countByProjection(g => quantize(step)(readDepth(g)))

  /**
    * @param step (optional). Size of the step interval for binning the quality values.
    * @return Returns the distribution of SNPs by quality.
    */
  def qualityDistribution(step: Double = DEFAULT_QUALITY_STEP) = countByProjection(g => quantize(step)(quality(g)))

  /**
    * @param step (optional). Size of the step interval for binning the allele frequency values.
    * @return Returns the distribution of SNPs by allele frequency.
    */
  def alleleFrequencyDistribution(step: Double = DEFAULT_ALLELE_FREQUENCY_STEP) = countByProjection(g => quantize(step)(alleleFrequency(g)))

  def zygositySwitchCount = countBySwitch(zygosity)

  def allelesSwitchCount = countBySwitch(a => Some(genotypeAllelesString(a)))

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
      .flatMapValues(projection(_)) // flatMap acts as a filter if the projection returns an empty Traversable
      .countByValue
      .map{ case ((cat, p), count) => CategoryProjectionCount(cat, p, count) }

  private def toRegion(g: AnnotatedGenotype): ReferenceRegion = {
    val variant = g.getGenotype.getVariant

    ReferenceRegion(
      variant.getContig.getContigName,
      variant.getStart,
      variant.getEnd)
  }

  def filterByGenes(queryGeneNames: String*)
                   (implicit featureRDD: RDD[Feature]): RDD[OccurrenceRow[AnnotatedGenotype]] = {

    val featureByRegionRDD: RDD[(ReferenceRegion, Feature)] =
      featureRDD
        .filter(feature =>
          queryGeneNames.exists(queryGene =>
            feature.getFeatureType.equalsIgnoreCase(queryGene)))
        .keyBy(ReferenceRegion(_))

    val occurrenceRowByRegionRDD =
      rdd.flatMap(row =>
        flattenToReps()(row)
          .map{ case (_, genotype) => toRegion(genotype) }
          .map(region => (region, row)))

    BroadcastRegionJoin
      .partitionAndJoin(featureByRegionRDD, occurrenceRowByRegionRDD)
      .values
  }

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