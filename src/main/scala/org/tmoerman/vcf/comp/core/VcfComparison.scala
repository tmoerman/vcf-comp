package org.tmoerman.vcf.comp.core

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.vcf.comp.core.Model._

import org.tmoerman.vcf.comp.util.Victorinox._

/**
 * @author Thomas Moerman
 */
object VcfComparison extends Serializable with Logging {

  def compare(rddA: RDD[AnnotatedGenotype],
              rddB: RDD[AnnotatedGenotype]): RDD[(Category, AnnotatedGenotype)] = {

    def prep(rdd: RDD[AnnotatedGenotype]) : RDD[(VariantKey, AnnotatedGenotype)] =
      rdd
        .keyBy(variantKey)
        .reduceByKey(withMax(quality))

    prep(rddA).cogroup(prep(rddB))
      .map(dropKey)
      .map{ case (a, b) => (a.headOption, b.headOption) }
      .map(catRep()) // TODO maybe add labels
  }

  protected def maybeSnp(snpOnly: Boolean)(rep: AnnotatedGenotype): Boolean = if (snpOnly) isSnp(rep) else true

  def snpCount(rdd: RDD[(Category, AnnotatedGenotype)]): Map[Category, Count] =
    rdd
      .filter{ case (_, rep) => isSnp(rep) }
      .map{ case (cat, _) => cat }
      .countByValue
      .toMap

  protected def countByCategory[T](snpOnly: Boolean)
                                  (f: AnnotatedGenotype => T)
                                  (rdd: RDD[(Category, AnnotatedGenotype)]): Map[(Category, T), Count] =
    rdd
      .filter{ case (_, rep) => maybeSnp(snpOnly)(rep) }
      .mapValues(f)
      .countByValue
      .toMap

  def variantTypeCount(rdd: RDD[(Category, AnnotatedGenotype)]): Map[(Category, VariantType), Count] =
    countByCategory(snpOnly = false)(variantType)(rdd)

  def countSnpBaseChanges(rdd: RDD[(Category, AnnotatedGenotype)]) =
    countByCategory(snpOnly = true)(baseChange)(rdd)

  def countSnpBaseChangesPatterns(rdd: RDD[(Category, AnnotatedGenotype)]) =
    countByCategory(snpOnly = true)(baseChangePattern)(rdd)

  def readDepthDistribution(snpOnly: Boolean = true)(rdd: RDD[(Category, AnnotatedGenotype)]) =
    countByCategory(snpOnly)(readDepth)(rdd)

  def qualityDistribution(snpOnly: Boolean = true)(rdd: RDD[(Category, AnnotatedGenotype)]) =
    countByCategory(snpOnly)(quality)(rdd) // TODO bin quality

  def alleleFrequencyDistribution(snpOnly: Boolean = true)(rdd: RDD[(Category, AnnotatedGenotype)]) =
    countByCategory(snpOnly)(alleleFrequency)(rdd) // TODO bin allele frequency

}