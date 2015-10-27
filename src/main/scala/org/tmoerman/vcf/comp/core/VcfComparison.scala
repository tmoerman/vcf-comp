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

  // TODO comparison row is not the main abstraction, (cat, rep) is!

  def compare(rddA: RDD[AnnotatedGenotype],
              rddB: RDD[AnnotatedGenotype]): RDD[(Category, AnnotatedGenotype)] = {

    def prep(rdd: RDD[AnnotatedGenotype]) : RDD[(VariantKey, AnnotatedGenotype)] =
      rdd
        .keyBy(variantKey)
        .reduceByKey(withMax(quality))

    prep(rddA).cogroup(prep(rddB))
      .map(dropKey)
      .map{ case (a, b) => (a.headOption, b.headOption) }
      .map(catRep)
  }

  def countVariantTypes(rdd: RDD[(Category, AnnotatedGenotype)]): Map[(Category, VariantType), Count] =
    rdd
      .map { case (cat, rep) => (cat, variantType(rep.getGenotype.getVariant)) }
      .countByValue
      .toMap

  def countSNPs(rdd: RDD[(Category, AnnotatedGenotype)]): Map[Category, Count] =
    rdd
      .filter{ case (_, rep) => isSnp(rep) }
      .map{ case (cat, _) => cat }
      .countByValue
      .toMap

  def countBaseChangesPatterns(rdd: RDD[(Category, AnnotatedGenotype)]): Map[(Category, BaseChangePattern), Count] =
    rdd
      .filter{ case (_, rep) => isSnp(rep) }
      .map{ case (cat, rep) => (cat, baseChangePattern(rep)) }
      .countByValue
      .toMap

  def readDepthDistribution(snpOnly: Boolean = true)(rdd: RDD[(Category, AnnotatedGenotype)]): Map[(Category, ReadDepth), Count] =
    rdd
      .filter{ case (_, rep) => if (snpOnly) isSnp(rep) else true }
      .map{ case (cat, rep) => (cat, readDepth(rep)) }
      .countByValue
      .toMap

  def qualityDistribution(snpOnly: Boolean = true)(rdd: RDD[(Category, AnnotatedGenotype)]): Map[(Category, Quality), Count] =
    rdd
      .filter{ case (_, rep) => if (snpOnly) isSnp(rep) else true }
      .map{ case (cat, rep) => (cat, quality(rep)) }
      .countByValue
      .toMap

}