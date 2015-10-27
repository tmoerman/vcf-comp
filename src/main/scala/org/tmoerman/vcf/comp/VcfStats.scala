package org.tmoerman.vcf.comp

import org.apache.spark.rdd.RDD

import Model._
import org.bdgenomics.formats.avro.Variant

/**
 * @author Thomas Moerman
 */
object VcfStats {

  def countVariantTypes(variants: RDD[Variant]): Map[VariantType, Count] =
    variants
      .map(toVariantType)
      .countByValue
      .toMap

  def countBaseChanges(variants: RDD[Variant]): Map[BaseChange, Count] =
    variants
      .filter(isSNP)
      .map(toBaseChange)
      .countByValue
      .toMap

  def countMutationPatterns(variants: RDD[Variant]): Map[BaseChangePattern, Count] =
    variants
      .filter(isSNP)
      .map(toBaseChange)
      .map(toBaseChangePattern)
      .countByValue
      .toMap

}