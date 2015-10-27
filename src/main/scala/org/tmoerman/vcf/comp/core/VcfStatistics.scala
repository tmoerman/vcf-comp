package org.tmoerman.vcf.comp.core

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Variant
import org.tmoerman.vcf.comp.core.Model._

/**
 * @author Thomas Moerman
 */
object VcfStatistics {

  def countVariantTypes(variants: RDD[Variant]): Map[VariantType, Count] =
    variants
      .map(variantType)
      .countByValue
      .toMap

  def countBaseChanges(variants: RDD[Variant]): Map[BaseChange, Count] =
    variants
      .filter(isSnp)
      .map(baseChange)
      .countByValue
      .toMap

  def countMutationPatterns(variants: RDD[Variant]): Map[BaseChangePattern, Count] =
    variants
      .filter(isSnp)
      .map(baseChange)
      .map(baseChangePattern)
      .countByValue
      .toMap

}