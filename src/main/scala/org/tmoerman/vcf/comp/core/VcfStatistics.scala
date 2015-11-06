package org.tmoerman.vcf.comp.core

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Variant
import org.tmoerman.vcf.comp.core.Model._

/**
 * @author Thomas Moerman
 */
object VcfStatistics {

  def variantTypeCount(variants: RDD[Variant]): Map[VariantType, Count] =
    variants
      .map(variantType)
      .countByValue
      .toMap

  def multiAllelicRegionCount(variants: RDD[Variant]) = ??? // TODO implement

}