package org.tmoerman.vcf.comp

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rich.RichVariant
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.vcf.comp.core.Model._
import org.tmoerman.vcf.comp.core.VcfComparison

class RichVariantRDDFunctions(val rdd: RDD[RichVariant]) extends Serializable with Logging {



}

class ComparisonRDDFunctions(val rdd: RDD[(Category, AnnotatedGenotype)]) extends Serializable with Logging {

  def snpCount = VcfComparison.snpCount(rdd)

  def variantTypeCount = VcfComparison.variantTypeCount(rdd)

  def snpBaseChangeCount = VcfComparison.countSnpBaseChanges(rdd)

  def snpBaseChangesPatternCount = VcfComparison.countSnpBaseChangesPatterns(rdd)

  def readDepthDistribution(snpOnly: Boolean = true) = VcfComparison.readDepthDistribution(snpOnly)(rdd)

  def qualityDistribution(snpOnly: Boolean = true) = VcfComparison.qualityDistribution(snpOnly)(rdd)

  def alleleFrequencyDistribution(snpOnly: Boolean = true) = VcfComparison.alleleFrequencyDistribution(snpOnly)(rdd)

}