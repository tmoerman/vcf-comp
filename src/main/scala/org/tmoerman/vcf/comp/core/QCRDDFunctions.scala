package org.tmoerman.vcf.comp.core

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import Model._
import org.tmoerman.vcf.comp.util.ApiHelp

import scala.reflect.ClassTag

class QCRDDFunctions(private[this] val rdd: RDD[VariantContext]) extends Serializable with ApiHelp {

  def variantTypeCount  = countByProjection(v => variantType(v.variant.variant))

  def multiAllelicRatio    (name: Boolean => String = _.toString) = countByProjection(v => name(v.genotypes.exists(fromMultiAllelic)))

  def readDepthDistribution(bin: ReadDepth => Double = identity)  = countByProjections(_.genotypes.map(g => bin(readDepth(g))))

  def qualityDistribution  (bin: Double => Double = quantize(25)) = countByProjections(_.genotypes.map(g => bin(quality(g))))

  def indelLengthDistribution =
    rdd
      .map(_.variant.variant)
      .filter(isIndel)
      .map(indelLength)
      .countByValue
      .map{ case (v, c) => ProjectionCount(v, c) }

  def countByProjections[P: ClassTag](projection: VariantContext => Iterable[P]) =
    rdd
      .flatMap(projection)
      .countByValue
      .map{ case (v, c) => ProjectionCount(v, c) }

  def countByProjection[P: ClassTag](projection: VariantContext => P) =
    rdd
      .map(projection)
      .countByValue
      .map{ case (v, c) => ProjectionCount(v, c) }

}
