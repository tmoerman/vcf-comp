package org.tmoerman.vcf.comp.core

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import Model._

import scala.reflect.ClassTag

class QCRDDFunctions(val rdd: RDD[VariantContext]) extends Serializable with Logging {

  def variantTypeCount      = countByProjection(v => variantType(v.variant.variant))

  def readDepthDistribution(bin: ReadDepth => Double = identity)  = countByProjections(_.genotypes.map(g => bin(readDepth(g))))

  def qualityDistribution  (bin: Double => Double = quantize(25)) = countByProjections(_.genotypes.map(g => bin(quality(g))))

  def multiAllelicRatio     = countByProjection(_.genotypes.exists(fromMultiAllelic))

  def indelLengthDistribution =
    rdd
      .map(_.variant.variant)
      .filter(isIndel)
      .map(indelLength)
      .toProjectionCount

  def countByProjections[P: ClassTag](projection: VariantContext => Iterable[P]) =
    rdd
      .flatMap(projection)
      .toProjectionCount

  def countByProjection[P: ClassTag](projection: VariantContext => P) =
    rdd
      .map(projection)
      .toProjectionCount

  private implicit class ProjectionRDDFunctions[P](val ps: RDD[P]) {

    def toProjectionCount: Iterable[ProjectionCount[P]] =
      ps
        .countByValue
        .toMap
        .map{ case (v, c) => ProjectionCount(v, c) }

  }

}
