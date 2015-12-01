package org.tmoerman.vcf.comp.core

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import Model._
import org.tmoerman.vcf.comp.util.ApiHelp

import scala.reflect.ClassTag

class QcComparisonRDDFunctions(private[this] val rdd: RDD[(Label, VariantContext)]) extends Serializable with ApiHelp {

  def variantTypeCount() = countByProjection(vc => variantType(vc.variant))

  def snpCountByContig() =
    countByTraversableProjection(trySnp(_).map(_.variant.getContig.getContigName))

  def snpReadDepthDistribution(step: ReadDepth = DEFAULT_READ_DEPTH_STEP) =
    countByTraversableProjection(trySnp(_).flatMap(_.genotypes.map(g => quantize(step)(readDepth(g)))))

  def snpQualityDistribution(step: Quality = DEFAULT_QUALITY_STEP) =
    countByTraversableProjection(trySnp(_).flatMap(_.genotypes.map(g => quantize(step)(quality(g)))))

  def indelLengthDistribution() =
    countByTraversableProjection(vc => tryIndel(vc).map(vc => indelLength(vc.variant)))

  private def trySnp(vc: VariantContext) = Some(vc).filter(vc => isSnp(vc.variant)).toTraversable

  private def tryIndel(vc: VariantContext) = Some(vc).filter(vc => isIndel(vc.variant)).toTraversable

  def countByProjection[P: ClassTag](projection: VariantContext => P): Iterable[QcProjectionCount[P]] =
    rdd
      .mapValues(projection)
      .countByValue
      .map{ case ((label, p), count) => QcProjectionCount(label, p, count) }

  def countByTraversableProjection[P: ClassTag](projection: VariantContext => Traversable[P]): Iterable[QcProjectionCount[P]] =
    rdd
      .flatMapValues(projection(_))
      .countByValue
      .map{ case ((label, p), count) => QcProjectionCount(label, p, count) }

}
