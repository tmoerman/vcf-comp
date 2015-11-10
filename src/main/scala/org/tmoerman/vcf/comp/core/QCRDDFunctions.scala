package org.tmoerman.vcf.comp.core

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.tmoerman.vcf.comp.core.Model.{Count, VariantType}
import Model._

class QCRDDFunctions(val rdd: RDD[VariantContext]) extends Serializable with Logging {

  def variantTypeCount = countByProjection(v => variantType(v.variant.variant))

  def qualityDistribution = countByProjections(_.genotypes.map(quality))

  def readDepthDistribution = countByProjections(_.genotypes.map(readDepth))

  

  def countByProjections[P](projection: VariantContext => Iterable[P]): Iterable[ProjectionCount[P]] =
    rdd
      .flatMap(projection)
      .toProjectionCount

  def countByProjection[P](projection: VariantContext => P): Iterable[ProjectionCount[P]] =
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


  //def qualityDistribution: Map[]

  //  def multiAllelicRegionCount(variants: RDD[Variant]) = ??? // TODO implement

}
