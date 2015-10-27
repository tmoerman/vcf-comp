package org.tmoerman.vcf.comp.core

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.vcf.comp.core.Model._

import scala.Function._
import scala.reflect.ClassTag

/**
 * @author Thomas Moerman
 */
object VcfComparison extends Serializable with Logging {

  // TODO make Float a generic numeric type
  def withMax[A, N](f: A => Float)(l: A, r: A): A = if (f(l) > f(r)) l else r

  def dropKey[T: ClassTag]: ((_, T)) => T = _._2

  def compare(rddA: RDD[AnnotatedGenotype],
              rddB: RDD[AnnotatedGenotype]): RDD[ComparisonRow] = {

    def prep(rdd: RDD[AnnotatedGenotype]) : RDD[(VariantKey, AnnotatedGenotype)] =
      rdd
        .keyBy(variantKey)
        .reduceByKey(withMax(_.getGenotype.getVariantCallingAnnotations.getVariantCallErrorProbability))

    prep(rddA).cogroup(prep(rddB))
      .map(dropKey)
      .map{ case (a, b) => (a.headOption, b.headOption) }
  }

  private def isSNP = tupled(isSnp _)

  def countVariantTypes(rdd: RDD[ComparisonRow]): Map[(Category, VariantType), Count] =
    rdd
      .keyBy(toCategory)
      .map { case (cat, row) => (cat, toRepresenter(cat, row).map(_.getGenotype.getVariant).map(toVariantType).get) }
      .countByValue
      .toMap

  def countSNPs(rdd: RDD[ComparisonRow]): Map[Category, Count] =
    rdd
      .keyBy(toCategory)
      .filter(isSNP)
      .map(_._1)
      .countByValue
      .toMap

  def countBaseChangesPatterns(rdd: RDD[ComparisonRow]): Map[(Category, BaseChangePattern), Count] =
    rdd
      .keyBy(toCategory)
      .filter(isSNP)
      .map { case (cat, row) => (cat, toRepresenter(cat, row).map(_.getGenotype.getVariant).map(toBaseChange).map(toBaseChangePattern).get) }
      .countByValue
      .toMap

}