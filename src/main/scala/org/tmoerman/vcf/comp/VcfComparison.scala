package org.tmoerman.vcf.comp

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype

import Model._

import scala.reflect.ClassTag
import Function.tupled

/**
 * @author Thomas Moerman
 */
object VcfComparison extends Serializable with Logging {

  type Category = String

  val LEFT_UNIQUE  = "LEFT-UNIQUE"
  val RIGHT_UNIQUE = "RIGHT-UNIQUE"
  val CONCORDANT   = "CONCORDANT"

  type ComparisonRow = (Option[AnnotatedGenotype], Option[AnnotatedGenotype])

  def variantKey(annotatedGenotype: AnnotatedGenotype): VariantKey = {
    val genotype = annotatedGenotype.getGenotype
    val variant  = genotype.getVariant

    (genotype.getSampleId,
     variant.getContig.getContigName,
     variant.getStart,
     variant.getReferenceAllele,
     variant.getAlternateAllele)
  }

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

  implicit def pimpComparisonRDD(rdd: RDD[ComparisonRow]): ComparisonRDDFunctions = new ComparisonRDDFunctions(rdd)

  class ComparisonRDDFunctions(rdd: RDD[ComparisonRow]) extends Serializable {

    type Labels = Map[Category, String]

    private def snp(cat: Category, row: ComparisonRow) = toRepresenter(cat, row).map(_.getGenotype.getVariant).map(isSNP).get

    def toCategory(row: ComparisonRow): Category = {
      row match {
        case (Some(_), Some(_)) => CONCORDANT
        case (Some(_), None) => LEFT_UNIQUE
        case (None, Some(_)) => RIGHT_UNIQUE
        case (None, None) => throw new Exception("glitch in the matrix")
      }
    }

    def toRepresenter(category: Category, row: ComparisonRow): Option[AnnotatedGenotype] =
      category match {
        case CONCORDANT   => row._1
        case LEFT_UNIQUE  => row._1
        case RIGHT_UNIQUE => row._2
      }

    private val _SNP = tupled(snp _)

    def countVariantTypes(implicit labels: Labels): Map[(Category, VariantType), Count] =
      rdd
        .keyBy(toCategory)
        .map { case (cat, row) => (cat, toRepresenter(cat, row).map(_.getGenotype.getVariant).map(toVariantType).get) }
        .countByValue
        .toMap

    def countSNPs(implicit labels: Labels): Map[Category, Count] =
      rdd
        .keyBy(toCategory)
        .filter(_SNP)
        .map(_._1)
        .countByValue
        .toMap

    def countBaseChangesPatterns: Map[(Category, BaseChangePattern), Count] =
      rdd
        .keyBy(toCategory)
        .filter(_SNP)
        .map { case (cat, row) => (cat, toRepresenter(cat, row).map(_.getGenotype.getVariant).map(toBaseChange).map(toBaseChangePattern).get) }
        .countByValue
        .toMap

  }

}