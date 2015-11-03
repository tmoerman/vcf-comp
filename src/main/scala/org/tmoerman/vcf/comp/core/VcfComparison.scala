package org.tmoerman.vcf.comp.core

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.vcf.comp.core.Model._

import org.tmoerman.vcf.comp.util.Victorinox._
import scalaz._
import Scalaz._

/**
 * @author Thomas Moerman
 */
object VcfComparison extends Serializable with Logging {

  type Sample = String
  type Contig = String
  type Start  = Long
  
  type VariantKey = (Option[Sample], Contig, Start)

  def variantKey(matchOnSampleId: Boolean = false)
                (annotatedGenotype: AnnotatedGenotype): VariantKey = {

    val genotype = annotatedGenotype.getGenotype
    val variant  = genotype.getVariant

    (if (matchOnSampleId) Some(genotype.getSampleId) else None,
     variant.getContig.getContigName,
     variant.getStart)
  }

  type Category = String

  val A_ONLY     = "A-ONLY"
  val B_ONLY     = "B-ONLY"
  val CONCORDANT = "CONCORDANT"
  val DISCORDANT = "DISCORDANT"

  type Label = String
  type Labels = Map[Category, Label]

  // TODO forget labels:

  type ComparisonRow = (Iterable[AnnotatedGenotype], Iterable[AnnotatedGenotype])

  def categorizedDelegates(labels: Labels)(row: ComparisonRow): Iterable[(Category, AnnotatedGenotype)] = {

    def labeledDelegates(category: Category)
                        (genotypesByBaseChange: Map[BaseChange, Iterable[AnnotatedGenotype]]): Iterable[(Label, AnnotatedGenotype)] = {

      val label = labels.getOrElse(category, category)

      // entanglement going on here...
      genotypesByBaseChange           // Map[BaseChange, Iterable[AnnotatedGenotype]]
        .mapValues(_.maxBy(quality))  // Map[BaseChange, AnnotatedGenotyope]  -- select rep with max Quality
        .values                       // Iterable[AnnotatedGenotype]          -- ignore base changes
        .map(withKey(label))          // Iterable[(Label, AnnotatedGenotype)] -- add category labels
    }

    val temp: Map[Category, Map[BaseChange, Iterable[AnnotatedGenotype]]] = categorize(row)

    ???
  }

  def categorize(row: ComparisonRow): Map[Category, Map[BaseChange, Iterable[AnnotatedGenotype]]] = {
    row match {
      case (Nil, Nil) => throw new scala.Exception("impossible")

      case (a, Nil) => Map(A_ONLY -> a.groupBy(baseChange))

      case (Nil, b) => Map(B_ONLY -> b.groupBy(baseChange))

      case (a, b) =>
        val A = a.groupBy(baseChange)
        val B = b.groupBy(baseChange)

        val intersection = A.intersectWith(B)(_ ++ _)
        val symmetricDiff = (A -- B.keys).unionWith(B -- A.keys)(_ ++ _)

        Map(CONCORDANT -> intersection,
            DISCORDANT -> symmetricDiff)
    }
  }

  case class VcfComparisonParams(matchOnSampleId: Boolean = false,
                                 categoryLabels:  Labels  = Map(),
                                 qualityThresholds:   (Quality, Quality)     = (0, 0),
                                 readDepthThresholds: (ReadDepth, ReadDepth) = (1, 1))

  def compare(params: VcfComparisonParams = new VcfComparisonParams())
             (rddA: RDD[AnnotatedGenotype],
              rddB: RDD[AnnotatedGenotype]): RDD[Iterable[(Category, AnnotatedGenotype)]] = {

    def prep(rdd: RDD[AnnotatedGenotype]) =
      rdd
        .filter(isSnp)
        .keyBy(variantKey(params.matchOnSampleId))

    prep(rddA).cogroup(prep(rddB))
      .map(dropKey)
      .map(categorizedDelegates(params.categoryLabels))
  }

//  def countByCategory[T](projection: Iterable[(Category, AnnotatedGenotype)] => T, snpOnly: Boolean = true)
//                        (rdd: RDD[Iterable[(Category, AnnotatedGenotype)]]): Map[(Category, T), Count] = {
//
//  }
  
  def countByCategory[T](projection: AnnotatedGenotype => T)
                        (rdd: RDD[Iterable[(Category, AnnotatedGenotype)]]): Map[(Category, T), Count] =
    rdd
      .flatMap(identity)
      .mapValues(projection)
      .countByValue
      .toMap


}