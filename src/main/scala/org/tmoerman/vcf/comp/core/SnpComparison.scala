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
object SnpComparison extends Serializable with Logging {

  // VARIANT KEY

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

  // CATEGORY

  type Label = String

  type Occurrence = String

  val UNIQUE     = "unique"
  val CONCORDANT = "concordant"
  val DISCORDANT = "discordant"

  type Category = (Label, Occurrence)

  def name(cat: Category) = cat match { case (label, occurrence) => s"$label-$occurrence" }

  // COMPARISON

  type ComparisonRow = (Iterable[AnnotatedGenotype], Iterable[AnnotatedGenotype])

  def categorize(A: Label, B: Label)(row: ComparisonRow): Map[Category, Map[BaseChange, Iterable[AnnotatedGenotype]]] =
    row match {

      case (genotypesA, Nil) => Map((A, UNIQUE) -> genotypesA.groupBy(baseChange))
      case (Nil, genotypesB) => Map((B, UNIQUE) -> genotypesB.groupBy(baseChange))

      case (genotypesA, genotypesB) =>
        val mA = genotypesA.groupBy(baseChange)
        val mB = genotypesB.groupBy(baseChange)

        val concordant: Map[Category, Map[BaseChange, Iterable[AnnotatedGenotype]]] =
          (mA intersectWith mB) { case t => t }
            .toIterable
            .map { case (baseChange, (gtA, gtB)) => ((baseChange, gtA), (baseChange, gtB)) }
            .unzip match {
              case (concordantA, concordantB) => Map((A, CONCORDANT) -> concordantA.toMap,
                                                     (B, CONCORDANT) -> concordantB.toMap) }

        val A_min_B = (mA -- mB.keys).mapKeys(withKey((A, DISCORDANT)))
        val B_min_A = (mB -- mA.keys).mapKeys(withKey((B, DISCORDANT)))

        val discordant: Map[Category, Map[BaseChange, Iterable[AnnotatedGenotype]]] =
          (A_min_B ++ B_min_A)
            .toIterable // mapping over an Iterable differs from mapping over a Map
            .map { case ((cat, baseChange), genotypes) => Map(cat -> Map(baseChange -> genotypes.toList)) }
            .reduce(_ |+| _)

        concordant ++ discordant
    }

  case class VcfComparisonParams(matchOnSampleId: Boolean           = false,
                                 labels:     (Label, Label)         = ("A", "B"),
                                 qualities:  (Quality, Quality)     = (0, 0),
                                 readDepths: (ReadDepth, ReadDepth) = (1, 1))

  def compareSnps(params: VcfComparisonParams = new VcfComparisonParams())
                 (rddA: RDD[AnnotatedGenotype],
                  rddB: RDD[AnnotatedGenotype]): RDD[Map[Category, Iterable[AnnotatedGenotype]]] = params match {

    case VcfComparisonParams(matchOnSampleId, (labelA, labelB), (qA, qB), (rdA, rdB)) =>

      def prep(q: Quality, rd: ReadDepth, rdd: RDD[AnnotatedGenotype]) =
        rdd
          .filter(isSnp)
          .filter(quality(_) >= q)
          .filter(readDepth(_) >= rd)
          .keyBy(variantKey(params.matchOnSampleId))

      prep(qA, rdA, rddA).cogroup(prep(qB, rdB, rddB))
        .map(dropKey)                                              // coGroup:             (Iterable[AnnotatedGenotype], Iterable[AnnotatedGenotype])
        .map(categorize(A = labelA, B = labelB))                   // categorized:         Map[Category, Map[BaseChange, Iterable[AnnotatedGenotype]]]
        .map(_.mapValues(_.mapValues(_.maxBy(quality)).values))    // max Q by BaseChange: Map[Category, Iterable[AnnotatedGenotype]]
  }

}