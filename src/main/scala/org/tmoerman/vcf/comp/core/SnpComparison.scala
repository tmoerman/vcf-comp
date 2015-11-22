package org.tmoerman.vcf.comp.core

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.vcf.comp.core.Model._

import org.tmoerman.vcf.comp.util.Victorinox._
import scala.collection.immutable.ListMap
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

  type Occurrence = String

  val UNIQUE     = "unique"
  val CONCORDANT = "concordant"
  val DISCORDANT = "discordant"

  type Category = (Label, Occurrence)

  def name(cat: Category) =
    cat match {
      case ("", CONCORDANT)    => CONCORDANT
      case (label, occurrence) => s"$label-$occurrence"
    }

  // COMPARISON

  type CoGroupRow[G] = (Iterable[G], Iterable[G])

  type OccurrenceRow[G, K] = Map[Occurrence, Map[Label, Map[K, Iterable[G]]]]


  // generic types for testability
  def groupByOccurrence[G, K](A: Label, B: Label, matchFunction: G => K)
                             (row: CoGroupRow[G]): OccurrenceRow[G, K] =
    row match {

      case (genotypesA, Nil) => Map(UNIQUE -> Map(A -> genotypesA.groupBy(matchFunction)))
      case (Nil, genotypesB) => Map(UNIQUE -> Map(B -> genotypesB.groupBy(matchFunction)))

      case (genotypesA, genotypesB) =>
        val mA = genotypesA.groupBy(matchFunction)
        val mB = genotypesB.groupBy(matchFunction)

        val concordant: OccurrenceRow[G, K] =
          (mA intersectWith mB){ case t => t }
            .toIterable
            .map { case (k, (gtA, gtB)) => ((k, gtA), (k, gtB)) }
            .unzip match {
              case (Nil, Nil) => Map() // unzip yields a tuple of Nils if the intersection is empty
              case (ccA, ccB) => Map(CONCORDANT -> ListMap(A -> ccA.toMap, B -> ccB.toMap)) // ListMap to preserve A->B order
            }

        val AminusB = mA -- mB.keys
        val BminusA = mB -- mA.keys

        val discordant: OccurrenceRow[G, K] =
          (AminusB.toIterable ++ BminusA.toIterable) match {
            case Nil => Map()
            case _   => Map(DISCORDANT -> ListMap(A -> AminusB, B -> BminusA)) // ListMap to preserve A->B order
          }

        concordant ++ discordant
    }


  // we want the least amount of closed over parameters as possible in this function
  def flattenToReps(unifyConcordant: Boolean = true)
                   (row: OccurrenceRow[AnnotatedGenotype, Any]): Iterable[(Category, AnnotatedGenotype)] =
    for { (occ, labeled)   <- row
          unified = (occ, unifyConcordant) match {
                      case (CONCORDANT, true) => labeled.take(1)
                      case _                  => labeled
                    }
          (label, matched) <- unified
          (k, genotypes)   <- matched } yield ((label, occ), genotypes.maxBy(quality))


  // tying all together
  def snpComparison(params: SnpComparisonParams)
                   (rddA: RDD[AnnotatedGenotype],
                    rddB: RDD[AnnotatedGenotype]): RDD[OccurrenceRow[AnnotatedGenotype, Any]] = params match {

    case SnpComparisonParams(matchOnSampleId, matchFunction, (labelA, labelB), (qA, qB), (rdA, rdB)) =>

      def prep(q: Quality, rd: ReadDepth, rdd: RDD[AnnotatedGenotype]): RDD[(VariantKey, AnnotatedGenotype)] =
        rdd
          .filter(isSnp)
          .filter(quality(_) >= q)
          .filter(readDepth(_) >= rd)
          .keyBy(variantKey(params.matchOnSampleId))

      prep(qA, rdA, rddA).cogroup(prep(qB, rdB, rddB))
        .map(dropKey)
        .map(groupByOccurrence(labelA, labelB, matchFunction))
  }

}