package org.tmoerman.vcf.comp.core

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
object SnpComparison extends Serializable {

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
      case (_, CONCORDANT)     => CONCORDANT
      case (label, occurrence) => s"$label-$occurrence"
    }

  // COMPARISON

  type CoGroupRow[G] = (Iterable[G], Iterable[G])

  type OccurrenceRow[G] = Map[Occurrence, Map[Label, Map[Any, G]]]

  // generic types for testability
  def groupByOccurrence[G](A: Label,
                           B: Label,
                           matchFunction: G => Any,
                           selectFunction: Iterable[G] => G)
                          (row: CoGroupRow[G]): OccurrenceRow[G] =
    row match {

      case (genotypesA, Nil) => Map(UNIQUE -> Map(A -> genotypesA.groupBy(matchFunction).mapValues(selectFunction)))
      case (Nil, genotypesB) => Map(UNIQUE -> Map(B -> genotypesB.groupBy(matchFunction).mapValues(selectFunction)))

      case (genotypesA, genotypesB) =>
        val mA = genotypesA.groupBy(matchFunction).mapValues(selectFunction)
        val mB = genotypesB.groupBy(matchFunction).mapValues(selectFunction)

        val concordant: OccurrenceRow[G] =
          (mA intersectWith mB){ case t => t }
            .toIterable
            .map { case (k, (gtA, gtB)) => ((k, gtA), (k, gtB)) }
            .unzip match {
              case (Nil, Nil) => Map() // unzip yields a tuple of Nils if the intersection is empty
              case (ccA, ccB) => Map(CONCORDANT -> ListMap(A -> ccA.toMap, B -> ccB.toMap)) // ListMap to preserve A->B order
            }

        val AminusB = mA -- mB.keys
        val BminusA = mB -- mA.keys

        val discordant: OccurrenceRow[G] =
          (AminusB.toIterable ++ BminusA.toIterable) match {
            case Nil => Map()
            case _   => Map(DISCORDANT -> ListMap(A -> AminusB, B -> BminusA)) // ListMap to preserve A->B order
          }

        concordant ++ discordant
    }



  // we want the least amount of closed over parameters as possible in this function
  def flattenToReps[G](unifyConcordant: Boolean = true)
                      (row: OccurrenceRow[G]): Iterable[(Category, G)] =
    for { (occ, labeled)   <- row
          unified = (occ, unifyConcordant) match {
                      case (CONCORDANT, true) => labeled.take(1)
                      case _                  => labeled
                    }
          (label, matched) <- unified
          (k, genotype)    <- matched } yield ((label, occ), genotype)


  // tying all together
  def snpComparison(params: ComparisonParams = ComparisonParams())
                   (rddA: RDD[AnnotatedGenotype],
                    rddB: RDD[AnnotatedGenotype]): RDD[OccurrenceRow[AnnotatedGenotype]] = {

    val (labelA, labelB) = params.labels
    val (qA, qB)         = params.qualities
    val (rdA, rdB)       = params.readDepths

    def prep(q: Quality, rd: ReadDepth, rdd: RDD[AnnotatedGenotype]): RDD[(VariantKey, AnnotatedGenotype)] =
      rdd
        .filter(isSnp)
        .filter(quality(_) >= q)
        .filter(readDepth(_) >= rd)
        .keyBy(variantKey(params.matchOnSampleId))

    prep(qA, rdA, rddA).cogroup(prep(qB, rdB, rddB))
      .map(dropKey)
      .map(groupByOccurrence(labelA, labelB, params.matchFunction, params.selectFunction))
  }

}