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

  type ComparisonRow = (Iterable[AnnotatedGenotype], Iterable[AnnotatedGenotype])

  def categorize[K](A: Label, B: Label, unifyConcordant: Boolean, matchFunction: AnnotatedGenotype => K)
                   (row: ComparisonRow): Map[Category, Map[K, Iterable[AnnotatedGenotype]]] = {

    row match {

      case (genotypesA, Nil) => Map((A, UNIQUE) -> genotypesA.groupBy(matchFunction))
      case (Nil, genotypesB) => Map((B, UNIQUE) -> genotypesB.groupBy(matchFunction))

      case (genotypesA, genotypesB) =>
        val mA = genotypesA.groupBy(matchFunction)
        val mB = genotypesB.groupBy(matchFunction)

        lazy val concordant: Map[Category, Map[K, Iterable[AnnotatedGenotype]]] =
          (mA intersectWith mB) { case t => t }
            .toIterable
            .map { case (k, (gtA, gtB)) => ((k, gtA), (k, gtB)) }
            .unzip match {
            case (Nil, Nil) => Map() // unzip yields a tuple of Nils if the intersection is empty
            case (ccA, ccB) =>
              if (unifyConcordant)
                Map(("", CONCORDANT) -> ccA.toMap)
              else
                Map((A, CONCORDANT) -> ccA.toMap,
                    (B, CONCORDANT) -> ccB.toMap)
          }

        lazy val A_min_B = (mA -- mB.keys).mapKeys(withKey((A, DISCORDANT)))
        lazy val B_min_A = (mB -- mA.keys).mapKeys(withKey((B, DISCORDANT)))

        lazy val discordant: Map[Category, Map[K, Iterable[AnnotatedGenotype]]] =
          (A_min_B ++ B_min_A)
            .toIterable // mapping over an Iterable differs from mapping over a Map
            .map { case ((cat, k), genotypes) => Map(cat -> Map(k -> genotypes.toList)) } match {
              case Nil => Map()
              case list => list.reduce(_ |+| _)
            }

        concordant ++ discordant
    }
  }

  def compareSnps(params: SnpComparisonParams = new SnpComparisonParams())
                 (rddA: RDD[AnnotatedGenotype],
                  rddB: RDD[AnnotatedGenotype]): RDD[(Category, AnnotatedGenotype)] = params match {

    case SnpComparisonParams(matchOnSampleId, unify, matchFn, (labelA, labelB), (qA, qB), (rdA, rdB)) =>

      def prep(q: Quality, rd: ReadDepth, rdd: RDD[AnnotatedGenotype]): RDD[(VariantKey, AnnotatedGenotype)] =
        rdd
          .filter(isSnp)
          .filter(quality(_) >= q)
          .filter(readDepth(_) >= rd)
          .keyBy(variantKey(params.matchOnSampleId))

      val genotypesByCategory: RDD[Map[Category, Iterable[AnnotatedGenotype]]] =
        prep(qA, rdA, rddA).cogroup(prep(qB, rdB, rddB))
          .map(dropKey)                                            // coGroup:             (Iterable[AnnotatedGenotype], Iterable[AnnotatedGenotype])
          .map(categorize(A = labelA,
                          B = labelB,
                          unifyConcordant = unify,
                          matchFunction = matchFn))                // categorized:         Map[Category, Map[BaseChange, Iterable[AnnotatedGenotype]]]
          .map(_.mapValues(_.mapValues(_.maxBy(quality)).values))  // max Q by BaseChange: Map[Category, Iterable[AnnotatedGenotype]]

      genotypesByCategory.flatMap(flattenWithKey)
  }

}