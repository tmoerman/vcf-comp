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

  type Labels = Map[Category, String]

  type ComparisonRow = (Iterable[AnnotatedGenotype], Iterable[AnnotatedGenotype])

  def categorizedDelegates(labels: Labels)(row: ComparisonRow): Iterable[(Category, AnnotatedGenotype)] = {

    def labeledDelegates(category: Category)
                        (genotypesByBaseChange: Map[BaseChange, Iterable[AnnotatedGenotype]]): Iterable[(Category, AnnotatedGenotype)] =
      genotypesByBaseChange.mapValues(_.maxBy(quality)).values.map(withKey(labels.getOrElse(category, category)))
    
    row match {
      case (Nil, Nil) => throw new Exception("impossible")

      case (a, Nil) => labeledDelegates(A_ONLY)(a.groupBy(baseChange))

      case (Nil, b) => labeledDelegates(B_ONLY)(b.groupBy(baseChange))

      case (a, b) =>
        val A = a.groupBy(baseChange)
        val B = b.groupBy(baseChange)

        val intersection  = A.intersectWith(B)(_ ++ _)
        val symmetricDiff = (A -- B.keys).unionWith(B -- A.keys)(_ ++ _)

        labeledDelegates(CONCORDANT)(intersection) ++
        labeledDelegates(DISCORDANT)(symmetricDiff)
    }
  }

  case class VcfComparisonParams(matchOnSampleId: Boolean   = false,
                                 labels:          Labels    = Map()
//                                 quality:         Quality   = null,
//                                 readDepth:       ReadDepth = null
                                  )

  val DEFAULT_PARAMS = new VcfComparisonParams()

  def compare(params: VcfComparisonParams = DEFAULT_PARAMS)
             (rddA: RDD[AnnotatedGenotype],
              rddB: RDD[AnnotatedGenotype]): RDD[(Category, AnnotatedGenotype)] = {

    def keyed(rdd: RDD[AnnotatedGenotype]) = rdd.keyBy(variantKey(params.matchOnSampleId))

    keyed(rddA).cogroup(keyed(rddB))
      .map(dropKey)
      .flatMap(categorizedDelegates(params.labels))
  }

  def countByCategory[T](snpOnly: Boolean)
                        (projection: AnnotatedGenotype => T)
                        (rdd: RDD[(Category, AnnotatedGenotype)]): Map[(Category, T), Count] = {

    def maybeSnp(snpOnly: Boolean)(representative: AnnotatedGenotype) = if (snpOnly) isSnp(representative) else true

    rdd
      .filter{ case (_, rep) => maybeSnp(snpOnly)(rep) }
      .mapValues(projection)
      .countByValue
      .toMap
  }


}