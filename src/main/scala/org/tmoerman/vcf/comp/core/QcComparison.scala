package org.tmoerman.vcf.comp.core

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.tmoerman.vcf.comp.core.Model._

/**
 * @author Thomas Moerman
 */
object QcComparison extends Serializable {

  def meta(rdd: RDD[String]): Map[String, List[String]] =
    rdd
      .toLocalIterator
      .toList
      .takeWhile(line => line.startsWith("##"))
      .map(_.drop(2).split("=", 2) match { case Array(l, r, _*) => (l, r) })
      .groupBy(_._1)
      .mapValues(_.map(_._2))

  def qcComparison(params: ComparisonParams = new ComparisonParams())
                  (rddA: RDD[VariantContext],
                   rddB: RDD[VariantContext]): RDD[(Label, VariantContext)] = {

    val (labelA, labelB) = params.labels
    val (qA, qB)         = params.qualities
    val (rdA, rdB)       = params.readDepths

    def prep(q: Quality, rd: ReadDepth, rdd: RDD[VariantContext]) =
      rdd
        .filter(_.genotypes.forall(quality(_) >= q))
        .filter(_.genotypes.forall(readDepth(_) >= q))

    prep(qA, rdA, rddA).keyBy(_ => labelA) ++
    prep(qB, rdB, rddB).keyBy(_ => labelB)
  }

}