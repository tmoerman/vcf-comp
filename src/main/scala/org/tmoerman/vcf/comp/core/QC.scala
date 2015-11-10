package org.tmoerman.vcf.comp.core

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.formats.avro.{Genotype, Variant}
import org.tmoerman.vcf.comp.core.Model._
import org.bdgenomics.adam.rdd.ADAMContext._

/**
 * @author Thomas Moerman
 */
object QC {

  def meta(rdd: RDD[String]): Map[String, List[String]] =
    rdd
      .toLocalIterator
      .toList
      .takeWhile(line => line.startsWith("##"))
      .map(_.drop(2).split("=", 2) match { case Array(l, r, _*) => (l, r) })
      .groupBy(_._1)
      .mapValues(_.map(_._2))

  def prep(params: VcfQCParams = new VcfQCParams())
          (rdd: RDD[Genotype]): RDD[VariantContext] = params match {

    case VcfQCParams(label, q, rd) =>
      rdd
        .filter(quality(_) >= q)
        .filter(readDepth(_) >= rd)
        .toVariantContext
  }

}