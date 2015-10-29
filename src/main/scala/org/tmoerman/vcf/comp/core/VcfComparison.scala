package org.tmoerman.vcf.comp.core

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.model.RichAnnotatedGenotype
import org.tmoerman.vcf.comp.core.Model._

import org.tmoerman.vcf.comp.util.Victorinox._
import org.tmoerman.adam.fx.snpeff.model.RichAnnotated._

/**
 * @author Thomas Moerman
 */
object VcfComparison extends Serializable with Logging {

  def compare(labels: Option[Labels] = None)
             (rddA: RDD[AnnotatedGenotype],
              rddB: RDD[AnnotatedGenotype]): RDD[(Category, AnnotatedGenotype)] = {

    def prep(rdd: RDD[AnnotatedGenotype]) : RDD[(VariantKey, AnnotatedGenotype)] =
      rdd
        .keyBy(variantKey)
        .reduceByKey(withMax(quality))

    prep(rddA).cogroup(prep(rddB))
      .map(dropKey)
      .map{ case (a, b) => (a.headOption, b.headOption) }
      .map(catRep(labels))
  }

  protected def maybeSnp(snpOnly: Boolean)(rep: AnnotatedGenotype): Boolean = if (snpOnly) isSnp(rep) else true

  def countByCategory[T](snpOnly: Boolean)
                        (f: AnnotatedGenotype => T)
                        (rdd: RDD[(Category, AnnotatedGenotype)]): Map[(Category, T), Count] =
    rdd
      .filter{ case (_, rep) => maybeSnp(snpOnly)(rep) }
      .mapValues(f)
      .countByValue
      .toMap

}