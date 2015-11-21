package org.tmoerman.vcf.comp.core

import java.io.File

import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.test.spark.BaseSparkContextSpec
import org.tmoerman.vcf.comp.core.Model.SnpComparisonParams
import org.tmoerman.vcf.comp.VcfComparisonContext._
import org.tmoerman.vcf.comp.core.Model._

/**
 * @author Thomas Moerman
 */
class TumorNormalMatchingSpec extends BaseSparkContextSpec{

  val wd = "/Users/tmo/Work/exascience/data/VCF-comp/tumor.normal/"

  val tumor  = wd + "4146_T.vcf.gz.annotated.gz"
  val normal = wd + "4146_N.vcf.gz.annotated.gz"

  val m2 = (g: AnnotatedGenotype) => genotypeAlleles(g)
  val m3 = (g: AnnotatedGenotype) => (g.getGenotype.getIsPhased, genotypeAlleles(g))

  val p2 = new SnpComparisonParams(labels = ("TUMOR", "NORMAL"), matchFunction = m2)
  val p3 = new SnpComparisonParams(labels = ("TUMOR", "NORMAL"), matchFunction = m3)

  val rdd2 = sc.startSnpComparison(tumor, normal, p2).cache()
  val rdd3 = sc.startSnpComparison(tumor, normal, p3).cache()

  "bla" should "bla" in {
    println(rdd2.categoryCount)

    println(rdd3.categoryCount)
  }

}
