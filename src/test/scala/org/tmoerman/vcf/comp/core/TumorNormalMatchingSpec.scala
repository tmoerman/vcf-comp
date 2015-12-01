package org.tmoerman.vcf.comp.core

import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.test.spark.BaseSparkContextSpec
import org.tmoerman.vcf.comp.core.Model.ComparisonParams
import org.tmoerman.vcf.comp.VcfComparisonContext._
import org.tmoerman.vcf.comp.core.Model._
import org.bdgenomics.adam.rdd.ADAMContext._

/**
 * @author Thomas Moerman
 */
class TumorNormalMatchingSpec extends BaseSparkContextSpec{

  val wd = "/Users/tmo/Work/exascience/data/VCF-comp/tumor.normal/"

  "loading annotated VCFs" should "work" in {
    val tumor  = wd + "4146_T.vcf.gz.annotated.gz"
    val normal = wd + "4146_N.vcf.gz.annotated.gz"

    val m2 = (g: AnnotatedGenotype) => genotypeAlleles(g)
    val m3 = (g: AnnotatedGenotype) => (g.getGenotype.getIsPhased, genotypeAlleles(g))

    val p2 = new ComparisonParams(labels = ("TUMOR", "NORMAL"), matchFunction = m2)
    val p3 = new ComparisonParams(labels = ("TUMOR", "NORMAL"), matchFunction = m3)

    val rdd2 = sc.startSnpComparison(tumor, normal, p2).cache()
    val rdd3 = sc.startSnpComparison(tumor, normal, p3).cache()

    println(rdd2.categoryCount)

    println(rdd3.categoryCount)
  }

  "loading non-annotated VCFs" should "work" ignore {
    val tumor  = wd + "4146_T.vcf.gz"
    val normal = wd + "4146_N.vcf.gz"

    sc.loadVcf(tumor, None).take(10)
  }

}
