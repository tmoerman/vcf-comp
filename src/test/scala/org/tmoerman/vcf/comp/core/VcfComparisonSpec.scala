package org.tmoerman.vcf.comp.core

import org.tmoerman.test.spark.BaseSparkContextSpec

import org.tmoerman.vcf.comp.VcfComparisonContext._
import org.tmoerman.vcf.comp.core.VcfComparison.CONCORDANT

/**
 * @author Thomas Moerman
 */
class VcfComparisonSpec extends BaseSparkContextSpec {

  val annotated = "src/test/resources/small.snpEff.vcf"

  val rdd = sc.startComparison(annotated, annotated).cache()

  "comparing the same VCF file" should "result in the correct amount of variants" in {
    rdd.count shouldBe 133
  }

  "comparing the same VCF file" should "result in concordant genotypes only" in {
    rdd.map(_._1).distinct().collect shouldBe Array(CONCORDANT)
  }

}
