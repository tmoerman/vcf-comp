package org.tmoerman.vcf.comp.core

import org.tmoerman.test.spark.BaseSparkContextSpec

import org.tmoerman.vcf.comp.VcfComparisonContext._
import org.tmoerman.vcf.comp.core.Model.SnpComparisonParams
import org.tmoerman.vcf.comp.core.SnpComparison.{CONCORDANT}

/**
 * @author Thomas Moerman
 */
class SnpComparisonSpec extends BaseSparkContextSpec {

  val annotated = "src/test/resources/small.snpEff.vcf"

  val params = new SnpComparisonParams()

  val rdd = sc.startSnpComparison(annotated, annotated, params).cache()

  "comparing the same VCF file" should "result in the correct amount of variants" in {
    rdd.count shouldBe 127 // not 133 because only SNPs are counted
  }

  "comparing the same VCF file" should "result in concordant genotypes only" in {
    rdd.flatMap(_.keys).distinct().collect shouldBe Array(CONCORDANT)
  }

}
