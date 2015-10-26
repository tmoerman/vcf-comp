package org.tmoerman.vcf.comp

import org.tmoerman.test.spark.BaseSparkContextSpec

import VcfComparisonContext._

/**
 * @author Thomas Moerman
 */
class MetaFieldsSpec extends BaseSparkContextSpec {

  val small = "src/test/resources/small.vcf"

  "small VCF meta fields" should "include multiple INFO fields" in {
    sc.getMetaFields(small)("INFO").size should be > 10
  }

  "small VCF meta fields" should "not include SnpEff meta information" in {
    sc.getMetaFields(small).get("SnpEffCmd") shouldBe empty
  }

  val annotated = "src/test/resources/small.snpEff.vcf"

  "annotated VCF meta fields" should "include SnpEff meta information" in {
    sc.getMetaFields(annotated).get("SnpEffCmd") shouldBe defined
  }

}
