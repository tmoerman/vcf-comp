package org.tmoerman.vcf.comp.core

import org.tmoerman.test.spark.BaseSparkContextSpec

import org.tmoerman.vcf.comp.VcfComparisonContext._
import Model._

import org.tmoerman.vcf.comp.util.Victorinox._

/**
 * @author Thomas Moerman
 */
class TumorNormalSpec extends BaseSparkContextSpec {

  val wd = "/Users/tmo/Work/exascience/data/VCF-comp/tumor.normal/"
  val out = wd + "report/"

  val tumor  = wd + "4146_T.vcf.gz.annotated.gz"
  val normal = wd + "4146_N.vcf.gz.annotated.gz"

  implicit val labels = Map(A_ONLY -> "TUMOR-only",
                            B_ONLY -> "NORMAL-only")

  val rdd = sc.startComparison(tumor, normal).cache()

  "export snpCount" should "succeed" in {
    val snpCount = rdd.snpCount
    val s = s"category\tcount\n" + snpCount.map{ case (cat, count) => s"$cat\t$count" }.mkString("\n")

    write(out + "snpCount.txt", s)
  }

  "export variant types" should "succeed" in {
    val s = toCSV(List("category", "variant type", "count"), rdd.variantTypeCount)
    write(out + "variantTypeCount.txt", s)
  }

  "export base change count " should "succeed" in {
    val s = toCSV(List("category", "base change", "count"), rdd.snpBaseChangeCount)
    write(out + "snpBaseChangeCount.txt", s)
  }

  "export base change pattern" should "succeed" in {
    val s = toCSV(List("category", "base change pattern", "count"), rdd.snpBaseChangesPatternCount)
    write(out + "snpBaseChangePatternCount.txt", s)
  }

  "export read depth distribution" should "succeed" in {
    val s = toCSV(List("category", "read depth", "count"), rdd.readDepthDistribution(snpOnly = true))
    write(out + "snpReadDepthDistribution.txt", s)
  }

  "export quality distribution" should "succeed" in {
    val s = toCSV(List("category", "quality", "count"), rdd.qualityDistribution(snpOnly = true, bin = roundToDecimals(1)))
    write(out + "snpQualityDistribution.txt", s)
  }

  "export allele freq distribution" should "succeed" in {
    val s = toCSV(List("category", "allele freq", "count"), rdd.alleleFrequencyDistribution(snpOnly = true, bin = roundToDecimals(1)))
    write(out + "snpAlleleFrequencyDistribution.txt", s)
  }

  "export clinvar ratio" should "succeed" in {
    val s = toCSV(List("category", "clinvar?", "count"), rdd.clinvarRatio(snpOnly = true))
    write(out + "snpClinvarRatio.txt", s)
  }

  "export dbSNP ratio" should "succeed" in {
    val s = toCSV(List("category", "dbSNP?", "count"), rdd.dbSnpRatio(snpOnly = true))
    write(out + "snpDbSNPRatio.txt", s)
  }

//  "export snpEff ratio" should "succeed" in {
//    val s = toCSV(List("category", "snpEff?", "count"), rdd.snpEffRatio(snpOnly = true))
//    write(out + "snpSnpEffRatio.txt", s)
//  }

}
