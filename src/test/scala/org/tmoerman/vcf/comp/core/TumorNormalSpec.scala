package org.tmoerman.vcf.comp.core

import java.io.File

import org.tmoerman.test.spark.BaseSparkContextSpec
import org.tmoerman.vcf.comp.VcfComparisonContext._
import org.tmoerman.vcf.comp.core.Model.{CategoryCount, _}
import org.tmoerman.vcf.comp.core.SnpComparison._
import org.tmoerman.vcf.comp.util.Victorinox._

/**
 * @author Thomas Moerman
 */
class TumorNormalSpec extends BaseSparkContextSpec {

  val wd = "/Users/tmo/Work/exascience/data/VCF-comp/tumor.normal/"
  val out = wd + "report.v3/"

  new File(out).mkdir()

  val tumor  = wd + "4146_T.vcf.gz.annotated.gz"
  val normal = wd + "4146_N.vcf.gz.annotated.gz"

  val params = new SnpComparisonParams(unifyConcordant = true,
                                       labels = ("TUMOR", "NORMAL"))

  val rdd = sc.startSnpComparison(tumor, normal, params).cache()

  "filtering by occurrence" should "succeed" in {
    val uniqueAndConcordant = rdd.viewOnly(CONCORDANT, UNIQUE).baseChangePatternCount
  }

  "snpCount" should "succeed" in {
    val snpCount = rdd.categoryCount
  }

  "base change count" should "succeed" in {
    rdd.baseChangeCount
  }

  "base change pattern count" should "succeed" in {
    rdd.baseChangePatternCount
  }

  "read depth distribution" should "succeed" in {
    rdd.readDepthDistribution()
  }

  "quality distribution" should "succeed" in {
    rdd.qualityDistribution()
  }

  "allele freq distribution" should "succeed" in {
    rdd.alleleFrequencyDistribution()
  }

  "clinvar ratio" should "succeed" in {
    rdd.clinvarRatio()
  }

  "synonymous ratio" should "succeed" in {
    rdd.synonymousRatio()
  }

  "common SNP ratio" should "succeed" in {
    rdd.commonSnpRatio(Map(true -> "common SNP", false -> "not common SNP"))
  }

  "functional impact count" should "succeed" in {
    rdd.functionalImpactCount
  }

  "transcript biotype count" should "succeed" in {
    rdd.transcriptBiotypeCount
  }

  val tumorQCparams = new VcfQCParams(label = "TUMOR")

  val tumorQCrdd = sc.startQC(tumor, tumorQCparams)

  "QC variant types" should "succeed" in {
    val variantTypes = tumorQCrdd.variantTypeCount

    val multiAllelicRatio = tumorQCrdd.multiAllelicRatio()

    println(variantTypes, multiAllelicRatio)
  }



//  "exporting the entire data set" should "succeed" in {
//    val headers = List(
//      "sample_id",
//      "contig",
//      "start",
//      "category",
//      "base_change",
//      "base_change_pattern",
//      "read_depth",
//      "quality",
//      "allele_freq",
//      "Clinvar_annotated",
//      "DBSNP_annotated").mkString("\t") + "\n"
//
//    val labels = Map(
//      A_ONLY -> "tumor",
//      B_ONLY -> "normal")
//
//    val lines = rdd.filter{ case (_, rep) => isSnp(rep) }.enrich(labels).toLocalIterator
//
//    val file = new File(out + "dump.txt")
//    val bw = new BufferedWriter(new FileWriter(file))
//
//    bw.write(headers)
//    lines.foreach(line => bw.write(line))
//
//    bw.close()
//  }


}
