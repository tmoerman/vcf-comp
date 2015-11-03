package org.tmoerman.vcf.comp.core

import java.io.{FileWriter, BufferedWriter, File}

import org.tmoerman.test.spark.BaseSparkContextSpec

import org.tmoerman.vcf.comp.VcfComparisonContext._
import VcfComparison._
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

//  "export snpCount" should "succeed" in {
//    val snpCount = rdd.snpCount
//    val s = s"category\tcount\n" + snpCount.map{ case (cat, count) => s"$cat\t$count" }.mkString("\n")
//
//    write(out + "snpCount.txt", s)
//  }
//
//  "export base change count " should "succeed" in {
//    val s = toCSV(List("category", "base change", "count"), rdd.snpBaseChangeCount)
//    write(out + "snpBaseChangeCount.txt", s)
//  }
//
//  "export base change pattern" should "succeed" in {
//    val s = toCSV(List("category", "base change pattern", "count"), rdd.snpBaseChangesPatternCount)
//    write(out + "snpBaseChangePatternCount.txt", s)
//  }
//
//  "export read depth distribution" should "succeed" in {
//    val s = toCSV(List("category", "read depth", "count"), rdd.readDepthDistribution())
//    write(out + "snpReadDepthDistribution.txt", s)
//  }
//
//  "export quality distribution" should "succeed" in {
//    val s = toCSV(List("category", "quality", "count"), rdd.qualityDistribution(bin = roundToDecimals(1)))
//    write(out + "snpQualityDistribution.txt", s)
//  }
//
//  "export allele freq distribution" should "succeed" in {
//    val s = toCSV(List("category", "allele freq", "count"), rdd.alleleFrequencyDistribution(bin = roundToDecimals(1)))
//    write(out + "snpAlleleFrequencyDistribution.txt", s)
//  }
//
//  "export clinvar ratio" should "succeed" in {
//    val s = toCSV(List("category", "clinvar?", "count"), rdd.clinvarRatio)
//    write(out + "snpClinvarRatio.txt", s)
//  }
//
//  "export dbSNP ratio" should "succeed" in {
//    val s = toCSV(List("category", "dbSNP?", "count"), rdd.dbSnpRatio)
//    write(out + "snpDbSNPRatio.txt", s)
//  }

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
