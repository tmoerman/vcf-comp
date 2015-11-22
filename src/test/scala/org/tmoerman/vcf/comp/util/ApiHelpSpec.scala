package org.tmoerman.vcf.comp.util

import org.scalatest.{Matchers, FlatSpec}
import org.tmoerman.vcf.comp.core.SnpComparisonRDDFunctions

/**
  * Created by tmo on 22/11/15.
  */
class ApiHelpSpec extends FlatSpec with Matchers {

  class GuineaPig extends ApiHelp {

    def foo(arg: String): String = arg

    def bar = 42

  }

  "an object with API help" should "provide a list of functions" in {
    new GuineaPig().help shouldBe List("- bar", "- foo").mkString(System.lineSeparator)
  }

  "SnpComparisonRDDFunctions" should "only provide relevant functions" in {
    new SnpComparisonRDDFunctions(null).help.contains("log") shouldBe false

    new SnpComparisonRDDFunctions(null).help.contains("rdd") shouldBe false
  }

}
