package org.tmoerman.vcf.comp.core

import org.scalatest.{Matchers, FlatSpec}

import Model._

/**
  * Created by tmo on 06/12/15.
  */
class ModelSpec extends FlatSpec with Matchers {

  "base change type" should "be correct" in {

//    baseChangeType(("A", "C")) shouldBe "Tv"

    val transitions = List(
      ("A","G"), ("G","A"),
      ("C","T"), ("T","C"))

    transitions.map(baseChangeType).distinct shouldBe List("Ti")

    val transversions = List(
      ("A","C"), ("C", "A"),
      ("A","T"), ("T", "A"),
      ("G","C"), ("C", "G"),
      ("G","T"), ("T", "G"))

    transversions.map(baseChangeType).distinct shouldBe List("Tv")

  }

}
