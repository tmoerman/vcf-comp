package org.tmoerman.vcf.comp

import org.scalatest.{Matchers, FlatSpec}
import org.tmoerman.adam.fx.avro.Impact
import org.tmoerman.adam.fx.avro.Impact._

/**
 * @author Thomas Moerman
 */
class SpikeSpec extends FlatSpec with Matchers {

  "bla" should "bla" in {

    val li = List[Int]()

    val m = li.groupBy(_ % 2 == 0).mapValues(_.maxBy(identity))

    val r = m match {
      case Map => "leeg"
      case _         => "vol"
    }

    println(r)
  }

  "java enums" should "be sorted correctly" in {

    List(LOW, HIGH, MODERATE, MODIFIER).sorted shouldBe List(HIGH, MODERATE, LOW, MODIFIER)

  }

}
