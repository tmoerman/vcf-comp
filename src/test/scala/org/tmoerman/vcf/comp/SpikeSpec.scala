package org.tmoerman.vcf.comp

import org.scalatest.FlatSpec

/**
 * @author Thomas Moerman
 */
class SpikeSpec extends FlatSpec {

  "bla" should "bla" in {

    val li = List[Int]()

    val m = li.groupBy(_ % 2 == 0).mapValues(_.maxBy(identity))

    val r = m match {
      case Map => "leeg"
      case _         => "vol"
    }

    println(r)
  }

}
