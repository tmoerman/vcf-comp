package org.tmoerman.test.spike

import org.scalatest.FlatSpec
import scalaz._
import Scalaz._

/**
 * @author Thomas Moerman
 */
class ScalazSpec extends FlatSpec {

  "bla" should "bla" in {

    val a = Map("a" -> 1, "b" -> 2)
    val b = Map("a" -> 5, "c" -> 3)

    println(a.--(b.keys))

  }

}
