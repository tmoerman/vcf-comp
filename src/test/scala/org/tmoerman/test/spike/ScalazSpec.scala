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

  "semigroup magic" should "bla" in {
    val a = Map("a" -> Map(1 -> 11), "b" -> Map(2 -> 22))
    val b = Map("a" -> Map(5 -> 55), "c" -> Map(3 -> 33))

    val r = List(a, b).reduce(_ |+| _)

    println(r)
  }

  import org.tmoerman.vcf.comp.util.Victorinox._

  "intersecting maps" should "bla" in {
    val A = Map("a:t" -> List(1, 2), "c:g" -> List(4), "g:t" -> List(666), "k:l" -> List(99))

    val B = Map("a:t" -> List(3,8), "c:g" -> List(5), "t:a" -> List(777))

    val C = Map("prul" -> List(88))

    def concordant2(mA: Map[String, List[Int]], mB: Map[String, List[Int]]) =
      (mA intersectWith mB){ case pair => pair }
        .toList match {
          case Nil => Map()
          case list =>
            list
              .map{ case (k, (a, b)) => ((k, a), (k, b))}
              .unzip match { case (concA, concB) => Map(("A", "CONCORDANT") -> concA.toMap,
                                                        ("B", "CONCORDANT") -> concB.toMap)
              }
          }

    def concordant(mA: Map[String, List[Int]], mB: Map[String, List[Int]]) =
      (mA intersectWith mB){ case pair => pair }
        .toIterable
        .map{ case (k, (a, b)) => ((k, a), (k, b))}
        .unzip match {
          case (Nil,   Nil  ) => Map()
          case (concA, concB) => Map(("A", "CONCORDANT") -> concA.toMap,
                                     ("B", "CONCORDANT") -> concB.toMap)
          }

    val r = concordant(A, C)

    println(r)
  }

}

//    val concordant2 =
//      (A intersectWithKey B){ case (k, a, b) => ((k, a), (k, b)) }
//        .map(dropKey)
//        .unzip match {
//          case (concA, concB) => Map (("A", "CONCORDANT") -> concA.toMap,
//                                      ("B", "CONCORDANT") -> concB.toMap)
//        }
//
//    val discordant =
//      ((A -- B.keys).mapKeys(bc => (("A", "Disc"), bc)) ++
//       (B -- A.keys).mapKeys(bc => (("B", "Disc"), bc)))//(_ ++ _)
//        .toList // mapping over a List != mapping over a Map
//        .map{case ((cat, bc), discA) => Map(cat -> Map(bc -> discA))}
//        .reduce(_ |+| _)
//        //.reduce(_ ++ _)

//.mapValues(_.unzip)

//        .unzip match { case (concA, concB) =>
//          (concA.map(_.head).map(e => ("A", e)),
//           concB.map(_.head).map(e => ("B", e)))}

