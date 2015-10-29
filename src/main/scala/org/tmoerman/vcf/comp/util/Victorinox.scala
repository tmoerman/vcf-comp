package org.tmoerman.vcf.comp.util

import java.io.{BufferedWriter, FileWriter, File}

import scala.math.BigDecimal.RoundingMode.HALF_UP
import scala.reflect.ClassTag

/**
 * Assorted utility functions.
 *
 * @author Thomas Moerman
 */
object Victorinox {

  // TODO make Float a generic numeric type
  def withMax[A, N](f: A => Double)(l: A, r: A): A = if (f(l) > f(r)) l else r

  def dropKey[T: ClassTag]: ((_, T)) => T = _._2

  def toCSV[A, B, C](h: List[String], m: Map[(A, B), C]): String =
    h.mkString("\t") + "\n" + m.toList.map{ case ((a, b), c) => s"$a\t$b\t$c" }.sorted.mkString("\n")

  def write(fileName: String, content: String): Unit = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(content)
    bw.close()
  }

  def roundToDecimals(nDecimals: Int = 2)(d: Double): Double = BigDecimal(d).setScale(nDecimals, HALF_UP).toDouble

}
