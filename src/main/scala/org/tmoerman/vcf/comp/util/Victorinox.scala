package org.tmoerman.vcf.comp.util

import scala.reflect.ClassTag

/**
 * Assorted utility functions.
 *
 * @author Thomas Moerman
 */
object Victorinox {

  // TODO make Float a generic numeric type
  def withMax[A, N](f: A => Float)(l: A, r: A): A = if (f(l) > f(r)) l else r

  def dropKey[T: ClassTag]: ((_, T)) => T = _._2

}
