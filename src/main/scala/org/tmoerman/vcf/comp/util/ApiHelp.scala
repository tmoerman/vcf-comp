package org.tmoerman.vcf.comp.util

import System.lineSeparator

/**
  * @author Thomas Moerman
  */
trait ApiHelp {

  /**
    * @return Returns a help String.
    */
  def help =
    getClass
      .getDeclaredMethods
      .filter(m => ! (m.getName.contains("$") || m.getName == "help"))
      .map(m => "- " + m.getName)
      .sorted
      .mkString(lineSeparator)

}
