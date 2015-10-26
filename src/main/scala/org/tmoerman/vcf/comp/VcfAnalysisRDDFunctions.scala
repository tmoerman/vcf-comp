package org.tmoerman.vcf.comp

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rich.RichVariant

/**
 * @author Thomas Moerman
 */
class RichVariantRDDFunctions(val rdd: RDD[RichVariant]) extends Serializable with Logging {

  //

}
