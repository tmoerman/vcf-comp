package org.tmoerman.vcf.comp

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rich.RichVariant
import org.tmoerman.vcf.comp.core.Model.ComparisonRow

object RDDFunctions {



}

class RichVariantRDDFunctions(val rdd: RDD[RichVariant]) extends Serializable with Logging {

}

class ComparisonRDDFunctions(val rdd: RDD[ComparisonRow]) extends Serializable with Logging {

}