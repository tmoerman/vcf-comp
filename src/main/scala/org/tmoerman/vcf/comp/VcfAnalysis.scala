package org.tmoerman.vcf.comp

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rich.RichVariant

/**
 * @author Thomas Moerman
 */
object VcfAnalysis {

  type ContigName = String
  type Base       = String
  type Count      = Long

  /// ---

  type VariantType = String
  val SNP       = "snp"
  val INSERTION = "insertion"
  val DELETION  = "deletion"
  val OTHER     = "other"

  val PURINE     = "purine"
  val PYRIMIDINE = "pyrimidine"

  type BaseType = String
  val BASE_TYPES = Map("A" -> PURINE,
                       "G" -> PURINE,
                       "C" -> PYRIMIDINE,
                       "T" -> PYRIMIDINE)

  def baseType(n: Base) = BASE_TYPES(n)

  type BaseChangeType = String
  val TRANSITION   = "Ti"
  val TRANSVERSION = "Tv"

  /// ---

  /**
   * @param v a RichVariant instance
   * @return Returns a String representation of the variant type: "snp", "insertion", "deletion" or "other".
   */
  def variantType(v: RichVariant): VariantType =
         if (v.isSingleNucleotideVariant()) SNP
    else if (v.isInsertion())               INSERTION
    else if (v.isDeletion())                DELETION
    else                                    OTHER

  /**
   * @param variants RDD of RichVariants
   * @return Returns a Map of counts by variant type.
   */
  def countVariantTypes(variants: RDD[RichVariant]): Map[VariantType, Count] =
    variants
      .map(variantType)
      .countByValue
      .toMap

  /**
   * @param v RichVariant
   * @return Returns the directional base change of the Variant.
   */
  def baseChange(v: RichVariant): BaseChange = (v.getReferenceAllele, v.getAlternateAllele)

  /**
   * @param v RichVariant
   * @return Returns the SNP base change type (Ti or Tv).
   */
  def baseChangeType(v: RichVariant): BaseChangeType = {
    if (! v.isSingleNucleotideVariant()) {
      throw new IllegalArgumentException("variant is not a SNP")
    }
    
    baseChange(v).distinctBaseTypes.size match {
      case 1 => TRANSITION
      case 2 => TRANSVERSION
    }
  }
  
  // Mutation pattern (nucleotide changes)

  type BaseChange = (Base, Base)
  type MutationPattern = Set[Base]

  /**
   * @param variants RDD of RichVariants
   * @return Returns the count per directional base change
   */
  def countBaseChanges(variants: RDD[RichVariant]): Map[BaseChange, Count] =
    variants
      .filter(_.isSingleNucleotideVariant())
      .map(baseChange)
      .countByValue
      .toMap
  
  def countMutationPatterns(variants: RDD[RichVariant]): Map[MutationPattern, Count] =
    variants
      .filter(_.isSingleNucleotideVariant())
      .map(baseChange(_).toMutationPattern)
      .countByValue
      .toMap

  implicit def pimpBaseChange(baseChange: BaseChange): BaseChangeFunctions = new BaseChangeFunctions(baseChange)

  class BaseChangeFunctions(baseChange: BaseChange) extends Serializable {

    /**
     * @return Returns a Set representing the base change without taking into account the direction of change.
     */
    def toMutationPattern: MutationPattern = baseChange match { case (a, b) => Set(a, b) }

    /**
     * @return Returns a distinct Set of base types (Purine, Pyrimidine) corresponding to the bases.
     */
    def distinctBaseTypes: Set[BaseType] = baseChange.toMutationPattern.map(baseType)

  }

}