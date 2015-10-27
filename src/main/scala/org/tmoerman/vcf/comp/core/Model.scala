package org.tmoerman.vcf.comp.core

import org.bdgenomics.adam.rich.RichVariant._
import org.bdgenomics.formats.avro.Variant
import org.tmoerman.adam.fx.avro.AnnotatedGenotype

import scala.Function._
import scala.util.Try

/**
 * @author Thomas Moerman
 */
object Model extends Serializable {

  type Sample        = String
  type Contig        = String
  type Base          = String
  type Ref           = String
  type Alt           = String

  type Start         = Long
  type Count         = Long

  type VariantKey = (Sample, Contig, Start, Ref, Alt)

  def variantKey(annotatedGenotype: AnnotatedGenotype): VariantKey = {
    val genotype = annotatedGenotype.getGenotype
    val variant  = genotype.getVariant

    (genotype.getSampleId,
      variant.getContig.getContigName,
      variant.getStart,
      variant.getReferenceAllele,
      variant.getAlternateAllele)
  }

  def assertSNP(v: Variant): Unit = if (! v.isSingleNucleotideVariant()) throw new Exception("variant is not a SNP")

  def trySNP(v: Variant): Try[Variant] = Try { assertSNP(v); v}

  // VARIANT TYPE

  type VariantType = String

  val SNP       = "SNP"
  val INSERTION = "INSERTION"
  val DELETION  = "DELETION"
  val OTHER     = "OTHER"

  def toVariantType(v: Variant): VariantType =
         if (v.isSingleNucleotideVariant()) SNP
    else if (v.isInsertion())               INSERTION
    else if (v.isDeletion())                DELETION
    else                                    OTHER

  // BASE TYPE

  type BaseType = String

  val PURINE     = "PURINE"
  val PYRIMIDINE = "PYRIMIDINE"

  val BY_BASE =
    Map("A" -> PURINE,
        "G" -> PURINE,
        "C" -> PYRIMIDINE,
        "T" -> PYRIMIDINE)

  def toBaseType(b: Base): BaseType = BY_BASE(b)

  // BASE CHANGE

  type BaseChange = (Base, Base)

  def toBaseChange(v: Variant): BaseChange = (v.getReferenceAllele, v.getAlternateAllele)

  // BASE CHANGE PATTERN

  type BaseChangePattern = Set[Base]

  def toBaseChangePattern(baseChange: BaseChange): BaseChangePattern = baseChange match { case (a, b) => Set(a, b) }

  // BASE CHANGE TYPE

  type BaseChangeType = String

  val TRANSITION   = "Ti"
  val TRANSVERSION = "Tv"

  def toBaseChangeType(baseChange: BaseChange): BaseChangeType =
    toBaseChangePattern(baseChange)
      .map(toBaseType)
      .size match {
        case 1 => TRANSITION
        case 2 => TRANSVERSION
      }

  def toBaseChangeType(v: Variant): BaseChangeType =
    trySNP(v)
      .map(toBaseChange)
      .map(toBaseChangeType)
      .get

  // COMPARISON

  type Category = String

  val LEFT_UNIQUE  = "LEFT-UNIQUE"
  val RIGHT_UNIQUE = "RIGHT-UNIQUE"
  val CONCORDANT   = "CONCORDANT"

  type ComparisonRow = (Option[AnnotatedGenotype], Option[AnnotatedGenotype])

  type Labels = Map[Category, String]

  def toCategory(row: ComparisonRow): Category =
    row match {
      case (Some(_), Some(_)) => CONCORDANT
      case (Some(_), None) => LEFT_UNIQUE
      case (None, Some(_)) => RIGHT_UNIQUE
      case (None, None) => throw new Exception("glitch in the matrix")
    }

  def toRepresenter(category: Category, row: ComparisonRow): Option[AnnotatedGenotype] =
    category match {
      case CONCORDANT   => row._1
      case LEFT_UNIQUE  => row._1
      case RIGHT_UNIQUE => row._2
    }
  
  def isSnp(cat: Category, row: ComparisonRow) =
    toRepresenter(cat, row)
      .map(_.getGenotype.getVariant)
      .map(_.isSingleNucleotideVariant())
      .get

}
