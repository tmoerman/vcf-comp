package org.tmoerman.vcf.comp.core

import org.bdgenomics.adam.rich.RichVariant._
import org.bdgenomics.formats.avro.Variant
import org.tmoerman.adam.fx.avro.AnnotatedGenotype

import scala.util.Try

/**
 * @author Thomas Moerman
 */
object Model extends Serializable {

  type Base  = String
  type Count = Long

  def alleleFrequency(genotype: AnnotatedGenotype) = ???

  // READ DEPTH

  type ReadDepth = Int

  def readDepth(genotype: AnnotatedGenotype): Int = genotype.getGenotype.getReadDepth

  // QUALITY

  type Quality = Float

  def quality(genotype: AnnotatedGenotype): Float = genotype.getGenotype.getVariantCallingAnnotations.getVariantCallErrorProbability

  // Variant Key

  type Sample = String
  type Contig = String
  type Start  = Long
  type Ref    = String
  type Alt    = String

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

  def variantType(v: Variant): VariantType =
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

  def baseChange(v: Variant): BaseChange = (v.getReferenceAllele, v.getAlternateAllele)

  // BASE CHANGE PATTERN

  type BaseChangePattern = Set[Base]

  def baseChangePattern(baseChange: BaseChange): BaseChangePattern = baseChange match { case (a, b) => Set(a, b) }

  def baseChangePattern(genotype: AnnotatedGenotype): BaseChangePattern =
    baseChangePattern(baseChange(genotype.getGenotype.getVariant))

  // BASE CHANGE TYPE

  type BaseChangeType = String

  val TRANSITION   = "Ti"
  val TRANSVERSION = "Tv"

  def baseChangeType(baseChange: BaseChange): BaseChangeType =
    baseChangePattern(baseChange)
      .map(toBaseType)
      .size match {
        case 1 => TRANSITION
        case 2 => TRANSVERSION
      }

  def baseChangeType(v: Variant): BaseChangeType =
    trySNP(v)
      .map(baseChange)
      .map(baseChangeType)
      .get

  // COMPARISON

  type Category = String

  val LEFT_UNIQUE  = "LEFT-UNIQUE"
  val RIGHT_UNIQUE = "RIGHT-UNIQUE"
  val CONCORDANT   = "CONCORDANT"

  type ComparisonRow = (Option[AnnotatedGenotype], Option[AnnotatedGenotype])

  type Labels = Map[Category, String]

  def category(row: ComparisonRow): Category =
    row match {
      case (Some(_), Some(_)) => CONCORDANT
      case (Some(_), None) => LEFT_UNIQUE
      case (None, Some(_)) => RIGHT_UNIQUE
      case (None, None) => throw new Exception("glitch in the matrix")
    }
  
  def representant(category: Category, row: ComparisonRow): AnnotatedGenotype =
    category match {
      case CONCORDANT   => row._1.get
      case LEFT_UNIQUE  => row._1.get
      case RIGHT_UNIQUE => row._2.get
    }
  
  def catRep(row: ComparisonRow): (Category, AnnotatedGenotype) = {
    val cat: Category = category(row)

    (cat, representant(cat, row))
  }

  def isSnp(genotype: AnnotatedGenotype): Boolean = isSnp(genotype.getGenotype.getVariant)

  def isSnp(variant: Variant): Boolean = variant.isSingleNucleotideVariant()

}
