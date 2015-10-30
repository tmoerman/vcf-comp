package org.tmoerman.vcf.comp.core

import org.bdgenomics.adam.rich.RichVariant._
import org.bdgenomics.formats.avro.Variant
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.model.RichAnnotatedGenotype

import scala.util.Try

/**
 * @author Thomas Moerman
 */
object Model extends Serializable {

  type Base  = String
  type Count = Long

  // ALLELE FREQUENCY

  type AlleleFrequency = Double

  def alleleFrequency(genotype: AnnotatedGenotype): AlleleFrequency =
    genotype.getGenotype.getAlternateReadDepth.toDouble / genotype.getGenotype.getReadDepth

  // READ DEPTH

  type ReadDepth = Int

  def readDepth(genotype: AnnotatedGenotype): ReadDepth = genotype.getGenotype.getReadDepth

  // QUALITY

  type Quality = Double

  def quality(genotype: AnnotatedGenotype): Quality = genotype.getGenotype.getVariantCallingAnnotations.getVariantCallErrorProbability.toDouble

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

    (genotype.getSampleId, // TODO is this necessary?
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

  def variantType(genotype: AnnotatedGenotype): VariantType = variantType(genotype.getGenotype.getVariant)

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

  def format(baseChange: BaseChange): String = baseChange match { case (a,b) => s"$a->$b" }

  def baseChange(v: Variant): BaseChange = (v.getReferenceAllele, v.getAlternateAllele)

  def baseChangeString(genotype: AnnotatedGenotype): String = format(baseChange(genotype.getGenotype.getVariant))

  // BASE CHANGE PATTERN

  type BaseChangePattern = Set[Base]

  def baseChangePattern(baseChange: BaseChange): BaseChangePattern = baseChange match { case (a, b) => Set(a, b) }

  def baseChangePattern(genotype: AnnotatedGenotype): BaseChangePattern = baseChangePattern(baseChange(genotype.getGenotype.getVariant))

  def baseChangePatternString(genotype: AnnotatedGenotype): String = baseChangePattern(genotype).toList.sorted.mkString(":")

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

  val A_ONLY = "A-ONLY"
  val B_ONLY = "B-ONLY"
  val BOTH   = "BOTH"

  type ComparisonRow = (Option[AnnotatedGenotype], Option[AnnotatedGenotype])

  type Labels = Map[Category, String]

  def category(row: ComparisonRow): Category =
    row match {
      case (Some(_), Some(_)) => BOTH
      case (Some(_), None   ) => A_ONLY
      case (None   , Some(_)) => B_ONLY
      case (None   , None   ) => throw new Exception("glitch in the matrix")
    }
  
  def representant(category: Category, row: ComparisonRow): AnnotatedGenotype =
    category match {
      case BOTH   => row._1.get
      case A_ONLY  => row._1.get
      case B_ONLY => row._2.get
    }
  
  def catRep(labels: Option[Labels] = None)(row: ComparisonRow): (Category, AnnotatedGenotype) = {
    val cat: Category = category(row)

    val label: String = labels.flatMap(_.get(cat)).getOrElse(cat)

    (label, representant(cat, row))
  }

  def isSnp(genotype: AnnotatedGenotype): Boolean = isSnp(genotype.getGenotype.getVariant)

  def isSnp(variant: Variant): Boolean = variant.isSingleNucleotideVariant()

  def hasClinvarAnnotations(r: RichAnnotatedGenotype) = r.annotations.exists(a => a.clinvarAnnotations.isDefined)

  def hasDbSnpAnnotations(r: RichAnnotatedGenotype)   = r.annotations.exists(a => a.dbSnpAnnotations.isDefined)

  def hasSnpEffAnnotations(r: RichAnnotatedGenotype)  = r.annotations.exists(a => a.functionalAnnotations.nonEmpty ||
                                                                                  a.nonsenseMediatedDecay.nonEmpty ||
                                                                                  a.lossOfFunction.nonEmpty)

}
