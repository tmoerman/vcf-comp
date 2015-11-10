package org.tmoerman.vcf.comp.core

import org.bdgenomics.adam.rich.RichGenotype._
import org.bdgenomics.adam.rich.RichVariant._
import org.bdgenomics.formats.avro.GenotypeAllele.{Alt, Ref}
import org.bdgenomics.formats.avro.{Genotype, Variant}
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.model.RichAnnotatedGenotype
import scala.collection.JavaConversions._

import scala.util.Try

/**
 * @author Thomas Moerman
 */
object Model extends Serializable {

  type Base  = String
  type Label = String
  type Count = Long

  // ALLELE FREQUENCY

  type AlleleFrequency = Double

  def alleleFrequency(genotype: Genotype): AlleleFrequency = genotype.getAlternateReadDepth.toDouble / genotype.getReadDepth

  def alleleFrequency(genotype: AnnotatedGenotype): AlleleFrequency = alleleFrequency(genotype.getGenotype)

  // READ DEPTH

  type ReadDepth = Int

  def readDepth(genotype: Genotype): ReadDepth = genotype.getReadDepth

  def readDepth(genotype: AnnotatedGenotype): ReadDepth = readDepth(genotype.getGenotype)

  // QUALITY

  type Quality = Double

  def quality(genotype: Genotype): Quality = genotype.getVariantCallingAnnotations.getVariantCallErrorProbability.toDouble

  def quality(genotype: AnnotatedGenotype): Quality = quality(genotype.getGenotype)

  // SNP

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

  def baseChange(genotype: AnnotatedGenotype): BaseChange = baseChange(genotype.getGenotype.getVariant)

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

  def baseChangeType(v: Variant): BaseChangeType = trySNP(v).map(baseChange).map(baseChangeType).get

  def baseChangeType(genotype: AnnotatedGenotype): BaseChangeType = baseChangeType(genotype.getGenotype.getVariant)

  // SNP

  def isSnp(genotype: AnnotatedGenotype): Boolean = isSnp(genotype.getGenotype.getVariant)

  def isSnp(variant: Variant): Boolean = variant.isSingleNucleotideVariant()

  // ZYGOSITY

  type Zygosity = String

  val HOMOZYGOUS   = "HOMOZYGOUS"
  val HETEROZYGOUS = "HETEROZYGOUS"
  val NO_CALL      = "NO CALL"

  def zygosity(genotype: AnnotatedGenotype): Zygosity = genotype.getGenotype.getAlleles.toList.distinct match { // @see RichGenotype.getType
    case List(Ref)        => HOMOZYGOUS
    case List(Alt)        => HOMOZYGOUS
    case List(Ref, Alt) |
         List(Alt, Ref)   => HETEROZYGOUS
    case _                => NO_CALL
  }

  // Functional annotations

  val NA = "N/A"

  def functionalImpact(genotype: AnnotatedGenotype): String =
    genotype.getAnnotations.getFunctionalAnnotations.map(_.getImpact).sorted.headOption.map(_.toString).getOrElse(NA)

  def functionalAnnotation(genotype: AnnotatedGenotype): String =
    genotype.getAnnotations.getFunctionalAnnotations.map(_.getAnnotations.head).headOption.getOrElse(NA)

  def transcriptBiotype(genotype: AnnotatedGenotype): String =
    genotype.getAnnotations.getFunctionalAnnotations.flatMap(e => Option(e.getTranscriptBiotype)).headOption.getOrElse(NA)

  // TODO synonymous / non-synonymous count

  // ANNOTATIONS

  def hasClinvarAnnotations(r: RichAnnotatedGenotype) = r.annotations.exists(a => a.clinvarAnnotations.isDefined)

  def hasDbSnpAnnotations(r: RichAnnotatedGenotype)   = r.annotations.exists(a => a.dbSnpAnnotations.isDefined)

  def hasSnpEffAnnotations(r: RichAnnotatedGenotype)  = r.annotations.exists(a => a.functionalAnnotations.nonEmpty ||
                                                                                  a.nonsenseMediatedDecay.nonEmpty ||
                                                                                  a.lossOfFunction.nonEmpty)

  // Value Holders TODO perhaps move this to separate namespace?

  case class VcfQCParams(label:     Label = "X",
                         quality:   Quality = 0,
                         readDepth: ReadDepth = 0)

  case class SnpComparisonParams(matchOnSampleId: Boolean = false,
                                 unifyConcordant: Boolean = true,
                                 labels:     (Label, Label)         = ("A", "B"),
                                 qualities:  (Quality, Quality)     = (0, 0),
                                 readDepths: (ReadDepth, ReadDepth) = (1, 1))

  case class CategoryCount(category: String, count: Count)

  case class ProjectionCount[P](projection: P, count: Count)

  case class CategoryProjectionCount[P](category: String, projection: P, count: Count)

  // Quantize

  def quantize(step: Int)(value: Int): Int = value - (value % step) + (step / 2)

  def quantize(step: Double)(value: Double): Double = value - (value % step) + (step.toDouble / 2)

}
