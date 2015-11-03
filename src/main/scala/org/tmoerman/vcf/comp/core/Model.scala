package org.tmoerman.vcf.comp.core

import org.bdgenomics.adam.rich.RichVariant._
import org.bdgenomics.formats.avro.{GenotypeAllele, Variant}
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

  def baseChangeType(v: Variant): BaseChangeType =
    trySNP(v)
      .map(baseChange)
      .map(baseChangeType)
      .get

  // SNP

  def isSnp(genotype: AnnotatedGenotype): Boolean = isSnp(genotype.getGenotype.getVariant)

  def isSnp(variant: Variant): Boolean = variant.isSingleNucleotideVariant()

  // ANNOTATIONS

  import org.bdgenomics.adam.rich.RichGenotype._

  import scala.collection.JavaConversions._

  // def isHomozygous(r: RichAnnotatedGenotype) = r.genotype.getType =

  def hasClinvarAnnotations(r: RichAnnotatedGenotype) = r.annotations.exists(a => a.clinvarAnnotations.isDefined)

  def hasDbSnpAnnotations(r: RichAnnotatedGenotype)   = r.annotations.exists(a => a.dbSnpAnnotations.isDefined)

  def hasSnpEffAnnotations(r: RichAnnotatedGenotype)  = r.annotations.exists(a => a.functionalAnnotations.nonEmpty ||
                                                                                  a.nonsenseMediatedDecay.nonEmpty ||
                                                                                  a.lossOfFunction.nonEmpty)

}
