package org.broadinstitute.hellbender.tools.walkers.vqsr;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.Allele;
import org.broadinstitute.hellbender.utils.SimpleInterval;

import java.util.Comparator;
import java.util.List;

/*
 * Represents a data item for VQSR (a site).
 * Package private because it's not usable outside of VQSR.
 */
final class VariantDatum {

    public double[] annotations;
    public boolean[] isNull;
    public boolean isKnown;
    public double lod;
    public boolean atTruthSite;
    public boolean atTrainingSite;
    public boolean atAntiTrainingSite;
    public boolean isTransition;
    public boolean isSNP;
    public boolean failingSTDThreshold;
    public double originalQual;
    public double prior;
    public int consensusCount;
    public SimpleInterval loc;
    public int worstAnnotation;
    public double worstValue;
    public MultivariateGaussian assignment; // used in K-means implementation
    public boolean isAggregate; // this datum was provided to aid in modeling but isn't part of the input callset
    public Allele referenceAllele;
    public Allele alternateAllele;

    public static final Comparator<VariantDatum> VariantDatumLODComparator = (datum1, datum2) -> Double.compare(datum1.lod, datum2.lod);

    public static int countCallsAtTruth(final List<VariantDatum> data, double minLOD ) {
        return (int)data.stream().filter(d -> (d.atTruthSite && d.lod >= minLOD)).count(); //XXX cast to int for compatibility
    }

    /**
     * Return a comparator for VariantDatums, given a sequence Dictionary.
     * @param seqDictionary
     * @return a lambda closure comparator that uses the provided sequence dictionary
     */
    public static Comparator<VariantDatum> getComparator(final SAMSequenceDictionary seqDictionary) {
        return (VariantDatum vd1, VariantDatum vd2) ->
        {
            if ( vd1 == vd2 ) {
                return 0;
            }
            else {
                int i1 = seqDictionary.getSequence(vd1.loc.getContig()).getSequenceIndex();
                int i2 = seqDictionary.getSequence(vd2.loc.getContig()).getSequenceIndex();
                if ( i1 < i2 ) {
                    return -1;
                } else if ( i1 > i2 ) {
                    return 1;
                }
                // they're on the same contig, so check the start
                else if ( vd1.loc.getStart() < vd2.loc.getStart() ) {
                    return -1;
                } else if ( vd1.loc.getStart() > vd2.loc.getStart() ) {
                    return 1;
                }
                // same start, check end
                else if ( vd1.loc.getEnd() < vd2.loc.getEnd() ) {
                    return -1;
                }  else if ( vd1.loc.getEnd() > vd2.loc.getEnd() ) {
                    return 1;
                }
            }

            return 0;
        };
    }

}