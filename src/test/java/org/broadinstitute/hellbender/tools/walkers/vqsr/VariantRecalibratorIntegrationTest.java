package org.broadinstitute.hellbender.tools.walkers.vqsr;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.test.IntegrationTestSpec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

public class VariantRecalibratorIntegrationTest extends CommandLineProgramTest {

    @Override
    public String getTestedClassName() {
        return VariantRecalibrator.class.getSimpleName();
    }

    public String getToolTestDataDir(){
        return publicTestDir + "org/broadinstitute/hellbender/tools/walkers/VQSR/";
    }

    public String getLargeVQSRTestDataDir(){
        return largeFileTestDir + "VQSR/";
    }

    @BeforeMethod
    public void initializeVariantRecalTests() {
        //This aligns the initial state of the RNG with the initial state of the GATK3 RNG at the time
        //the tests start running, which we want to do in order to get the same results produced by
        //GATK3.
        logger.debug("Initializing VQSR tests/resetting random number generator");
        Utils.resetRandomGenerator();
    }

    @Test
    public void testVariantRecalibratorSNP() throws IOException {

        //NOTE: The number of iterations required to get the model used by this tool to converge (more
        //specifically, to have enough negative training data to proceed), and the end results, are both
        //very sensitive to the state of the random number generator at the time the tool starts to execute.
        //Sampling a single integer from the RNG at the start aligns the initial state of the random number
        //generator with the initial state of the GATK3 random number generator at the time the *tool* starts
        //executing (both RNGs use the same seed, but the pathway to the test in GATK3 results in an additional
        //integer being sampled), thus allowing the results to be easily compared against those produced by
        //GATK3 on the same inputs. This also happens to allow this particular test to succeed on the first
        //iteration (max_attempts=1) which we want for test performance reasons. Failing to call nextInt here
        //would result in the model failing to converge on the first 3 attempts (it would succeed on the 4th),
        //and the results would not match those produced by GATK3 on these same inputs.
        //
        //Also, as a result, this test cannot be reliably executed or reproduced outside of this test framework,
        //since it depends on the RNG being in the  initial state established by the the code in this file.

        @SuppressWarnings("unused")
        final int hack = Utils.getRandomGenerator().nextInt();

        final String inputFile = getLargeVQSRTestDataDir() + "phase1.projectConsensus.chr20.1M-10M.raw.snps.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " --variant " + inputFile +
                " -L 20:1,000,000-10,000,000" +
                " --resource a,known=true,prior=10.0:"
                        + getLargeVQSRTestDataDir() + "dbsnp_132_b37.leftAligned.20.1M-10M.vcf" +
                " --resource b,truth=true,training=true,prior=15.0:"
                        + getLargeVQSRTestDataDir() + "sites_r27_nr.b37_fwd.20.1M-10M.vcf" +
                " --resource c,training=true,truth=true,prior=12.0:"
                        + getLargeVQSRTestDataDir() + "Omni25_sites_1525_samples.b37.20.1M-10M.vcf" +
                " -an QD -an HaplotypeScore -an HRun" +
                " --trustAllPolymorphic" + // for speed
                " -mode SNP" +
                " --output %s" +
                //" --rscriptFile %s"  +
                " -tranchesFile %s",
                Arrays.asList(
                    // this "expected" vcf is also used as input for the ApplyVQSR test
                    getLargeVQSRTestDataDir() + "snpRecal.vcf",
                    //getToolTestDataDir() + "expected/SNPRScript.r",
                    getLargeVQSRTestDataDir() + "expected/SNPTranches.txt")
        );
        spec.executeTest("testVariantRecalibratorSNP-"+inputFile, this);
    }

}

