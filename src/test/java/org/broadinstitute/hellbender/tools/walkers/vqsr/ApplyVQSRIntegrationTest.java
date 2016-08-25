package org.broadinstitute.hellbender.tools.walkers.vqsr;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.test.IntegrationTestSpec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

public class ApplyVQSRIntegrationTest extends CommandLineProgramTest {

    @Override
    public String getTestedClassName() {
        return ApplyVQSR.class.getSimpleName();
    }

    public String getToolTestDataDir(){
        return publicTestDir + "org/broadinstitute/hellbender/tools/walkers/VQSR/";
    }

    public String getLargeVQSRTestDataDir(){
        return largeFileTestDir + "VQSR/";
    }

    @BeforeMethod
    public void initializeWalkerTests() {
        logger.debug("Initializing VQSR tests; resetting random number generator");
        Utils.resetRandomGenerator();
    }

    @Test
    public void testApplyRecalibration() throws IOException {
        final String inputFile = getLargeVQSRTestDataDir() + "phase1.projectConsensus.chr20.1M-10M.raw.snps.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                        " -L 20:1,000,000-10,000,000" +
                        " --variant " + inputFile +
                        " --lenient" +
                        " --output %s" +
                        " -mode SNP" +
//                        " -tranchesFile " + getLargeVQSRTestDataDir() + "expected/SNPTranches.txt" +
                        " -recalFile " + getLargeVQSRTestDataDir() + "snpRecal.vcf",
                Arrays.asList(getLargeVQSRTestDataDir() + "expected/snpApplyResult.vcf"));
        spec.executeTest("testApplyRecalibration-"+inputFile, this);
    }

}

