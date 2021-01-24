package com.flipkart.testScenario.dsp;

import com.flipkart.dto.TestRunDetails;
import com.flipkart.manager.ComparisonManager;
import com.flipkart.manager.JarManager;
import com.flipkart.testScenario.TestScenario;
import com.flipkart.utils.CompareType;
import com.flipkart.utils.DSPConstants;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.flipkart.utils.DSPConstants.*;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PromoteWorkFlowViaSandbox extends TestScenario {
    private final ComparisonManager comparisonManager;
    private final JarManager jarManager;


    @Override
    protected void runTestCase(Map<String, Object> input, TestRunDetails testRunDetails) throws Exception {
        log.info("Test Description: " + testRunDetails.getTestDescription());

        String[] runCommand = new String[7];
        runCommand[0] = "java";
        runCommand[1] = "-jar";
        runCommand[2] = input.get(SANDBOX_JAR_PATH).toString();
        runCommand[3] = DSPConstants.SANDBOX_ENVIRONMENT;
        runCommand[4] = input.get(BUCKET_POSTFIX).toString();
        runCommand[5] = DSPConstants.SANDBOX_PROMOTE_COMMAD;
        runCommand[6] = input.get(DSP_JOB_ID).toString();

        log.info(" Command Created to execute test Scenario 3 " + Arrays.toString(runCommand));

        Object output = jarManager.executeJar(runCommand, "Issue in Executing promote");
        log.info(" Result of Execution is as follow   " + output);
        if(output.toString().contains("DSP-Oncall will approve this pull request and merge it to master") ||
                output.toString().contains("Following are the workflow Configuration which is productionize")) {
            testRunDetails.setActualResult("COMPLETED");
            return;
        }
        testRunDetails.setActualResult("FAILED");
    }

    @Override
    protected Object loadExpectedResult() {
         return "COMPLETED";
    }

    @Override
    protected boolean assertEqual(Object expectedValue, Object actualValue) {
        return comparisonManager.compare(expectedValue, actualValue, CompareType.TEXT);
    }
}
