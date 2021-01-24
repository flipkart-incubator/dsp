package com.flipkart.testScenario.dsp;

import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dto.TestRunDetails;
import com.flipkart.enums.TestExecutionStatus;
import com.flipkart.exception.FileOperationException;
import com.flipkart.exception.JarException;
import com.flipkart.exception.TestScenarioExecutionException;
import com.flipkart.manager.ComparisonManager;
import com.flipkart.manager.JarManager;
import com.flipkart.testScenario.TestScenario;
import com.flipkart.utils.CompareType;
import com.flipkart.utils.DSPConstants;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Map;

import static com.flipkart.utils.DSPConstants.*;

//TEST cases For Yaml Version 1

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RunYAMLVersion20BySandbox extends TestScenario {
    private final JarManager jarManager;
    private final ComparisonManager comparisonManager;

    @Override
    public void runTestCase(Map<String, Object> input, TestRunDetails testRunDetails) throws InterruptedException, JarException {
        log.info("Test Description: " + testRunDetails.getTestDescription());
        String yamlFilePath = input.get(YAML_LOCAL_PATH).toString();
        String[] runCommand = new String[7];
        runCommand[0] = "java";
        runCommand[1] = "-jar";
        runCommand[2] = input.get(SANDBOX_JAR_PATH).toString();
        runCommand[3] = DSPConstants.SANDBOX_ENVIRONMENT;
        runCommand[4] = input.get(BUCKET_POSTFIX).toString();
        runCommand[5] = DSPConstants.SANDBOX_RUN_COMMAD;
        runCommand[6] = yamlFilePath;

        log.info("Command Created to execute Yaml: " + Arrays.toString(runCommand));


        try {
            Long jobId = (Long) jarManager.executeJar(runCommand);
            if (jobId == 0) {
                testRunDetails.setTestExecutionStatus(TestExecutionStatus.FAILED);
                testRunDetails.setFailureReason("Unable To get JobId for the run");
            } else {
                testRunDetails.setActualResult("COMPLETED");
                testRunDetails.setTestExecutionStatus(TestExecutionStatus.PASSED);
                testRunDetails.setExtraDetails(jobId);
            }
        } catch (TestScenarioExecutionException e) {
            testRunDetails.setFailureReason("Workflow Failed to Execute in Platform");
            testRunDetails.setTestExecutionStatus(TestExecutionStatus.FAILED);
            testRunDetails.setActualResult("FAILED");
        }
    }

    @Override
    public Object loadExpectedResult() {
        return "COMPLETED";
    }

    @Override
    public boolean assertEqual(Object expectedValue, Object actualValue) {
        log.info("Asserted started");
        return comparisonManager.compare(expectedValue, actualValue, CompareType.TEXT);
    }
}
