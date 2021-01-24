package com.flipkart.testScenario;

import com.flipkart.dsp.utils.JsonUtils;
import com.flipkart.dto.TestExecutionDetails;
import com.flipkart.dto.TestRunDetails;
import com.flipkart.enums.DSPTestScenarioEnum;
import com.flipkart.enums.TestExecutionStatus;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class TestScenario {
    protected abstract void runTestCase(Map<String, Object> input, TestRunDetails testRunDetails) throws Exception;
    protected abstract Object loadExpectedResult();
    protected abstract boolean assertEqual(Object expectedValue, Object actualValue);

    public TestExecutionDetails run(Map<String, Object> input, String testCaseName) {
        TestExecutionDetails testExecutionDetails = TestExecutionDetails.builder().testScenarioName(testCaseName).build();
        try {
            Object expectedResult = loadExpectedResult();
            TestRunDetails testRunDetails = TestRunDetails.builder()
                    .testDescription(DSPTestScenarioEnum.valueOf(testCaseName).getTestDescription()).build();
            runTestCase(input, testRunDetails);
            testExecutionDetails.setTestDescription(testRunDetails.getTestDescription());
            boolean isPassed = false;
            if (expectedResult != null  && testRunDetails.getActualResult() != null) {
                isPassed = assertEqual(expectedResult, testRunDetails.getActualResult());
            }
            if(!isPassed) {
                 String failureReason = testRunDetails.getFailureReason().isEmpty() ? "Mismatch between Actual and Expected Output" :
                         testRunDetails.getFailureReason();
                 testExecutionDetails.setFailureReason(failureReason);
                 TestExecutionStatus testExecutionStatus = testRunDetails.getTestExecutionStatus() == null ? TestExecutionStatus.FAILED :
                         testRunDetails.getTestExecutionStatus();
                 testExecutionDetails.setTestExecutionStatus(testExecutionStatus);
            } else {
                testExecutionDetails.setTestExecutionStatus(TestExecutionStatus.PASSED);
                testExecutionDetails.setExtraDetails(testRunDetails.getExtraDetails());
            }
        } catch (Exception e) {
            e.printStackTrace();
            testExecutionDetails.setTestExecutionStatus(TestExecutionStatus.FAILED);
            testExecutionDetails.setFailureReason(e.getMessage());
            log.error("Error Encountered while executing Test Case " + testCaseName);
        }
        log.info("Test Execution Details: " + JsonUtils.DEFAULT.toJson(testExecutionDetails));
        return testExecutionDetails;
    }

    protected HttpURLConnection getHttpURLConnection(String targetUrl) throws IOException {
        URL url = new URL(targetUrl);
        HttpURLConnection request = (HttpURLConnection) url.openConnection();
        request.setRequestProperty("Accept", "application/json");
        request.setRequestProperty("Content-Type", "application/json");
        request.setDoOutput(true);
        request.setDoInput(true);
        return request;
    }
}
