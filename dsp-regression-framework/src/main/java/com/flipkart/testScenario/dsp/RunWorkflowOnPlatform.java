package com.flipkart.testScenario.dsp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.client.HttpRequestClient;
import com.flipkart.dsp.models.ExecutionOutput;
import com.flipkart.dsp.utils.JsonUtils;
import com.flipkart.dto.TestRunDetails;
import com.flipkart.enums.TestExecutionStatus;
import com.flipkart.exception.TestScenarioExecutionException;
import com.flipkart.manager.ComparisonManager;
import com.flipkart.team.dsp.RequestStatusCheck;
import com.flipkart.testScenario.TestScenario;
import com.flipkart.utils.CompareType;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;

import static com.flipkart.utils.DSPConstants.DSP_API_URL;
import static com.flipkart.utils.DSPConstants.DSP_PLATFORM_RUN_PAYLOAD;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RunWorkflowOnPlatform extends TestScenario {
    private final HttpRequestClient httpRequestClient;
    private final ComparisonManager comparisonManager;
    private final RequestStatusCheck requestStatusCheck;

    @Override
    public void runTestCase(Map<String, Object> input, TestRunDetails testRunDetails) throws Exception {
        log.info("Test Description: " + testRunDetails.getTestDescription());
        String targetUrl = input.get(DSP_API_URL).toString();
        TypeReference<ExecutionOutput> typeReference = new TypeReference<ExecutionOutput>() {};

        HttpURLConnection httpURLConnection = getHttpURLConnection(targetUrl);
        ExecutionOutput executionOutput;
        try {
            executionOutput = httpRequestClient.postRequest(httpURLConnection, input.get(DSP_PLATFORM_RUN_PAYLOAD), typeReference);
        } catch (IOException e) {
            throw new TestScenarioExecutionException(e.getMessage());
        }

        log.info("Execution Output: " + JsonUtils.DEFAULT.toJson(executionOutput));
        TestExecutionStatus testExecutionStatus = requestStatusCheck.waitForRequestCompletion(String.valueOf(executionOutput.getJobId()));
        if(testExecutionStatus.equals(TestExecutionStatus.PASSED)) {
            testRunDetails.setActualResult("COMPLETED");
            testRunDetails.setTestExecutionStatus(TestExecutionStatus.PASSED);
            return;
        }
        testRunDetails.setFailureReason("Workflow Failed to Execute in Platform");
        testRunDetails.setTestExecutionStatus(TestExecutionStatus.FAILED);
        testRunDetails.setActualResult("FAILED");
    }

    @Override
    public Object loadExpectedResult() {
        return "COMPLETED";
    }

    @Override
    public boolean assertEqual(Object expectedValue, Object actualValue) {
        return comparisonManager.compare(expectedValue, actualValue, CompareType.TEXT);
    }
}
