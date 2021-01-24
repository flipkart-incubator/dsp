package com.flipkart.testScenario.dsp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.HttpRequestClient;
import com.flipkart.dsp.models.workflow.WorkflowPromoteRequest;
import com.flipkart.dsp.models.workflow.WorkflowPromoteResponse;
import com.flipkart.dsp.utils.JsonUtils;
import com.flipkart.dto.TestRunDetails;
import com.flipkart.enums.TestExecutionStatus;
import com.flipkart.exception.TestScenarioExecutionException;
import com.flipkart.manager.ComparisonManager;
import com.flipkart.testScenario.TestScenario;
import com.flipkart.utils.CompareType;
import com.flipkart.utils.DSPConstants;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;

import static com.flipkart.utils.DSPConstants.*;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
public class PromoteWorkflowViaPlatform extends TestScenario {

    private final ObjectMapper objectMapper;
    private final HttpRequestClient httpRequestClient;
    private final ComparisonManager comparisonManager;

    @Override
    protected void runTestCase(Map<String, Object> input, TestRunDetails testRunDetails) throws Exception {
        log.info("Test Description: " + testRunDetails.getTestDescription());

        String targetUrl = String.format(DSPConstants.PROMOTE_WORKLFOW_GROUP_VIA_PLATFORM_URL_FORMAT,
                DSPConstants.REGRESSION_HOST_NAME, input.get(DSP_JOB_ID).toString());

        TypeReference<WorkflowPromoteResponse> typeReference = new TypeReference<WorkflowPromoteResponse>() {};
        HttpURLConnection httpURLConnection = getHttpURLConnection(targetUrl);

        try {
            WorkflowPromoteResponse promoteResponse = httpRequestClient.postRequest(httpURLConnection, input.get(DSP_PLATFORM_PROMOTE_PAYLOAD), typeReference);
            log.info("promoteResponse: " + promoteResponse);
            if(promoteResponse == null) {
                throw new TestScenarioExecutionException("Test ExecutionFailed");
            } else {
                testRunDetails.setActualResult("COMPLETED");
                testRunDetails.setTestExecutionStatus(TestExecutionStatus.PASSED);
            }
        } catch (IOException | TestScenarioExecutionException e) {
            testRunDetails.setFailureReason("Workflow Failed to Execute in Platform");
            testRunDetails.setTestExecutionStatus(TestExecutionStatus.FAILED);
            testRunDetails.setActualResult("FAILED");
        }
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
