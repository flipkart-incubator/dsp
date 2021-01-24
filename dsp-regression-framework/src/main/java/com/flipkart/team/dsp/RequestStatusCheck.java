package com.flipkart.team.dsp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.client.HttpRequestClient;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.RunStatusDTO;
import com.flipkart.enums.TestExecutionStatus;
import com.flipkart.utils.DSPConstants;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RequestStatusCheck {
    private final HttpRequestClient httpRequestClient;

    public TestExecutionStatus getRequestStatus(String jobId) {
        String targetUrl = String.format(DSPConstants.RUN_STATUS_URL_FORMAT, DSPConstants.REGRESSION_HOST_NAME, jobId);
        TypeReference<RunStatusDTO> typeReference = new TypeReference<RunStatusDTO>() {};

        try {
            HttpURLConnection httpURLConnection = getHttpURLConnection(targetUrl);
            RunStatusDTO runStatusDTO = httpRequestClient.getRequest(httpURLConnection, typeReference);
            if (runStatusDTO.getRequestStatus().equals(RequestStatus.ACTIVE)
                    || runStatusDTO.getRequestStatus().equals(RequestStatus.CREATED)) {
                return TestExecutionStatus.RUNNING;
            } else if (runStatusDTO.getRequestStatus().equals(RequestStatus.FAILED)) {
                return TestExecutionStatus.FAILED;
            }
            return TestExecutionStatus.PASSED;
        } catch (IOException e) {
            return TestExecutionStatus.FAILED;
        }
    }

    public TestExecutionStatus waitForRequestCompletion(String jobId) {
        while (true) {
            TestExecutionStatus executionStatus = getRequestStatus(jobId);
            if (executionStatus == TestExecutionStatus.RUNNING) {
                try {
                    log.info("Waiting for Execution to Complete");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    return TestExecutionStatus.FAILED;
                }
            } else
                return executionStatus;
        }
    }

    private HttpURLConnection getHttpURLConnection(String targetUrl) throws IOException {
        URL url = new URL(targetUrl);
        HttpURLConnection request = (HttpURLConnection) url.openConnection();
        request.setRequestProperty("Accept", "application/json");
        request.setRequestProperty("Content-Type", "application/json");
        request.setDoOutput(true);
        request.setDoInput(true);
        return request;
    }
}
