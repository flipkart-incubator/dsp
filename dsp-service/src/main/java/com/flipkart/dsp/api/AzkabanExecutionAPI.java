package com.flipkart.dsp.api;

import com.flipkart.dsp.actors.RequestActor;
import com.flipkart.dsp.azkaban.AzkabanCreateProjectResponse;
import com.flipkart.dsp.azkaban.AzkabanJobStatusResponse;
import com.flipkart.dsp.azkaban.AzkabanLoginResponse;
import com.flipkart.dsp.azkaban.AzkabanWorkflowSubmitResponse;
import com.flipkart.dsp.client.AzkabanClient;
import com.flipkart.dsp.config.AzkabanConfig;
import com.flipkart.dsp.entities.enums.AzkabanJobStatus;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.utils.NodeMetaData;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.WebApplicationException;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class AzkabanExecutionAPI {

    private final RequestActor requestActor;
    private final AzkabanClient azkabanClient;
    private final AzkabanConfig azkabanConfig;

    AzkabanWorkflowSubmitResponse triggerAzkabanFlowV2(String jobName, NodeMetaData nodeMetaData, String azkabanProject, String disabledNodes) throws AzkabanException {
        AzkabanWorkflowSubmitResponse response = azkabanClient.getAzkabanFlowSubmitRequestV2(azkabanConfig,
                getAzkabanSessionId(), jobName, nodeMetaData, azkabanProject, disabledNodes).executeSync();
        if (response == null || response.getExecid() <= 0) {
            throw new AzkabanException("Invalid response while triggering job: " + jobName);
        }

        return response;
    }

    private String getAzkabanSessionId() throws AzkabanException {
        AzkabanLoginResponse
                azkabanLoginResponse =
                azkabanClient.getAzkabanLoginRequest().azkabanConfig(azkabanConfig).executeSync();
        boolean isSuccess = azkabanLoginResponse != null && azkabanLoginResponse.getStatus() != null && azkabanLoginResponse.getStatus().equalsIgnoreCase("success");
        String sessionId = isSuccess ? azkabanLoginResponse.getSessionId() : null;
        if (sessionId == null) {
            throw new AzkabanException("exception in getting session id from azkaban");
        }
        return sessionId;
    }

    public void createProject(String projectName, String description) throws AzkabanException {
        AzkabanCreateProjectResponse response = azkabanClient.createProject(getAzkabanSessionId(), projectName, description);
        if (isNull(response))
            throw new AzkabanException("Could not create Azkaban Project: " + projectName);
    }

    public void uploadProject(String projectName, String zipFilePath) throws AzkabanException {
        azkabanClient.uploadProject(getAzkabanSessionId(), projectName, zipFilePath);
    }

    public void killJob(Long azkabanExecId) throws DSPSvcException {
        try {
            azkabanClient.killAzkabanJob(getAzkabanSessionId(), azkabanExecId);
        } catch (AzkabanException e) {
            throw new DSPSvcException("Failed to kill azkaban job with exec id: " + azkabanExecId);
        }
    }

    public Long retryJob(Long azkabanExecId, Request request) throws DSPSvcException {
        //re-start the azkaban job
        AzkabanJobStatusResponse jobStatus = getJobStatus(azkabanExecId, request.getId());
        validateAzkabanJobStatus(jobStatus.getStatus(), azkabanExecId, request.getId());
        String nodesTobeSkipped = getSkippedNode(jobStatus);

        NodeMetaData nodeMetaData = new NodeMetaData();
        nodeMetaData.setRequestId(request.getId());

        AzkabanWorkflowSubmitResponse azkabanWorkflowSubmitResponse = null;
        try {
            azkabanWorkflowSubmitResponse = triggerAzkabanFlowV2(jobStatus.getFlow(), nodeMetaData, jobStatus.getProject(), nodesTobeSkipped);
        } catch (AzkabanException e) {
            log.error("Failed to restart the azkaban job with id  {} and requestEntity id {}", azkabanExecId, request.getId(), e);
            throw new WebApplicationException("Failed to restart the azkaban job with id " + azkabanExecId + " and requestEntity id " + request.getId());
        }
        request.setRequestStatus(RequestStatus.ACTIVE);
        request.setAzkabanExecId(azkabanWorkflowSubmitResponse.getExecid());
        requestActor.save(request);
        return azkabanWorkflowSubmitResponse.getExecid();
    }

    public AzkabanJobStatusResponse getJobStatus(Long azkabanExecId, Long requestId) throws DSPSvcException {
        try {
            return azkabanClient.getAzkabanJobStatus(getAzkabanSessionId(), azkabanExecId);
        } catch (AzkabanException e) {
            log.error("Failed to fetch azkaban job status with id {} and requestEntity id {}", azkabanExecId, requestId, e);
            throw new DSPSvcException("Failed to fetch azkaban job status with id " + azkabanExecId + " and requestEntity id " + requestId);
        }
    }

    private void validateAzkabanJobStatus(AzkabanJobStatus status, Long azkabanExecId, Long requestId) throws DSPSvcException {
        if (status.equals(AzkabanJobStatus.RUNNING) || status.equals(AzkabanJobStatus.PREPARING)
                || status.equals(AzkabanJobStatus.READY) || status.equals(AzkabanJobStatus.QUEUED) ||
                status.equals(AzkabanJobStatus.PAUSED) | status.equals(AzkabanJobStatus.WAITING)) {
            throw new DSPSvcException("Job requested to retry with id " + azkabanExecId
                    + " and request id " + requestId + "is not in terminal state.");
        }

    }

    private String getSkippedNode(AzkabanJobStatusResponse jobStatusResponse) {
        return jobStatusResponse.getNodes().stream().filter(node -> node.getId().endsWith("_OTS")
                || node.getId().endsWith("_SG")).map(node -> "\"" + node.getId() + "\"").collect(Collectors.joining(","));
    }
}
