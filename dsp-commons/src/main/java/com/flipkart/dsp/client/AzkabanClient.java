package com.flipkart.dsp.client;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.azkaban.*;
import com.flipkart.dsp.config.AzkabanConfig;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.utils.NodeMetaData;
import com.ning.http.client.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

/**
 */
@Getter
@Slf4j
public class AzkabanClient implements AutoCloseable {
    public static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 60000;
    public static final String BASE_PATH = "";

    private final String baseUrl;
    private final int requestTimeoutMillis;
    private AsyncHttpClient asyncHttpClient;
    private ObjectMapper objectMapper;

    public AzkabanClient(String host, int port, int requestTimeoutMillis, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.baseUrl = "http://" + host + (port != 80 ? (":" + port) : "") + BASE_PATH;
        log.debug("baseURL: {}", this.baseUrl);
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.asyncHttpClient = new AsyncHttpClient(new AsyncHttpClientConfig.Builder().setAcceptAnyCertificate(true).setHostnameVerifier((hostname, session) -> true).build());
    }

    public <R> R executesSync(String method,
                              String path,
                              JavaType type,
                              Function<RequestBuilder, RequestBuilder> function) throws AzkabanException {
        Error error;
        try {
            RequestBuilder requestBuilder = new RequestBuilder(method).setUrl(baseUrl + path)
                    .setRequestTimeout(requestTimeoutMillis);

            Request request = function.apply(requestBuilder).build();
            log.debug("Request being sent: {}", request);
            Response response = asyncHttpClient.executeRequest(request).get();
            if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                if(type == null)
                    return null;
                return objectMapper.readValue(response.getResponseBody(), type);
            }

            error = objectMapper.readValue(response.getResponseBody(), Error.class);
            log.debug("Got error response for API call error: {}", error);
            throw new AzkabanException(error.getMessage());
        }catch (Exception e) {
            throw new AzkabanException(e.getMessage(), e);
        }
    }

    public AzkabanLoginRequest getAzkabanLoginRequest() {
        return new AzkabanLoginRequest(this);
    }

    public AzkabanFlowSubmitRequestV2 getAzkabanFlowSubmitRequestV2(AzkabanConfig azkabanConfig, String sessionId,
                                                                    String jobName, NodeMetaData nodeMetaData, String azkabanProject,
                                                                    String disabledNodes) {
        return new AzkabanFlowSubmitRequestV2(this, azkabanConfig, sessionId, jobName, nodeMetaData, azkabanProject, disabledNodes);
    }

    public AzkabanJobStatusResponse getAzkabanJobStatus(String sessionId, long execId) throws AzkabanException {
            return new AzkabanJobStatusRequest(this, sessionId, execId).executeSync();
    }

    public AzkabanJobKillResponse killAzkabanJob(String sessionId, Long execId) throws AzkabanException {
        return new KillAzkabanJob(this, sessionId, execId).executeSync();
    }

    @Override
    public void close() {
        if(asyncHttpClient != null && !asyncHttpClient.isClosed()) {
            this.asyncHttpClient.close();
        }
    }

    public AzkabanCreateProjectResponse createProject(String azkabanSessionId, String projectName, String projectDesc) throws AzkabanException {
        return new AzkabanCreateProjectRequest(this, objectMapper, azkabanSessionId, projectName, projectDesc).executeSync();
    }

    public Void uploadProject(String azkabanSessionId, String projectName, String zipFilePath) throws AzkabanException {
        return new AzkabanUploadProjectRequest(this, azkabanSessionId, projectName, zipFilePath).executeSync();
    }

    public AzkabanProjectScheduleRequestResponse scheduleProject(String isRecurring, String period, String projectName, String flow, Long projectId,
                                                                 String scheduleTime,
                                                                 String scheduleDate,
                                                                 String azkabanSessionId)  throws AzkabanException {
        return new AzkabanScheduleProjectRequest(this, isRecurring, period, projectName
                , flow, projectId, scheduleTime,scheduleDate , azkabanSessionId).executeSync();
    }
}
