package com.flipkart.dsp.client;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.dataframe.*;
import com.flipkart.dsp.client.exceptions.DSPClientException;
import com.flipkart.dsp.client.exceptions.DSPServiceException;
import com.flipkart.dsp.client.misc.CreateEventAuditRequest;
import com.flipkart.dsp.client.misc.CreateExecutionEnvironmentSnapshotRequest;
import com.flipkart.dsp.client.misc.GetDataTableRequest;
import com.flipkart.dsp.client.misc.GetExternalCredentialsByClientAliasRequest;
import com.flipkart.dsp.client.notification.callback.UpdateEntityBatchRequest;
import com.flipkart.dsp.client.notification.email.GetNotificationPreferenceRequest;
import com.flipkart.dsp.client.notification.email.SendEmailNotificationForPartitionStateChangeRequest;
import com.flipkart.dsp.client.pipelinestep.*;
import com.flipkart.dsp.client.request.GetRequestByRequestId;
import com.flipkart.dsp.client.request.GetRequestStatusByRequestId;
import com.flipkart.dsp.client.script.GetScriptMetaRequest;
import com.flipkart.dsp.client.workflow.GetQueueInfoRequest;
import com.flipkart.dsp.client.workflow.GetWorkflowDetailsRequest;
import com.flipkart.dsp.client.workflow.TriggerEntityRegister;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.dto.QueueInfoDTO;
import com.flipkart.dsp.dto.UpdateEntityDTO;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.misc.NotificationPreference;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepSGAudit;
import com.flipkart.dsp.entities.run.config.RunConfig;
import com.flipkart.dsp.entities.script.ScriptMeta;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.ExceptionResponse;
import com.flipkart.dsp.models.ExecutionEnvironmentSnapshot;
import com.flipkart.dsp.models.ExternalCredentials;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.event_audits.EventAudit;
import com.flipkart.dsp.models.misc.PartitionDetailsEmailNotificationRequest;
import com.flipkart.dsp.models.sg.DataTable;
import com.flipkart.dsp.utils.HTTPRequestUtil;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.Strings;

import java.awt.image.ImagingOpException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 */
@Getter
@Slf4j
public class DSPServiceClient implements AutoCloseable {
    public static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 60000;
    public static final String BASE_PATH = "";

    private final String baseUrl;
    private final int requestTimeoutMillis;
    private final int maxRetries;
    private final int retryGapInMillis;
    private final String scriptBaseDir;
    private final AsyncHttpClient asyncHttpClient;

    public DSPServiceClient(String host, int port, int maxRetries, int retryGapInMillis, String scriptBaseDir) {
        this(host, port, maxRetries, retryGapInMillis, scriptBaseDir, DEFAULT_REQUEST_TIMEOUT_SECONDS, new AsyncHttpClient());
    }

    public DSPServiceClient(String host, int port, int maxRetries, int retryGapInMillis, String scriptBaseDir, int requestTimeoutMillis) {
        this(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, new AsyncHttpClient());
    }

    public DSPServiceClient(String host, int port, int maxRetries, int retryGapInMillis, String scriptBaseDir, int requestTimeoutMillis, AsyncHttpClient asyncHttpClient) {
        this.baseUrl = "http://" + host
                + (port != 80 ? (":" + port) : "")
                + BASE_PATH;
        log.debug("baseURL: {}", this.baseUrl);

        this.scriptBaseDir = scriptBaseDir;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.asyncHttpClient = asyncHttpClient;
        this.maxRetries = maxRetries;
        this.retryGapInMillis = retryGapInMillis;
    }

    public void close() {
        log.info("Closing DSP Service Client.");
        try {
            if (asyncHttpClient != null && !asyncHttpClient.isClosed()) {
                asyncHttpClient.close();
            }
        } catch (RuntimeException e) {
            //No harm done
            log.warn("", e);
        }
    }

    public <R> R executesSync(String method,
                              String path,
                              JavaType type,
                              Function<RequestBuilder, RequestBuilder> function) {

        try {
            RequestBuilder requestBuilder = new RequestBuilder(method).setUrl(baseUrl + path)
                    .setRequestTimeout(requestTimeoutMillis);

            Request request = function.apply(requestBuilder).build();
            Response response = HTTPRequestUtil.executeWithRetries(() -> asyncHttpClient.executeRequest(request).get(), maxRetries, retryGapInMillis);
            if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                if (type.equals(JsonUtils.DEFAULT.mapper.constructType(InputStream.class))) {
                    return (R) response.getResponseBodyAsStream();
                } else if (!type.equals(JsonUtils.DEFAULT.mapper.constructType(Void.class))) {
                    if (Strings.isNullOrEmpty(response.getResponseBody()))
                        return null;
                    return JsonUtils.DEFAULT.mapper.readValue(response.getResponseBody(), type);
                } else {
                    return null;
                }
            }
            try {
                ExceptionResponse exceptionResponse = JsonUtils.DEFAULT.mapper.readValue(response.getResponseBody(), ExceptionResponse.class);
                Error error = new Error(500 ,"DSP service failed because of unknown issues.");
                if (exceptionResponse.getErrors().size() > 0) {
                    ExceptionResponse.Error errorResponse = exceptionResponse.getErrors().get(0);
                    error = new Error(400, errorResponse.getMessage());
                }
                throw new DSPServiceException(error);
            } catch (IOException e) {
                Error error = JsonUtils.DEFAULT.mapper.readValue(response.getResponseBody(), Error.class);
                throw new DSPServiceException(error);
            }
        } catch (DSPServiceException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DSPClientException(e);
        }
    }

    public void updateWorkFlowAuditStatusBatch(List<UpdateEntityDTO> updateEntityDTOList) {
        new UpdateEntityBatchRequest(this, updateEntityDTOList).executeSync();
    }

    public void updateDataFrameAuditStatus(Long requestId, Long workflowId, String currentStatus, String newStatus) {
        new UpdateDataFrameAuditStatus(this, requestId, workflowId, currentStatus, newStatus);
    }

    public DataFrameAudit getDataFrameAudit(Long dataFrameAuditId) {
        return new GetDataFrameAuditByIdRequest(this, dataFrameAuditId).executeSync();
    }

    public DataFrameAudit getDataFrameAudit(Long dataFrameId, Long dataFrameOverrideAuditId, String partitions) {
        return new GetDataFrameAuditRequest(this, dataFrameId, dataFrameOverrideAuditId, partitions).executeSync();
    }

    public DataFrameAudit getLatestDataFrameAuditByDataFrameId(Long dataFrameId) {
        return new GetLatestDataFrameAuditByDataFrameIdRequest(this, dataFrameId).executeSync();
    }

    public DataFrameAudit saveDataFrameAudit(DataFrameAudit dataFrameAudit) {
        return new CreateDataFrameAuditRequest(this, dataFrameAudit).executeSync();
    }

    public Map<String, List<String>> persistRequestDataframeAudit(Long requestId, Long workflowId, Long pipelineStepId, Set<DataFrameAudit> dataFrameAudits) {
        return new CreateRequestDataFrameAuditsRequest(this, requestId, workflowId, pipelineStepId, dataFrameAudits).executeSync();
    }

    public void updateDataFrameAudit(DataFrameAudit dataFrameAudit) {
        new CreateDataFrameAuditRequest(this, dataFrameAudit).executeSync();
    }


    public DataFrameOverrideAudit saveDataFrameOverrideAudit(DataFrameOverrideAudit dataFrameOverrideAudit) {
        return new CreateDataFrameOverrideAuditRequest(this, dataFrameOverrideAudit).executeSync();
    }

    public void updateDataFrameOverrideAudit(DataFrameOverrideAudit dataFrameOverrideAudit) {
        new UpdateDataFrameOverrideAuditRequest(this, dataFrameOverrideAudit).executeSync();
    }

    public void updateFailedDataFrameOverrideAudit(Long requestId) {
        new UpdateFailedDataframeOverrideAuditRequest(this, requestId).executeSync();
    }

    public DataFrameOverrideAudit getDataFrameOverrideAudit(Long dataFrameId, String inputDataId, DataFrameOverrideType dataFrameOverrideType) {
        return new GetDataFrameOverrideAuditRequest(this, dataFrameId, inputDataId, dataFrameOverrideType).executeSync();
    }

    public DataFrameOverrideAudit getDataFrameOverrideAuditById(Long id) {
        return new GetDataFrameOverrideAuditByIdRequest(this, id).executeSync();
    }

    public DataFrameOverrideAudit getDataFrameOverrideAuditByIdAndRequestId(Long dataFrameId, Long requestId) {
        return new GetDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest(this, dataFrameId, requestId).executeSync();
    }

    public DataFrameOverrideAudit getDataFrameOverrideAuditByIdRequestAndType(Long dataFrameId, Long requestId, DataFrameOverrideType dataFrameOverrideType) {
        return new GetDataframeOverrideAuditByIdRequestTypeRequest(this, dataFrameId, requestId, dataFrameOverrideType).executeSync();
    }

    public DataTable getDataTable(String tableName) {
        return new GetDataTableRequest(this, tableName).executeSync();
    }

    public void saveEventAudit(EventAudit eventAudit) {
        new CreateEventAuditRequest(this, eventAudit).executeSync();
    }


    public WorkflowDetails getWorkflowDetails(Long workflowId) {
        return new GetWorkflowDetailsRequest(this, workflowId).executeSync();
    }


    public void triggerEntityRegisterForWorkflow(Long requestId, String serializedTableList) {
        new TriggerEntityRegister(this, requestId, serializedTableList).executeSync();
    }

    public com.flipkart.dsp.entities.request.Request getRequest(Long requestId) {
        return new GetRequestByRequestId(this, requestId).executeSync();
    }

    public RequestStatus getRequestStatusById(Long requestId) {
        return new GetRequestStatusByRequestId(this, requestId).executeSync();
    }


    public ScriptMeta getScriptMetaById(long scripId) {
        return new GetScriptMetaRequest(this, scripId).executeSync();
    }

    public void createPipelineStepRuntimeConfig(Long pipelineStepId, RunConfig runConfig, ConfigPayload configPayload) {
        PipelineStepRuntimeConfig pipelineStepRuntimeConfig = PipelineStepRuntimeConfig.builder()
                .pipelineStepId(pipelineStepId).runConfig(runConfig).scope(configPayload.getScope())
                .pipelineExecutionId(configPayload.getPipelineExecutionId())
                .workflowExecutionId(configPayload.getWorkflowExecutionId()).build();
        new CreatePipelineStepRuntimeConfigRequest(this, pipelineStepRuntimeConfig).executeSync();
    }

    public PipelineStepRuntimeConfig getPipelineStepRuntimeConfig(String pipelineExecutionId, Long pipelineStepId) {
        return new GetPipelineStepRuntimeConfigRequest(this, pipelineExecutionId, pipelineStepId).executeSync();
    }

    public List<PipelineStepRuntimeConfig> getPipelineStepRuntimeConfig(String workflowExecutionId, String scope) {
        return new GetPipelineStepRuntimeConfigByScopeRequest(this, workflowExecutionId, scope).executeSync();
    }

    public PipelineStepAudit savePipelineStepAuditRequest(PipelineStepAudit pipelineStepAudit) {
        return new CreatePipelineStepAuditRequest(this, pipelineStepAudit).executeSync();
    }

    public long savePipelineStepSgAuditRequest(PipelineStepSGAudit pipelineStepSgAudit) {
        return new CreatePipelineStepSgAuditRequest(this, pipelineStepSgAudit).executeSync();
    }

    public List<PipelineStepAudit> getPipelineStepLogDetails(Integer attempt, Long pipelineStepId, String pipelineExecutionId, Long refreshId) {
        return new GetPipelineStepAuditsByPipelineExecutionIdRequest(this, attempt, refreshId, pipelineStepId, pipelineExecutionId).executeSync();
    }

    public QueueInfoDTO getQueueInfo(Long workflowId) {
        return new GetQueueInfoRequest(this, workflowId).executeSync();
    }


    public void saveExecutionEnvironmentSnapshotRequest(ExecutionEnvironmentSnapshot executionEnvironmentSnapShot) {
        new CreateExecutionEnvironmentSnapshotRequest(this, executionEnvironmentSnapShot).executeSync();
    }

    public ExternalCredentials getExternalCredentials(String clientAlias) {
        return new GetExternalCredentialsByClientAliasRequest(this, clientAlias).executeSync();
    }

    public void sendEmailNotificationForPartitionStateChangeRequest(PartitionDetailsEmailNotificationRequest partitionDetailsEmailNotificationRequest) {
        new SendEmailNotificationForPartitionStateChangeRequest(this, partitionDetailsEmailNotificationRequest).executeSync();
    }

    public NotificationPreference getNotificationPreference(Long workflowId) {
        return new GetNotificationPreferenceRequest(this, workflowId).executeSync();
    }

    public PipelineStep getPipelineStepById(Long pipelineStepId) {
        return new GetPipelineStepByIdRequest(this, pipelineStepId).executeSync();
    }
}
