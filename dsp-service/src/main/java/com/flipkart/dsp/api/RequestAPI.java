package com.flipkart.dsp.api;

import com.flipkart.dsp.actors.RequestActor;
import com.flipkart.dsp.actors.RequestStepAuditActor;
import com.flipkart.dsp.actors.UserActor;
import com.flipkart.dsp.azkaban.AzkabanWorkflowSubmitResponse;
import com.flipkart.dsp.db.entities.RequestEntity;
import com.flipkart.dsp.dto.AzkabanFlow;
import com.flipkart.dsp.entities.enums.RequestStepAuditStatus;
import com.flipkart.dsp.entities.request.RequestStepAudit;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.utils.JsonUtils;
import com.flipkart.dsp.utils.NodeMetaData;
import com.flipkart.dsp.validation.Validator;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.dsp.models.RequestStatus.ACTIVE;
import static com.flipkart.dsp.models.RequestStatus.CREATED;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RequestAPI {
    private final Validator validator;
    private final UserActor userActor;
    private final RequestActor requestActor;
    private final AzkabanExecutionAPI azkabanExecutionAPI;
    private final RequestStepAuditActor requestStepAuditActor;

    RequestEntity createRequest(String triggeredBy, WorkflowDetails workflowDetails, ExecuteWorkflowRequest executeWorkflowRequest) throws ValidationException {
        validator.validateTableInformation(workflowDetails.getWorkflow(), executeWorkflowRequest);
        workflowDetails.setParentWorkflowRefreshId(executeWorkflowRequest.getParentWorkflowRefreshId());
        RequestEntity requestEntity = RequestEntity.builder().requestId(executeWorkflowRequest.getRequestId()).azkabanExecId(-1L)
                .workflowId(workflowDetails.getWorkflow().getId()).data(executeWorkflowRequest).requestStatus(CREATED)
                .callbackUrl(getCallBackUrl(workflowDetails, executeWorkflowRequest))
                .workflowDetailsSnapshot(JsonUtils.DEFAULT.toJson(workflowDetails)).isNotified(false)
                .userEntity(userActor.getUserByName(triggeredBy)).build();
        requestEntity = requestActor.save(requestEntity);
        return requestEntity;
    }

    private String getCallBackUrl(WorkflowDetails workflowDetails, ExecuteWorkflowRequest executeWorkflowRequest) {
        if (isNotBlank(executeWorkflowRequest.getCallBackUrl()))
            return executeWorkflowRequest.getCallBackUrl();
        return workflowDetails.getWorkflow().getWorkflowMeta().getCallbackUrl();
    }

    void triggerAzkabanJob(RequestEntity requestEntity, AzkabanFlow azkabanFlow) throws AzkabanException {
        NodeMetaData nodeMetaData = new NodeMetaData();
        nodeMetaData.setRequestId(requestEntity.getId());
        log.debug("triggering dsp processing DAG with request_id: {}", requestEntity.getId());

        AzkabanWorkflowSubmitResponse azkabanWorkflowSubmitResponse = azkabanExecutionAPI.triggerAzkabanFlowV2(azkabanFlow.getFlow(),
                nodeMetaData, azkabanFlow.getProject(), null);
        log.debug("Triggered Azkaban job : {}", azkabanWorkflowSubmitResponse);

        requestEntity.setRequestStatus(ACTIVE);
        requestEntity.setAzkabanExecId(azkabanWorkflowSubmitResponse.getExecid());
    }

}
