package com.flipkart.dsp.api;

import com.flipkart.dsp.actors.RequestActor;
import com.flipkart.dsp.actors.WorkFlowActor;
import com.flipkart.dsp.actors.WorkflowExecutionResponseActor;
import com.flipkart.dsp.dao.SubscriptionAuditDAO;
import com.flipkart.dsp.dao.SubscriptionAuditToRequestDAO;
import com.flipkart.dsp.db.entities.RequestEntity;
import com.flipkart.dsp.db.entities.SubscriptionAuditEntity;
import com.flipkart.dsp.db.entities.SubscriptionAuditToRequestEntity;
import com.flipkart.dsp.dto.AzkabanFlow;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.subscription.SubscriptionCallback;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exception.AzkabanProjectCreationException;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.notifier.EmailNotification;
import com.flipkart.dsp.notifier.EmailNotifier;
import com.flipkart.dsp.service.AzkabanProjectHelper;
import com.flipkart.dsp.utils.EmailBuilderUtility;
import com.flipkart.dsp.utils.EmailRecipientHelper;
import com.flipkart.dsp.utils.HiveUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.EmailException;
import org.hibernate.exception.ConstraintViolationException;

import java.util.List;
import java.util.Map;

import static com.flipkart.dsp.entities.enums.SubscriptionRunStatusEnum.SUCCESSFUL;
import static com.flipkart.dsp.models.RequestStatus.FAILED;
import static java.util.stream.Collectors.toMap;
import static org.apache.logging.log4j.util.Strings.isNotBlank;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class SubscriptionAPI {
    private final RequestAPI requestAPI;
    private final RequestActor requestActor;
    private final WorkFlowActor workFlowActor;
    private final EmailNotifier emailNotifier;
    private final EmailBuilderUtility emailBuilderUtility;
    private final AzkabanProjectHelper azkabanProjectHelper;
    private final SubscriptionAuditDAO subscriptionAuditDAO;
    private final EmailRecipientHelper emailRecipientHelper;
    private final SubscriptionAuditToRequestDAO subscriptionAuditToRequestDAO;
    private final WorkflowExecutionResponseActor workflowExecutionResponseActor;


    public void executeWorkflowBySubscription(SubscriptionCallback callback, String triggeredBy) throws Exception {
        List<WorkflowDetails> workflowDetailsList = workFlowActor.getWorkFlowDetailsBySubscriptionId(callback.getSubscriptionId());
        if (workflowDetailsList.size() == 0)
            throw new DSPCoreException("No workflow associated with subscriptionId : " + callback.getSubscriptionId());

        SubscriptionAuditEntity audit = createSubscriptionAudit(callback);
        Map<String, Long> tables = callback.getTablesPartitions().entrySet().stream()
                .collect(toMap(e -> HiveUtils.getTableNameWithOutDBPrefix(e.getKey()), Map.Entry::getValue));
        callback.setTablesPartitions(tables);

        for (WorkflowDetails workflowDetails : workflowDetailsList) {
            Workflow workflow = workflowDetails.getWorkflow();
            if (!SUCCESSFUL.equals(callback.getSubscriptionRunStatus())) {
                String failureMessage = "Subscription failed !!! Subscription Run ID : " + callback.getSubscriptionRunId();
                notifyFailure(workflowDetails, failureMessage, null);
                continue;
            }
            ExecuteWorkflowRequest executeWorkflowRequest = ExecuteWorkflowRequest.builder().requestId(callback.getSubscriptionRunId())
                    .workflowName(workflow.getName()).workflowVersion(workflow.getVersion()).isProd(workflow.getIsProd())
                    .tables(callback.getTablesPartitions()).build();

            RequestEntity requestEntity = null;
            try {
                AzkabanFlow azkabanFlow = azkabanProjectHelper.setupAzkabanJob(workflowDetails);
                requestEntity = requestAPI.createRequest(triggeredBy, workflowDetails, executeWorkflowRequest);
                requestAPI.triggerAzkabanJob(requestEntity, azkabanFlow);
                subscriptionAuditToRequestDAO.persist(SubscriptionAuditToRequestEntity.builder()
                        .requestEntity(requestEntity).subscriptionAuditEntity(audit).build());
            } catch (AzkabanException | AzkabanProjectCreationException e) {
                log.error("Error while running Workflow for " + executeWorkflowRequest.getWorkflowName(), e);
                notifyFailure(workflowDetails, "Internal Server Error while running WorkflowEntity Group.Error partitionDetails " + e.getMessage(),
                        requestActor.wrap(requestEntity));
                throw new Exception(e.getMessage());
            }
        }
    }

    private SubscriptionAuditEntity createSubscriptionAudit(SubscriptionCallback callback) {
        SubscriptionAuditEntity audit = SubscriptionAuditEntity.builder().runId(callback.getSubscriptionRunId())
                .status(callback.getSubscriptionRunStatus().toString()).subscriptionId(callback.getSubscriptionId()).build();
        try {
            subscriptionAuditDAO.persist(audit);
        } catch (ConstraintViolationException e) {
            throw new IllegalArgumentException("Subscription Audit exists for given runId : " + callback.getSubscriptionRunId(), e);
        }
        return audit;
    }

    private void notifyFailure(WorkflowDetails workflowDetails, String message, Request request) {
        String emailRecipients = emailRecipientHelper.getEmailRecipientsForWorkflowStateNotifications(workflowDetails.getWorkflow(),
                FAILED, null);
        if (isNotBlank(emailRecipients)) {
            Object workflowExecutionPayload = workflowExecutionResponseActor.getWorkflowExecutionPayload(request, request.getRequestStatus(), workflowDetails);
            EmailNotification emailNotification = emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request,
                    workflowExecutionPayload, null, message, workflowDetails.getWorkflow().getName(), emailRecipients, FAILED);
            try {
                emailNotifier.notify(emailNotification);
            } catch (EmailException e) {
                throw new IllegalStateException("Exception occurred while notifying through email", e);
            }
        }
    }
}
