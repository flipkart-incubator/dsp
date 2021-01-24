package com.flipkart.dsp.jobs;

import com.flipkart.dsp.actors.DataFrameOverrideAuditActor;
import com.flipkart.dsp.actors.NotificationActor;
import com.flipkart.dsp.actors.RequestActor;
import com.flipkart.dsp.azkaban.AzkabanJobStatusResponse;
import com.flipkart.dsp.azkaban.AzkabanLoginResponse;
import com.flipkart.dsp.client.AzkabanClient;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.config.AzkabanConfig;
import com.flipkart.dsp.db.entities.DataFrameOverrideAuditEntity;
import com.flipkart.dsp.entities.enums.AzkabanJobStatus;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideState;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowMeta;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.exceptions.CallbackException;
import com.flipkart.dsp.exceptions.RequestException;
import com.flipkart.dsp.models.EventType;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.EventAuditUtil;
import com.flipkart.dsp.utils.NodeMetaData;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.EmailException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class AzkabanNotifierNode implements SessionfulApplication {

    private String sessionId;

    private final RequestActor requestActor;
    private final AzkabanClient azkabanClient;
    private final AzkabanConfig azkabanConfig;
    private final EventAuditUtil eventAuditUtil;
    private final DSPServiceClient dspServiceClient;
    private final NotificationActor notificationActor;
    private final DataFrameOverrideAuditActor dataFrameOverrideAuditActor;

    @Inject
    public AzkabanNotifierNode(RequestActor requestActor,
                               AzkabanConfig azkabanConfig,
                               AzkabanClient azkabanClient,
                               EventAuditUtil eventAuditUtil,
                               DSPServiceClient dspServiceClient,
                               NotificationActor notificationActor,
                               DataFrameOverrideAuditActor dataFrameOverrideAuditActor) {
        this.requestActor = requestActor;
        this.azkabanClient = azkabanClient;
        this.azkabanConfig = azkabanConfig;
        this.eventAuditUtil = eventAuditUtil;
        this.dspServiceClient = dspServiceClient;
        this.notificationActor = notificationActor;
        this.dataFrameOverrideAuditActor = dataFrameOverrideAuditActor;
    }

    @Override
    public String getName() {
        return Constants.NOTIFIER_NODE;
    }

    @Override
    public NodeMetaData execute(String[] args) throws AzkabanException {
        try {
            poll();
        } catch (Exception e) {
            throw new AzkabanException("Azkaban node failed: ", e);
        }
        return null;
    }

    private void poll() throws Exception {
        if (null == sessionId) {
            setSessionId();
        }
        try {

//          Get all requests which are ACTIVE(in batches of maxSize), ACTIVE requests can be FAILED or RUNNING at that time
            Integer maxSize = 20;
            Set<Long> exceptionIdSet = new HashSet<>();
            List<Request> activeRequests = requestActor.getActiveRequests(RequestStatus.ACTIVE, maxSize);
            log.info("Active requests count is: {}", activeRequests.size());

            while (activeRequests.size() > exceptionIdSet.size()) {

//              Notification for any Workflow group can be marked inactive by making its warning or kill time: -ve
                for (Request request : activeRequests) {
                    Workflow workflow = request.getWorkflowDetails().getWorkflow();
                    if (((workflow.getWorkflowMeta().getKillTimeForNotification()) > 0
                            && (workflow.getWorkflowMeta().getWarningTimeForNotification() > 0))) {
                        try {
                            processRequest(request);
                        } catch (Exception e) {
                            exceptionIdSet.add(request.getId());
                            log.info("failed to run notifier for request with id: " + request.getId() + " due to error: " + e);
                        }
                    } else {
                        exceptionIdSet.add(request.getId());
                        log.info("notification has been marked inactive for request with id: " + request.getId());
                    }
                }
                activeRequests = requestActor.getActiveRequests(RequestStatus.ACTIVE, maxSize);
                log.info("Active requests count is: {}", activeRequests.size());
                log.info("exception count is: {}", exceptionIdSet.size());

            }
        } finally {
            azkabanClient.close();
            dspServiceClient.close();
        }
    }

    private void processRequest(Request request) throws Exception {
        AzkabanJobStatusResponse azkabanJobStatusResponse = azkabanClient.getAzkabanJobStatus(sessionId, request.getAzkabanExecId());
//                      As azkaban session id expires in every 24 hours, thus need to refresh it
        if (null == azkabanJobStatusResponse.getProject()) {
            refreshSessionId();
        }
        AzkabanJobStatus azkabanJobStatus = azkabanJobStatusResponse.getStatus();
        if (null != azkabanJobStatus) {
            if (AzkabanJobStatus.FAILED.equals(azkabanJobStatus)) {  // If failed
                postProcessOverrideAudits(request);
                notificationActor.notifyExecutionStatus(RequestStatus.FAILED, request.getAzkabanExecId(), " is failed.", request.getId());
            } else if (AzkabanJobStatus.KILLED.equals(azkabanJobStatus)) { // if killed by UI
                eventAuditUtil.makeFlowTerminatingEntry("Azkaban Job Killed from UI", request.getId(),
                        request.getWorkflowId(), EventType.NotifierNode, true);
                postProcessOverrideAudits(request);
                notificationActor.notifyExecutionStatus(RequestStatus.FAILED, request.getAzkabanExecId(), " is killed from UI.", request.getId());
            } else if (AzkabanJobStatus.FAILED_FINISHING.equals(azkabanJobStatus)) { // If running with failure
                // send mail only
                notificationActor.notifyExecutionStatus(RequestStatus.ACTIVE, request.getAzkabanExecId(), " is running with failure.", request.getId());
            } else if (AzkabanJobStatus.RUNNING.equals(azkabanJobStatus)) {
                handleRunningRequests(azkabanJobStatusResponse, request);
            } else {
                throw new IllegalArgumentException("The azkaban job status: " + azkabanJobStatus + " is not supported.");
            }
        }
    }


    private void postProcessOverrideAudits(Request request) {
        List<DataFrameOverrideAuditEntity> dataFrameOverrideAuditEntities = dataFrameOverrideAuditActor.getByRequestId(request.getId());
        dataFrameOverrideAuditEntities.forEach(dataframeOverrideAuditEntity -> {
            if (dataframeOverrideAuditEntity.getState().equals(DataFrameOverrideState.STARTED)) {
                dataframeOverrideAuditEntity.setState(DataFrameOverrideState.FAILED);
            }
        });
        dataFrameOverrideAuditActor.save(dataFrameOverrideAuditEntities);
    }


    //      If crossed warning time and not notified then: send callback email and update request's isNotified field
    //      If crossed warning time and already notified then: don't do anything
    //      If crossed kill time then: kill azkaban DAG programmatically and send callback and email
    private void handleRunningRequests(AzkabanJobStatusResponse azkabanJobStatusResponse, Request request) throws EmailException, CallbackException, RequestException {
        WorkflowMeta workflowMeta = request.getWorkflowDetails().getWorkflow().getWorkflowMeta();
        Long timeElapsed = System.currentTimeMillis() - azkabanJobStatusResponse.getStartTime();
        if (!request.getIsNotified() && timeElapsed >= workflowMeta.getWarningTimeForNotification()) {
            notificationActor.notifyExecutionStatus(RequestStatus.ACTIVE, request.getAzkabanExecId(),
                    " has crossed warning time.", request.getId());
        } else if (timeElapsed >= workflowMeta.getKillTimeForNotification()) {  // kill azkaban DAG
            killAzkabanJob(request.getAzkabanExecId());
            postProcessOverrideAudits(request);
            requestActor.updateRequestStatus(request, RequestStatus.FAILED);
            notificationActor.notifyExecutionStatus(RequestStatus.FAILED, request.getAzkabanExecId(),
                    " is killed because of timed out.", request.getId());
        } else {
            throw new RequestException("Request with id :" + request.getId() + " has been submitted recently, no action required.");
        }
    }

    private void killAzkabanJob(Long azkabanExecId) {
        try {
            azkabanClient.killAzkabanJob(sessionId, azkabanExecId);
        } catch (Exception e) {
            throw new CallbackException("Failed to kill Azkaban Job: for execId: " + azkabanExecId, e);
        }
        log.info("Azkaban job killed with exec Id: " + azkabanExecId);

    }

    private void setSessionId() {
        this.sessionId = null;
        try {
            AzkabanLoginResponse azkabanLoginResponse = azkabanClient.getAzkabanLoginRequest().azkabanConfig(azkabanConfig).executeSync();
            this.sessionId = azkabanLoginResponse.getSessionId();
        } catch (Exception e) {
            log.error("exception in getting azkaban session id", e);
        }
    }

    private void refreshSessionId() {
        setSessionId();
    }
}
