package com.flipkart.dsp.jobs;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.cosmos.AzkabanCosmosTag;
import com.flipkart.dsp.cosmos.CosmosReporter;
import com.flipkart.dsp.entities.enums.RequestStepAuditStatus;
import com.flipkart.dsp.entities.enums.RequestStepType;
import com.flipkart.dsp.entities.request.*;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.utils.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import static com.flipkart.dsp.utils.Constants.*;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public abstract class AbstractAzkabanNode implements SessionfulApplication {

    private RequestActor requestActor;
    protected ObjectMapper objectMapper;
    private WorkFlowActor workFlowActor;
    private CosmosReporter cosmosReporter;
    protected EventAuditUtil eventAuditUtil;
    private RequestStepActor requestStepActor;
    protected DSPServiceClient dspServiceClient;
    private RequestStepAuditActor requestStepAuditActor;
    private static final int INDEX_ZERO = 0;

    AbstractAzkabanNode(RequestActor requestActor,
                        ObjectMapper objectMapper,
                        WorkFlowActor workFlowActor,
                        CosmosReporter cosmosReporter,
                        EventAuditUtil eventAuditUtil,
                        DSPServiceClient dspServiceClient,
                        RequestStepActor requestStepActor,
                        RequestStepAuditActor requestStepAuditActor) {
        this.requestActor = requestActor;
        this.objectMapper = objectMapper;
        this.workFlowActor = workFlowActor;
        this.cosmosReporter = cosmosReporter;
        this.eventAuditUtil = eventAuditUtil;
        this.dspServiceClient = dspServiceClient;
        this.requestStepActor = requestStepActor;
        this.requestStepAuditActor = requestStepAuditActor;
    }

    @Override
    @Metered
    @Timed
    public NodeMetaData execute(String[] args) throws AzkabanException {
        Long requestId = null;
        Workflow workflow = null;
        int azkabanCurrentRetryAttempt = 0;
        int azkabanMaxRetry = 0;
        try {
            Properties properties = new Properties();
            properties.load(new StringReader(args[INDEX_ZERO]));
            String dynamicArgsString = properties.getProperty(APPLICATION_CLASS_DYNAMIC_ARGS);
            azkabanCurrentRetryAttempt = Integer.parseInt(properties.getProperty(AZKABAN_CURRENT_RETRY_ATTEMPT));
            String maxRetries = properties.getProperty(AZKABAN_MAX_RETRIES);
            if (maxRetries != null) {
                azkabanMaxRetry = Integer.parseInt(maxRetries);
            }

            NodeMetaData nodeMetaData = JsonUtils.DEFAULT.fromJson(dynamicArgsString, NodeMetaData.class);
            checkArgument(!nodeMetaData.getRequestId().equals(0L), "Request id is null");
            requestId = nodeMetaData.getRequestId();
            WorkflowDetails workflowDetails = getWorkflowDetails(properties);
            workflow = workflowDetails.getWorkflow();
            setClientQueue();
            String azkabanCurrentNodeID = properties.getProperty(Constants.AZKABAN_JOB_ID);
            String inNodes = properties.getProperty(Constants.AZKABAN_IN_NODES);
            checkArgument(!requestId.equals(0L), "Request id is null");
            setActualDynamicArgs(inNodes, requestId, nodeMetaData);
            RequestStep requestStep = requestStepActor.createRequestStep(requestId, getRequestStepType(), azkabanCurrentNodeID);
            requestStepAuditActor.createRequestStepAudit(requestStep, RequestStepAuditStatus.STARTED, null);
            AzkabanCosmosTag.populateValue(workflowDetails.getWorkflow().getName(), nodeMetaData.getRequestId(), azkabanCurrentNodeID);
            performAction(requestStep.getId(), nodeMetaData, workflowDetails);
            String nodeMetadataStr = objectMapper.writeValueAsString(nodeMetaData);
            requestStepAuditActor.createRequestStepAudit(requestStep, RequestStepAuditStatus.SUCCESSFUL, nodeMetadataStr);
            return nodeMetaData;
        } catch (JsonProcessingException e) {
            String errorMessage = "Could not serialise dynamic args." + e.getMessage() + " " + e.toString();
            assert workflow != null;
            eventAuditUtil.makeFailedTerminationEventAuditEntry(requestId, workflow, azkabanCurrentRetryAttempt, azkabanMaxRetry, errorMessage, getName());
            throw new AzkabanException("Could not serialise dynamic args", e);
        } catch (IOException e) {
            String errorMessage = "Error occurred while loading Azkaban property." + e.getMessage() + " " + e.toString();
            eventAuditUtil.makeFailedTerminationEventAuditEntry(requestId, workflow, azkabanCurrentRetryAttempt, azkabanMaxRetry, errorMessage, getName());
            throw new AzkabanException("Error occurred while loading Azkaban property", e);
        } catch (Exception e) {
            String errorMessage = "Azkaban node failed because of following reason." + e.getMessage() + " " + e.toString();
            eventAuditUtil.makeFailedTerminationEventAuditEntry(requestId, workflow, azkabanCurrentRetryAttempt, azkabanMaxRetry, errorMessage, getName());
            throw new AzkabanException("Azkaban node failed because of following reason:", e);
        } finally {
            cosmosReporter.forceFlush();
            dspServiceClient.close();
        }
    }

    protected Request getRequest(Long requestId) throws AzkabanException {
        final Request request = requestActor.getRequest(requestId);
        if (request == null) {
            log.error("Request not found in database. Are you sure service and Azkaban are pointing to same database?");
            throw new AzkabanException("Request not found in database with id " + requestId + " !! ");
        }
        return request;
    }

    private WorkflowDetails getWorkflowDetails(Properties properties) {
        Long workflowId = Long.valueOf(properties.getProperty(Constants.APPLICATION_CLASS_ARGS));
        checkNotNull(workflowId);
        return workFlowActor.getWorkflowDetailsById(workflowId);
    }

    @Timed
    @Metered
    protected abstract void performAction(Long requestStepId, NodeMetaData nodeMetaData, WorkflowDetails workflowDetails) throws AzkabanException;

    protected abstract RequestStepType getRequestStepType();

    protected abstract void setClientQueue();

    private void setActualDynamicArgs(String inNodes, Long requestId, NodeMetaData nodeMetaData) {
        log.info("inNodes: {}", inNodes);

        //multiple parentNode/inNode
        HashSet<String> allDagEntitiesSet = new HashSet<>();

        if (!inNodes.isEmpty()) {

            String[] inNodeList = inNodes.split("\\s*,\\s*");

            for (String inNode : inNodeList) {

                nodeMetaData = getRequestStepAuditMetaData(inNode, requestId);

                if (nodeMetaData.getDagEntities() != null) {
                    allDagEntitiesSet.addAll(nodeMetaData.getDagEntities());
                    log.debug("allDynamicArgs {}", nodeMetaData.getDagEntities());
                }
            }
            List<String> allDagEntitesList = new ArrayList<>(allDagEntitiesSet);
            nodeMetaData.setDagEntities(allDagEntitesList);
        }
    }

    private NodeMetaData getRequestStepAuditMetaData(String inNode, Long requestId) {
        RequestStepAudit requestStepAudit = requestStepAuditActor.getRequestStepAuditByJobName(requestId, inNode);
        if (Objects.nonNull(requestStepAudit)) {
            if (Objects.nonNull(requestStepAudit.getMetaData())) {
                try {
                    return objectMapper.readValue(requestStepAudit.getMetaData(), NodeMetaData.class);
                } catch (IOException e) {
                    throw new IllegalStateException("Node meta data could not be parsed");
                }
            }
            throw new IllegalStateException("Meta data is null for requestStepAudit Id " + requestStepAudit.getId());
        }
        throw new IllegalStateException("No RequestStepAudit found for request_id: " + requestId);
    }

}
