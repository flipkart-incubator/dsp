package com.flipkart.dsp.executor.helper;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.misc.Resources;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.flipkart.dsp.models.EventLevel;
import com.flipkart.dsp.models.EventType;
import com.flipkart.dsp.models.event_audits.EventAudit;
import com.flipkart.dsp.models.event_audits.event_type.wf_node.*;
import com.flipkart.dsp.utils.MesosMemoryUsageUtils;
import com.flipkart.dsp.utils.PartitionScopeUtil;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.sql.Timestamp;
import java.util.*;

/**
 * +
 */
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class EventAuditHelper {

    private final DSPServiceClient dspServiceClient;
    private final PartitionScopeUtil partitionScopeUtil;
    private final MesosMemoryUsageUtils mesosMemoryUsageUtils;

    public void createWFContainerStartInfoEvent(String workflowName, ConfigPayload configPayload, PipelineStepAudit pipelineStepAudit,
                                                Map<Long /**pipelineStepAuditId*/, Integer /**log attempt*/> logAttemptMapping) {
        WFContainerStartedInfoEvent wfContainerStartedInfoEvent = WFContainerStartedInfoEvent.builder().workflowName(workflowName)
                .logAttemptMap(logAttemptMapping).pipelineStepID(configPayload.getPipelineStepId())
                .scope(partitionScopeUtil.populateJobName(configPayload.getScope()))
                .submissionTime(new Timestamp(new Date().getTime()))
                .attemptToMemoryDashboardMapping(getAttemptToMemoryDashboardMapping(pipelineStepAudit, configPayload, logAttemptMapping))
                .build();
        dspServiceClient.saveEventAudit(EventAudit.builder().requestId(configPayload.getRefreshId())
                .workflowId(configPayload.getWorkflowId()).eventLevel(EventLevel.INFO).eventType(EventType.WorkflowNode)
                .payload(wfContainerStartedInfoEvent).build());
    }

    public void createWFContainerCompletedInfoEvent(String workflowName, Resources resources, Map<Long, Integer> logAttemptMapping,
                                                    ConfigPayload configPayload, PipelineStepAudit pipelineStepAudit) {
        ContainerDetails containerDetails = ContainerDetails.builder().workflowName(workflowName)
                .pipelineStepId(configPayload.getPipelineStepId()).submissionTime(new Timestamp(new Date().getTime()))
                .logAttemptMap(logAttemptMapping).cpu(resources.getCpus()).memory(resources.getMemory())
                .scope(partitionScopeUtil.populateJobName(configPayload.getScope()))
                .currentAttempt(pipelineStepAudit.getAttempt()).build();
        WFContainerCompletedInfoEvent wfContainerCompletedInfoEvent = WFContainerCompletedInfoEvent.builder()
                .containerDetails(containerDetails)
                .attemptToMemoryDashboardMapping(getAttemptToMemoryDashboardMapping(pipelineStepAudit, configPayload, logAttemptMapping))
                .build();

        dspServiceClient.saveEventAudit(EventAudit.builder().requestId(configPayload.getRefreshId()).workflowId(configPayload.getWorkflowId())
                .eventLevel(EventLevel.INFO).eventType(EventType.WorkflowNode).payload(wfContainerCompletedInfoEvent).build());
    }

    public void createWFContainerFailedEvent(String workflowName, Resources resources, Map<Long, Integer> logAttemptMapping,
                                             ConfigPayload configPayload, String errorMessage, PipelineStepAudit pipelineStepAudit) {
        ContainerDetails containerDetails = ContainerDetails.builder().workflowName(workflowName)
                .pipelineStepId(configPayload.getPipelineStepId()).submissionTime(new Timestamp(new Date().getTime()))
                .logAttemptMap(logAttemptMapping).cpu(resources.getCpus()).memory(resources.getMemory())
                .scope(partitionScopeUtil.populateJobName(configPayload.getScope()))
                .currentAttempt(pipelineStepAudit.getAttempt()).failureMessage(errorMessage).build();
        WFContainerFailedEvent wfContainerFailedEvent = WFContainerFailedEvent
                .builder().containerDetails(containerDetails)
                .attemptToMemoryDashboardMapping(getAttemptToMemoryDashboardMapping(pipelineStepAudit, configPayload, logAttemptMapping))
                .build();

        dspServiceClient.saveEventAudit(EventAudit.builder().requestId(configPayload.getRefreshId()).workflowId(configPayload.getWorkflowId())
                .eventLevel(EventLevel.ERROR).eventType(EventType.WorkflowNode).payload(wfContainerFailedEvent).build());
    }

    public void createWFContainerStartedDebugEvent(String workflowName, ConfigPayload configPayload, PipelineStepAudit pipelineStepAudit,
                                                   Map<Long /**pipelineStepAuditId*/, Integer /**log attempt*/> logAttemptMapping) {
        Map<String /** dataframeName */, String /** hadoopcluster2 location of data 1 level above*/> dataframehadoopclusterLocation = new HashMap<>();
        WFContainerStartedDebugEvent wfContainerStartedDebugEvent = WFContainerStartedDebugEvent.builder().logAttemptMap(logAttemptMapping)
                .pipelineStepId(configPayload.getPipelineStepId()).scope(partitionScopeUtil.populateJobName(configPayload.getScope()))
                .submissionTime(new Timestamp(new Date().getTime())).workflowName(workflowName).inputDetails(dataframehadoopclusterLocation)
                .attemptToMemoryDashboardMapping(getAttemptToMemoryDashboardMapping(pipelineStepAudit, configPayload, logAttemptMapping))
                .build();
        dspServiceClient.saveEventAudit(EventAudit.builder().requestId(configPayload.getRefreshId())
                .workflowId(configPayload.getWorkflowId()).eventLevel(EventLevel.DEBUG).eventType(EventType.WorkflowNode)
                .payload(wfContainerStartedDebugEvent).build());
    }

    private LinkedHashMap<Integer, String> getAttemptToMemoryDashboardMapping(PipelineStepAudit pipelineStepAudit,
                                                                              ConfigPayload configPayload,
                                                                              Map<Long, Integer> logAttemptMapping) {
        Map<String, Set<String>> partitionDetails = mesosMemoryUsageUtils.getPartitionDetails(configPayload.getScope());
        LinkedHashMap<Integer, String> attemptToMemoryDashboardMapping = new LinkedHashMap<>();
        logAttemptMapping.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .forEachOrdered(x -> {
                    String usageDashboard = mesosMemoryUsageUtils.getMemoryUsageDashboard(x.getValue(), pipelineStepAudit, partitionDetails);
                    attemptToMemoryDashboardMapping.put(x.getValue(), usageDashboard);
                });
        return attemptToMemoryDashboardMapping;
    }

}
