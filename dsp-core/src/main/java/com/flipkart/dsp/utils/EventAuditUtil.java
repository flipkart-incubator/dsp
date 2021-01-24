package com.flipkart.dsp.utils;

import com.flipkart.dsp.actors.EventAuditActor;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.*;
import com.flipkart.dsp.models.event_audits.EventAudit;
import com.flipkart.dsp.models.event_audits.Events;
import com.flipkart.dsp.models.event_audits.event_type.*;
import com.flipkart.dsp.models.event_audits.event_type.output_ingestion.OutputIngestionErrorEvent;
import com.flipkart.dsp.models.event_audits.event_type.output_ingestion.ceph_ingestion_node.*;
import com.flipkart.dsp.models.event_audits.event_type.terminal_node.*;
import com.flipkart.dsp.models.event_audits.event_type.wf_node.*;
import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.sql.Timestamp;
import java.util.*;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class EventAuditUtil {
    private final EventAuditActor eventAuditsActor;

    private void persistEventAudit(Long requestId, Long workflowId, Events event, EventType eventType, EventLevel eventLevel) {
        if (requestId != 0L) {
            EventAudit eventAudit = EventAudit.builder().requestId(requestId).workflowId(workflowId).eventLevel(eventLevel)
                    .eventType(eventType).payload(event).build();
            eventAuditsActor.saveEvent(eventAudit);
        }
    }

    public void makeFailedTerminationEventAuditEntry(Long requestId, Workflow workflow, int azkabanCurrentRetryAttempt,
                                                     int azkabanMaxRetry, String errorMessage, String eventType) {
        if (azkabanCurrentRetryAttempt == azkabanMaxRetry)
            makeFlowTerminatingEntry(errorMessage, requestId, workflow.getId(), EventType.valueOf(eventType), true);
    }

    public void createCephIngestionStartInfoEvent(Long requestId, Long workflowId,
                                                 String dataFrameName, CephOutputLocation cephOutputLocation) {
        CephIngestionStartInfoEvent cephIngestionStartInfoEvent = CephIngestionStartInfoEvent.builder()
                .dataFrameName(dataFrameName).cephOutputLocation(cephOutputLocation).build();
        persistEventAudit(requestId, workflowId, cephIngestionStartInfoEvent, EventType.CephIngestion, EventLevel.INFO);
    }

    public void createCephIngestionErrorEvent(Long requestId, Long workflowId, String dataFrameName,
                                             String errorMessage, CephOutputLocation cephOutputLocation) {
        CephIngestionErrorEvent cephIngestionErrorEvent = CephIngestionErrorEvent.builder()
                .dataFrameName(dataFrameName).message(errorMessage).cephOutputLocation(cephOutputLocation).build();
        persistEventAudit(requestId, workflowId, cephIngestionErrorEvent, EventType.CephIngestion, EventLevel.ERROR);
    }

    public void createCephIngestionEndInfoEvent(Long requestId, Long workflowId, String dataFrameName, List<URL> urls) {
        CephIngestionEndInfoEvent cephIngestionEndInfoEvent = CephIngestionEndInfoEvent.builder()
                .dataFrameName(dataFrameName).urls(urls).build();
        persistEventAudit(requestId, workflowId, cephIngestionEndInfoEvent, EventType.CephIngestion, EventLevel.INFO);
    }


    public void creatOutputIngestionErrorEvent(Long requestId, Long workflowId, String workflowName, String errorMessage) {
        OutputIngestionErrorEvent outputIngestionErrorEvent = OutputIngestionErrorEvent.builder().workflowName(workflowName).
                errorMessage(errorMessage).build();
        persistEventAudit(requestId, workflowId, outputIngestionErrorEvent, EventType.OutputIngestionNode, EventLevel.ERROR);
    }


    public void createWFEndInfoEvent(Long requestId, Long workflowId, String workflowName,
                                     List<String> hiveTables) {
        WFEndInfoEvent wfEndInfoEvent = WFEndInfoEvent.builder().workflowName(workflowName).refreshId(requestId)
                .outputHiveTables(hiveTables).build();
        persistEventAudit(requestId, workflowId, wfEndInfoEvent, EventType.WorkflowNode, EventLevel.INFO);
    }

    public void createWFStartInfoEvent(Long requestId, Long workflowId, String workflowName) {
        WFStartInfoEvent wfStartInfoEvent = WFStartInfoEvent.builder().workflowName(workflowName).build();
        persistEventAudit(requestId, workflowId, wfStartInfoEvent, EventType.WorkflowNode, EventLevel.INFO);
    }

    public void createWFErrorEvent(Long requestId, Long workflowId, String workflowName, String errorMessage) {
        WFErrorEvent wfErrorEvent = WFErrorEvent.builder().workflowName(workflowName).message(errorMessage).build();
        persistEventAudit(requestId, workflowId, wfErrorEvent, EventType.WorkflowNode, EventLevel.ERROR);
    }

    public void makeFlowTerminatingEntry(String message, Long requestId, Long workflowId, EventType eventType, boolean failureState) {
        FlowTerminationSignal flowTerminationStatus = new FlowTerminationSignal(failureState, message);
        EventLevel eventLevel = failureState ? EventLevel.ERROR : EventLevel.DEBUG;
        persistEventAudit(requestId, workflowId, flowTerminationStatus, eventType, eventLevel);
    }

    public void createWFSubmittedDebugEvent(Long requestId, Long workflowId, String workflowName,
                                            Timestamp currentTime, Long pipelineStepId, Double cpu, Long memory) {
        WFSubmittedDebugEvent wfSubmittedDebugEvent = WFSubmittedDebugEvent.builder().workflowName(workflowName)
                .pipelineStepAuditId(pipelineStepId).submissionTime(currentTime).baseMemory(memory).baseCPU(cpu).build();
        persistEventAudit(requestId, workflowId, wfSubmittedDebugEvent, EventType.WorkflowNode, EventLevel.DEBUG);
    }

    private void createTerminalNodeDebugEvent(Long requestId, Long workflowId, String workflowName, Timestamp currentTime,
                                              String callbackUrl, String varadhiQueue, String payload, String varadhiResponse) {
        TerminalNodeDebugEvent terminalNodeDebugEvent = TerminalNodeDebugEvent.builder().workflowName(workflowName)
                .callbackUrl(callbackUrl).alertTriggerTime(currentTime).varadhiQueue(varadhiQueue)
                .payload(payload).varadhiResponse(varadhiResponse).build();
        persistEventAudit(requestId, workflowId, terminalNodeDebugEvent, EventType.TerminalNode, EventLevel.DEBUG);
    }

    private void createTerminalNodeInfoEvent(Long requestId, Long workflowId, String workflowName,
                                             Timestamp currentTime, String callbackUrl, String varadhiQueue) {
        TerminalNodeInfoEvent terminalNodeInfoEvent = TerminalNodeInfoEvent.builder().workflowName(workflowName)
                .callbackUrl(callbackUrl).alertTriggerTime(currentTime).varadhiQueue(varadhiQueue).build();
        persistEventAudit(requestId, workflowId, terminalNodeInfoEvent, EventType.TerminalNode, EventLevel.INFO);
    }

    public void createTerminalNodeErrorEvent(Long requestId, Long workflowId, String workflowName, String errorMessage) {
        TerminalNodeErrorEvent terminalNodeErrorEvent = TerminalNodeErrorEvent.builder()
                .workflowName(workflowName).errorMessage(errorMessage).build();
        persistEventAudit(requestId, workflowId, terminalNodeErrorEvent, EventType.TerminalNode, EventLevel.ERROR);
    }

    public LinkedHashMap<Integer, String> populateAbsoluteLogUrl(Map<Long /**pipelineStepAuditId */, Integer /**log attempt*/> logAttemptMap, String logPrefixPath) {
        LinkedHashMap<Integer, String> attemptLogUrlMapping = new LinkedHashMap<>();
        logAttemptMap.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .forEachOrdered(x -> attemptLogUrlMapping.put(x.getValue(), logPrefixPath + x.getKey() + "/log"));
        return attemptLogUrlMapping;
    }

    public void makeFlowStartInfoEventEntry(String version, Long requestId, String azkabanUrl, WorkflowDetails workflowDetails) {
        Map<String, ScriptDetails> scriptDetailsList = getPipelineStepLevelScriptDetails(workflowDetails);
        FlowStartInfoEvent flowStartInfoEvent = FlowStartInfoEvent.builder().workflowName(workflowDetails.getWorkflow().getName())
                .version(version).azkabanUrl(azkabanUrl).requestId(requestId).pipelineStepScriptDetails(scriptDetailsList)
                .mesosQueue(workflowDetails.getWorkflow().getWorkflowMeta().getMesosQueue())
                .build();

        persistEventAudit(requestId, workflowDetails.getWorkflow().getId(), flowStartInfoEvent, EventType.Service, EventLevel.INFO);
    }

    private Map<String, ScriptDetails> getPipelineStepLevelScriptDetails(WorkflowDetails workflowDetails) {
        Map<String, ScriptDetails> scriptDetailsMap = new HashMap<>();
        workflowDetails.getPipelineSteps().forEach(pipelineStep -> {
            ScriptDetails scriptDetail = ScriptDetails.builder().pipelineStepId(pipelineStep.getId())
                    .fileName(pipelineStep.getScript().getFilePath()).scriptRepoMeta(ScriptRepoMeta.builder()
                            .gitRepo(pipelineStep.getScript().getGitRepo()).gitCommitId(pipelineStep.getScript().getGitCommitId()).build()).build();
            if (Objects.nonNull(pipelineStep.getName()))
                scriptDetailsMap.put(pipelineStep.getName(), scriptDetail);
        });
        return scriptDetailsMap;
    }

    public String getSgLogUrl(String auditId, String logPrefixPath) {
        return logPrefixPath + "sg/" + auditId + "/log?log-type=stdout";
    }
}
