package com.flipkart.dsp.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.config.*;
import com.flipkart.dsp.entities.enums.RequestStepAuditStatus;
import com.flipkart.dsp.entities.enums.RequestStepType;
import com.flipkart.dsp.entities.misc.WhereClause;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.request.RequestStepAudit;
import com.flipkart.dsp.entities.workflow.DSPWorkflowExecutionResult;
import com.flipkart.dsp.models.*;
import com.flipkart.dsp.models.callback.*;
import com.flipkart.dsp.models.enums.WorkflowStateNotificationType;
import com.flipkart.dsp.models.enums.WorkflowStepStateNotificationType;
import com.flipkart.dsp.models.event_audits.EventAudit;
import com.flipkart.dsp.models.event_audits.event_type.output_ingestion.ceph_ingestion_node.CephIngestionEndInfoEvent;
import com.flipkart.dsp.models.event_audits.event_type.output_ingestion.ceph_ingestion_node.CephIngestionErrorEvent;
import com.flipkart.dsp.models.misc.PartitionDetailsEmailNotificationRequest;
import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
import com.flipkart.dsp.notifier.EmailNotification;
import com.google.inject.Inject;
import j2html.TagCreator;
import j2html.tags.ContainerTag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.Strings;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.flipkart.dsp.utils.Constants.*;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.groupingBy;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class EmailBuilderUtility {
    private static final String COMMA = ",";
    private final MiscConfig miscConfig;
    private final AzkabanConfig azkabanConfig;
    private final DSPClientConfig dspClientConfig;
    private final EventAuditActor eventAuditsActor;
    private final RequestStepAuditActor requestStepAuditActor;
    private final PipelineStepAuditActor pipelineStepAuditActor;

    public EmailNotification constructWorkflowStateChangeEmailNotification(Request request, Object payload, Long azkabanExecId, String message,
                                                                           String workflowName, String recipients, RequestStatus requestStatus) {
        String mailSubject = constructSubject(request, "", message, workflowName);
        String mailBody = constructBodyForWorkflowStateChangeEmail(azkabanExecId, payload, request, requestStatus);
        return getEmailNotificationObject(recipients, mailSubject, mailBody);
    }

    private String constructBodyForWorkflowStateChangeEmail(Long azkabanExecId, Object payload, Request request, RequestStatus requestStatus) {
        ContainerTag htmlTag = getHtmlTagWithAzkabanDetails("Azkaban link: ", azkabanExecId);
        htmlTag.with(getHtmlTagWithExecutionDetails(request.getId()));
        if (RequestStatus.FAILED.equals(requestStatus))
            return getFailureRequestBody(request, htmlTag);
        else
            return getSuccessMessageBody(payload, htmlTag);
    }

    private ContainerTag getHtmlTagWithAzkabanDetails(String messagePrefix, Long azkabanExecId) {
        String azkabanLink = AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId).toString();
        ContainerTag styleTag = TagCreator.style("table, th, td {\n" + "  border: 1px solid black;\n" +
                "  border-collapse: collapse;\n" + "}");
        ContainerTag htmlTag = TagCreator.html().with(TagCreator.head().with(TagCreator.title()).with(styleTag));
        ContainerTag bodyTag = TagCreator.body().with(TagCreator.p(messagePrefix + azkabanLink));
        return htmlTag.with(bodyTag);
    }

    private ContainerTag getHtmlTagWithExecutionDetails(Long requestId) {
        String executionDetailLink = String.format(Constants.LOG_PATH_PREFIX, Constants.http,
                dspClientConfig.getHost(), dspClientConfig.getPort(), Constants.LOG_RESOURCE_PREFIX) + requestId + slash + details;
        ContainerTag styleTag = TagCreator.style("table, th, td {\n" + "  border: 1px solid black;\n" +
                "  border-collapse: collapse;\n" + "}");
        ContainerTag htmlTag = TagCreator.html().with(TagCreator.head().with(TagCreator.title()).with(styleTag));
        ContainerTag bodyTag = TagCreator.body().with(TagCreator.p("Get execution details at " + executionDetailLink));
        return htmlTag.with(bodyTag);
    }

    private String getFailureRequestBody(Request request, ContainerTag htmlTag) {
        try {
            List<RequestStepAudit> requestStepAudits = requestStepAuditActor.getRequestStepAudits(request.getId());
            Map<Long, List<RequestStepAudit>> requestStepToAuditMap = requestStepAudits.stream()
                    .collect(groupingBy(RequestStepAudit::getRequestStepId));
            // filtering the requestAudits where first few attempt was failed but finally it got successfully
            List<RequestStepAudit> failedRequestAudits = requestStepToAuditMap.entrySet().stream()
                    .filter(entry -> entry.getValue().stream().noneMatch(requestStepAudit -> requestStepAudit.getRequestStepAuditStatus().
                            equals(RequestStepAuditStatus.SUCCESSFUL)) && entry.getValue().stream()
                            .anyMatch(requestStepAudit -> requestStepAudit.getRequestStepAuditStatus().
                                    equals(RequestStepAuditStatus.FAILED))).findFirst().map(Map.Entry::getValue).orElse(new ArrayList<>());

            if (failedRequestAudits.size() != 0) {
                RequestStepAudit requestStepAudit = failedRequestAudits.get(0); // final failure can be only one requestEntity node only, there will be only one entry
                processWorkflowNodeFailure(request.getId(), requestStepAudit.getRequestStepType(), htmlTag);
                processOutputIngestionNodeFailure(request.getId(), requestStepAudit.getRequestStepType(), htmlTag);
            }
            return htmlTag.render();
        } catch (Exception e) {
            return TagCreator.html().with(TagCreator.body()
                    .with(TagCreator.p("Error while constructing email message body.Please contact dsp-oncall@flipkart.com"))).render();
        }
    }

    private void processWorkflowNodeFailure(Long requestId, RequestStepType requestStepType, ContainerTag htmlTag) throws IOException {
        if (requestStepType.equals(RequestStepType.WF)) {
            List<PipelineStepAudit> pipelineStepAudits = pipelineStepAuditActor.getPipelineStepAudits(requestId);
            Map<String, Long> successfulPartitionToAuditIdMapping = getPartitionToAuditIdMapping(pipelineStepAudits, PipelineStepStatus.SUCCESS);
            Map<String, Long> failedPartitionToAuditIdMapping = getPartitionToAuditIdMapping(pipelineStepAudits, PipelineStepStatus.FAILED);
            successfulPartitionToAuditIdMapping.forEach((partition, auditId) -> failedPartitionToAuditIdMapping.remove(partition));
            String message = "Request failed because of script Error. Please find logs for partitions:";
            ContainerTag bodyTag = TagCreator.body().with(TagCreator.p(message));
            bodyTag.with(TagCreator.p("No of successful Partitions: " + successfulPartitionToAuditIdMapping.size()));
            addLogDetails(successfulPartitionToAuditIdMapping, bodyTag);
            bodyTag.with(TagCreator.p("No of failed Partitions: " + failedPartitionToAuditIdMapping.size()));
            addLogDetails(failedPartitionToAuditIdMapping, bodyTag);
            htmlTag.with(bodyTag);
        }
    }

    private void addLogDetails(Map<String, Long> partitionToLogMapping, ContainerTag bodyTag) {
        if (partitionToLogMapping.size() == 0) return;
        ContainerTag tableTag = TagCreator.table();
        String partitionName = new ArrayList<>(partitionToLogMapping.keySet()).get(0);
        List<String> partitionString = new ArrayList<>();
        if (partitionName.equals("default_partition")) partitionString.add(partitionName);
        else partitionString = Arrays.asList(new ArrayList<>(partitionToLogMapping.keySet()).get(0).split("^"));
        ContainerTag headingTag = TagCreator.tr();
        partitionString.forEach(partition -> headingTag.with(TagCreator.th(partition.split(equal)[0])));
        headingTag.with(TagCreator.th("Log Url"));
        tableTag.with(headingTag);
        new TreeMap<>(partitionToLogMapping).forEach((partitionDetails, auditId) -> {
            ContainerTag lineItem = TagCreator.tr();
            if (partitionDetails.equals("default_partition"))
                lineItem.with(TagCreator.td(partitionDetails));
            else {
                List<String> partition = Arrays.asList(partitionDetails.split("^")); // ^ separator is used for
                partition.forEach(partition1 -> lineItem.with(TagCreator.td(partition1.split(equal)[1])));
            }
            lineItem.with(TagCreator.td(getLogUrl(auditId)));
            tableTag.with(lineItem);
        });
        bodyTag.with(tableTag);
    }

    private String getLogUrl(Long auditId) {
        return Constants.http + dspClientConfig.getHost() + colon + dspClientConfig.getPort() + Constants.LOG_RESOURCE_PREFIX
                + slash + auditId + slash + "log";
    }

    private void processOutputIngestionNodeFailure(Long requestId, RequestStepType requestStepType, ContainerTag htmlTag) {
        if (requestStepType.equals(RequestStepType.OI)) {
            String message = "Request failed while ingesting output. Script ran successfully. Please contact dsp-oncall@flipkart.com";
            ContainerTag bodyTag = TagCreator.body().with(TagCreator.p(message));
            addOutputIngestionDetails(requestId, bodyTag, true);
            addOutputIngestionDetails(requestId, bodyTag, false);
            htmlTag.with(bodyTag);
        }
    }

    private void addOutputIngestionDetails(Long requestId, ContainerTag bodyTag, boolean isIngestionSuccessful) {
        List<EventAudit> uniqueCephAudits = getUniqueCephIngestionAudits(requestId, isIngestionSuccessful);

        if (uniqueCephAudits.size() != 0) {
            String message = "Details for " + (isIngestionSuccessful ? "successful Ingestion" : "failed Ingestion:");
            bodyTag.with(TagCreator.p(message));
            ContainerTag tableTag = TagCreator.table();
            ContainerTag headingTag = TagCreator.tr().with(TagCreator.th("DataFrame Name"))
                    .with(TagCreator.th("Output Location")).with(TagCreator.th("Output Details"));
            tableTag.with(headingTag);
            addBodyForCephIngestion(tableTag, uniqueCephAudits, isIngestionSuccessful);
            bodyTag.with(tableTag);
        }
    }

    private List<EventAudit> getUniqueCephIngestionAudits(Long requestId, boolean isIngestionSuccessful) {
        EventLevel eventLevel = isIngestionSuccessful ? EventLevel.INFO : EventLevel.ERROR;
        List<EventAudit> cephIngestionAudits = eventAuditsActor.getEvents(requestId, eventLevel, EventType.CephIngestion);
        if (isIngestionSuccessful)
            cephIngestionAudits = cephIngestionAudits.stream().filter(eventAudit ->
                    eventAudit.getPayload() instanceof CephIngestionEndInfoEvent).collect(Collectors.toList());

        List<EventAudit> uniqueEventAudits = new ArrayList<>();
        cephIngestionAudits.forEach(eventAudit -> {
            boolean isSimilar = uniqueEventAudits.stream().anyMatch(eventAudit1 -> compareSuccessfulCephDetails(eventAudit, eventAudit1, isIngestionSuccessful)
                    && compareFailedCephDetails(eventAudit, eventAudit1, isIngestionSuccessful));
            if (!isSimilar)
                uniqueEventAudits.add(eventAudit);
        });
        return uniqueEventAudits;
    }

    private boolean compareSuccessfulCephDetails(EventAudit eventAudit, EventAudit eventAudit1, boolean isIngestionSuccessful) {
        if (isIngestionSuccessful)
            return ((CephIngestionEndInfoEvent) eventAudit.getPayload()).getUrls().equals(((CephIngestionEndInfoEvent) eventAudit1.getPayload()).getUrls());
        return true;
    }

    private boolean compareFailedCephDetails(EventAudit eventAudit, EventAudit eventAudit1, boolean isIngestionSuccessful) {
        if (!isIngestionSuccessful) {
            CephOutputLocation currentOutputLocation = ((CephIngestionErrorEvent) eventAudit.getPayload()).getCephOutputLocation();
            CephOutputLocation existingOutputLocation = ((CephIngestionErrorEvent) eventAudit1.getPayload()).getCephOutputLocation();
            return currentOutputLocation.getClientAlias().equalsIgnoreCase(existingOutputLocation.getClientAlias())
                    && currentOutputLocation.getPath().equalsIgnoreCase(existingOutputLocation.getPath())
                    && currentOutputLocation.getBucket().equalsIgnoreCase(existingOutputLocation.getBucket());
        }
        return true;
    }

    private void addBodyForCephIngestion(ContainerTag tableTag, List<EventAudit> eventAudits, boolean isIngestionSuccessful) {
        for (EventAudit eventAudit : eventAudits) {
            ContainerTag cephDetailsTag = getCephDetailsTag(eventAudit, isIngestionSuccessful);
            tableTag.with(TagCreator.tr().with(TagCreator.td(getCephDataFrameName(eventAudit, isIngestionSuccessful)))
                    .with(TagCreator.td("Ceph")).with(cephDetailsTag));
        }
    }

    private String getCephDataFrameName(EventAudit eventAudit, boolean isIngestionSuccessful) {
        return isIngestionSuccessful ? ((CephIngestionEndInfoEvent) eventAudit.getPayload()).getDataFrameName()
                : ((CephIngestionErrorEvent) eventAudit.getPayload()).getDataFrameName();
    }

    private ContainerTag getCephDetailsTag(EventAudit eventAudit, boolean isIngestionSuccessful) {
        if (!isIngestionSuccessful)
            return TagCreator.td(JsonUtils.DEFAULT.toJson(((CephIngestionErrorEvent) eventAudit.getPayload()).getCephOutputLocation()));
        else {
            CephIngestionEndInfoEvent cephIngestionEndInfoEvent = (CephIngestionEndInfoEvent) eventAudit.getPayload();
            ContainerTag detailsTag = TagCreator.td().attr("rowspan", cephIngestionEndInfoEvent.getUrls().size());
            for (URL url : cephIngestionEndInfoEvent.getUrls()) detailsTag.with(TagCreator.p(url.toString()));
            return detailsTag;
        }
    }

    private String getSuccessMessageBody(Object payload, ContainerTag htmlTag) {
        if (payload instanceof DSPWorkflowExecutionResult)
            addLegacyExecutionDetails(htmlTag, (DSPWorkflowExecutionResult) payload);
        if (payload instanceof WorkflowGroupExecutionResult)
            addNewExecutionsDetails(htmlTag, (WorkflowGroupExecutionResult) payload, false);
        return htmlTag.render();

    }

    private void addLegacyExecutionDetails(ContainerTag htmlTag, DSPWorkflowExecutionResult executionResult) {
        ContainerTag bodyTag = TagCreator.body().with(TagCreator.p("output_details:"));
        ContainerTag tableTag = TagCreator.table();
        ContainerTag headingTag = TagCreator.tr().with(TagCreator.th("entity Name")).with(TagCreator.th("Partition Id"));
        tableTag.with(headingTag);
        executionResult.getPartitionOverrides().forEach((entityName, partitionId) ->
                tableTag.with(TagCreator.tr().with(TagCreator.td(entityName))).with(TagCreator.td(partitionId.toString())));
        bodyTag.with(tableTag);
        htmlTag.with(bodyTag);
    }

    private void addNewExecutionsDetails(ContainerTag htmlTag, WorkflowGroupExecutionResult workflowGroupExecutionResult, boolean onlyIngestionEntity) {
        workflowGroupExecutionResult.getWorkflowExecutionResultMap().forEach((workflowName, workflowExecutionResult) ->
                htmlTag.with(addOutputDetails(workflowExecutionResult.getScriptExecutionResultMap(), onlyIngestionEntity)));
    }

    private ContainerTag addOutputDetails(Map<String, List<ScriptExecutionResult>> scriptExecutionResultMap, boolean onlyIngestionEntity) {
        if (scriptExecutionResultMap.size() == 0)
            return TagCreator.body();
        List<ContainerTag> scriptOutputVariableTags = new ArrayList<>();
        for (Map.Entry<String, List<ScriptExecutionResult>> entry : scriptExecutionResultMap.entrySet()) {
            List<List<ContainerTag>> outputContainerTagsList = new ArrayList<>();
            String dataFrameName = entry.getKey();
            List<ScriptExecutionResult> scriptExecutionResults = entry.getValue();
            scriptExecutionResults.forEach(scriptExecutionResult -> {
                processHiveResult(outputContainerTagsList, scriptExecutionResult, onlyIngestionEntity);
                processCephResult(outputContainerTagsList, scriptExecutionResult);
                processHDFSResult(outputContainerTagsList, scriptExecutionResult, onlyIngestionEntity);
            });
            if (outputContainerTagsList.size() > 0) {
                ContainerTag scriptVariableTag = TagCreator.tr().with(TagCreator.td(dataFrameName).attr("rowspan", scriptExecutionResults.size()));
                outputContainerTagsList.get(0).forEach(scriptVariableTag::with);
                scriptOutputVariableTags.add(scriptVariableTag);
                IntStream.range(1, outputContainerTagsList.size()).forEach(i -> {
                    ContainerTag tag = TagCreator.tr();
                    outputContainerTagsList.get(i).forEach(tag::with);
                    scriptOutputVariableTags.add(tag);
                });
            }
        }
        if (scriptOutputVariableTags.size() > 0) {
            String message = onlyIngestionEntity ? "Ingestion Details:" : "output_details:";
            ContainerTag tableTag = TagCreator.table().with(TagCreator.tr().with(TagCreator.th("DataFrame Name"))
                    .with(TagCreator.th("Output Location")).with(TagCreator.th("Output Details")));
            scriptOutputVariableTags.forEach(tableTag::with);
            return TagCreator.body().with(TagCreator.p(message)).with(tableTag);
        } else {
            String message = String.format("No Output %sEntity Found.", (onlyIngestionEntity ? "Ingestion " : ""));
            return TagCreator.body().with(TagCreator.p(message));
        }
    }

    private void processHiveResult(List<List<ContainerTag>> outputContainerTagsList, ScriptExecutionResult scriptExecutionResult, boolean onlyIngestionEntity) {
        if (scriptExecutionResult instanceof HiveScriptExecutionResult && !onlyIngestionEntity) {
            HiveScriptExecutionResult hiveScriptExecutionResult = (HiveScriptExecutionResult) scriptExecutionResult;
            List<ContainerTag> containerTags = new ArrayList<>();
            containerTags.add(TagCreator.td("HIVE"));
            containerTags.add(TagCreator.td(getTableDetailsString(hiveScriptExecutionResult.getDatabase(),
                    hiveScriptExecutionResult.getTable(), hiveScriptExecutionResult.getRefreshId())));
            outputContainerTagsList.add(containerTags);
        }
    }

    private String getTableDetailsString(String dbName, String tableName, Long refreshId) {
        return dbName + dot + tableName + questionMark + equal + refreshId;
    }

    private void processHDFSResult(List<List<ContainerTag>> outputContainerTagsList, ScriptExecutionResult scriptExecutionResult, boolean onlyIngestionEntity) {
        if (scriptExecutionResult instanceof HDFSScriptExecutionResult && !onlyIngestionEntity) {
            HDFSScriptExecutionResult hdfsScriptExecutionResult = (HDFSScriptExecutionResult) scriptExecutionResult;
            List<ContainerTag> containerTags = new ArrayList<>();
            containerTags.add(TagCreator.td("HDFS"));
            containerTags.add(TagCreator.td(hdfsScriptExecutionResult.getLocation()));
            outputContainerTagsList.add(containerTags);
        }
    }

    private void processCephResult(List<List<ContainerTag>> outputContainerTagsList, ScriptExecutionResult scriptExecutionResult) {
        if (scriptExecutionResult instanceof CephScriptExecutionResult) {
            CephScriptExecutionResult cephScriptExecutionResult = (CephScriptExecutionResult) scriptExecutionResult;
            ContainerTag detailsTag = TagCreator.td();
            for (URL url : cephScriptExecutionResult.getUrls())
                detailsTag.with(TagCreator.p(url.toString()));

            List<ContainerTag> containerTags = new ArrayList<>();
            containerTags.add(TagCreator.td("Ceph"));
            containerTags.add(detailsTag);
            outputContainerTagsList.add(containerTags);
        }
    }

    EmailNotification constructWorkflowStateChangeEmailNotification(Request request, String workflowName, String emailRecipients,
                                                                    WorkflowStepStateNotificationType workflowStepStateNotificationType,
                                                                    WorkflowGroupExecutionResult workflowGroupExecutionResult) {
        String subjectPrefix = getSubjectPrefix(workflowStepStateNotificationType);
        String subjectPostfix = getSubjectPostfix(workflowStepStateNotificationType);
        String emailSubject = constructSubject(request, subjectPrefix, subjectPostfix, workflowName);
        String emailBody = constructBodyForWorkflowStateEmail(request, workflowStepStateNotificationType, workflowGroupExecutionResult);
        return getEmailNotificationObject(emailRecipients, emailSubject, emailBody);
    }

    private String getSubjectPrefix(WorkflowStepStateNotificationType workflowStepStateNotificationType) {
        switch (workflowStepStateNotificationType) {
            case STARTED:
                return "Execution of ";
            case SG:
                return "SG of ";
            case WORKFLOW:
                return "Script execution of ";
            case OUTPUT_INGESTION:
                return "Output ingestion of ";
            default:
                return "";
        }
    }

    private String getSubjectPostfix(WorkflowStepStateNotificationType workflowStepStateNotificationType) {
        return workflowStepStateNotificationType == WorkflowStepStateNotificationType.STARTED ? " has started." : " is successfully completed.";
    }

    private String constructBodyForWorkflowStateEmail(Request request, WorkflowStepStateNotificationType workflowStepStateNotificationType,
                                                      WorkflowGroupExecutionResult workflowGroupExecutionResult) {
        try {
            ContainerTag htmlTag = getHtmlTagWithAzkabanDetails("Track progress at ", request.getAzkabanExecId());
            htmlTag.with(getHtmlTagWithExecutionDetails(request.getId()));
            if (workflowStepStateNotificationType.equals(WorkflowStepStateNotificationType.OUTPUT_INGESTION))
                addNewExecutionsDetails(htmlTag, workflowGroupExecutionResult, true);
            else if (workflowStepStateNotificationType.equals(WorkflowStepStateNotificationType.WORKFLOW))
                addSuccessfulWorkflowDetails(htmlTag, request.getId());
            return htmlTag.render();
        } catch (Exception e) {
            return TagCreator.html().with(TagCreator.body()
                    .with(TagCreator.p("Error while constructing email message body.Please contact dsp-oncall@flipkart.com"))).render();
        }
    }

    private void addSuccessfulWorkflowDetails(ContainerTag htmlTag, Long requestId) {
        List<PipelineStepAudit> pipelineStepAudits = pipelineStepAuditActor.getPipelineStepAudits(requestId);
        Map<String, Long> successfulPartitionToAuditIdMapping = getPartitionToAuditIdMapping(pipelineStepAudits, PipelineStepStatus.SUCCESS);
        String message = "Please find logs for some partitions:";
        ContainerTag bodyTag = TagCreator.body().with(TagCreator.p(message));
        addLogDetails(successfulPartitionToAuditIdMapping, bodyTag);
        htmlTag.with(bodyTag);
    }

    public EmailNotification constructPartitionStateChangeEmailNotification(Request request, String workflowName, String emailRecipients,
                                                                            PartitionDetailsEmailNotificationRequest emailNotificationRequest) {
        String subjectPrefix = "Script execution for partition granularity : " + getPartitionGranularityString(emailNotificationRequest.getPartitionDetails()) + " of ";
        String subjectPostfix = getSubjectPostfix(emailNotificationRequest.getWorkflowStateNotificationType());
        String emailSubject = constructSubject(request, subjectPrefix, subjectPostfix, workflowName);
        String emailBody = constructBodyForPartitionStateChangeEmail(request.getAzkabanExecId(), emailNotificationRequest);
        return getEmailNotificationObject(emailRecipients, emailSubject, emailBody);
    }

    private String getPartitionGranularityString(Map<String, Object> partitionDetails) {
        List<String> partitionDetailsList = partitionDetails.keySet().stream()
                .map(partitionKey -> partitionKey + equal + partitionDetails.get(partitionKey)).collect(Collectors.toList());
        if (partitionDetailsList.size() == 0)
            partitionDetailsList.add("default_partition");
        return String.format("[%s]", String.join(",", partitionDetailsList));
    }

    private String getSubjectPostfix(WorkflowStateNotificationType workflowStateNotificationType) {
        return workflowStateNotificationType.toString().equalsIgnoreCase(WorkflowStateNotificationType.SUCCESS.toString())
                ? " is successfully completed." : " is failed.";
    }

    private String constructBodyForPartitionStateChangeEmail(Long azkabanExecId, PartitionDetailsEmailNotificationRequest emailNotificationRequest) {
        ContainerTag htmlTag = getHtmlTagWithAzkabanDetails("Track progress at ", azkabanExecId);
        ContainerTag bodyTag = TagCreator.body().with(TagCreator.p("Details"));
        ContainerTag tableTag = TagCreator.table();
        ContainerTag headingTag = TagCreator.tr();
        headingTag.with(TagCreator.th("PartitionKey")).with(TagCreator.th("PartitionValue"));
        tableTag.with(headingTag);
        emailNotificationRequest.getPartitionDetails().forEach((partitionKey, partitionValue) ->
                tableTag.with(TagCreator.td(partitionKey).with(TagCreator.td(partitionValue.toString()))));
        bodyTag.with(tableTag);
        bodyTag.with(TagCreator.p("Log url: " + emailNotificationRequest.getLogs()));
        return htmlTag.with(bodyTag).render();
    }

    private Map<String, Long> getPartitionToAuditIdMapping(List<PipelineStepAudit> pipelineStepAudits, PipelineStepStatus pipelineStepStatus) {
        List<PipelineStepAudit> successfulAudits = pipelineStepAudits.stream().filter(pipelineStepAudit ->
                pipelineStepAudit.getPipelineStepStatus().equals(pipelineStepStatus)).collect(Collectors.toList()).
                stream().limit(10).collect(Collectors.toList());

        Map<String, Long> partitionToAuditIdMapping = new HashMap<>();
        for (PipelineStepAudit pipelineStepAudit : successfulAudits) {
            TypeReference<LinkedHashSet<WhereClause>> typeRef = new TypeReference<LinkedHashSet<WhereClause>>() {
            };
            LinkedHashSet<WhereClause> scopes = JsonUtils.DEFAULT.fromJson(pipelineStepAudit.getScope(), typeRef);
            String partition = scopes.stream().map(scope -> scope.getId() + equal + String.join(",", scope.getValues()) + "^").collect(Collectors.joining());
            if (scopes.size() > 0) partition = partition.substring(0, partition.length() - 1);
            if (Strings.isNullOrEmpty(partition)) partition = "default_partition";
            if (!partitionToAuditIdMapping.containsKey(partition))
                partitionToAuditIdMapping.put(partition, pipelineStepAudit.getId());
        }
        return partitionToAuditIdMapping;
    }

    private EmailNotification getEmailNotificationObject(String recipients, String emailSubject, String emailBody) {
        EmailNotification emailNotification = new EmailNotification();
        emailNotification.setBody(emailBody);
        emailNotification.setFrom(miscConfig.getDefaultNotificationEmailId());
        emailNotification.setSubject(emailSubject);
        emailNotification.setTo(constructToArray(recipients));
        emailNotification.setBodyTypeHtml(true);
        return emailNotification;
    }

    private String[] constructToArray(String recipientsID) {
        String[] emailIds = recipientsID.trim().split(COMMA);
        Arrays.stream(emailIds).map(String::trim).toArray(unused -> emailIds);
        return emailIds;
    }

    private String constructSubject(Request request, String prefix, String postfix, String workflowName) {
        StringBuilder subject = new StringBuilder();
        subject.append(prefix);
        if (!isNull(request)) {
            subject.append("Request: ");
            subject.append(request.getId());
            subject.append(" for ");
        }
        subject.append("workflow: ");
        subject.append(workflowName);
        subject.append(postfix);
        return subject.toString();
    }
}
