package com.flipkart.dsp.api;

import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.azkaban.AzkabanJobStatusResponse;
import com.flipkart.dsp.config.DSPClientConfig;
import com.flipkart.dsp.entities.enums.RequestStepType;
import com.flipkart.dsp.entities.misc.Resources;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepSGAudit;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowAudit;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.exceptions.EntityNotFoundException;
import com.flipkart.dsp.models.PipelineStepStatus;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.misc.JobDetailsDTO;
import com.flipkart.dsp.service.AzkabanProjectHelper;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.MesosMemoryUsageUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static com.flipkart.dsp.utils.Constants.questionMark;
import static com.flipkart.dsp.utils.Constants.slash;
import static java.util.stream.Collectors.groupingBy;

/**
 * +
 */
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class JobDetailsAPI {
    private final RequestActor requestActor;
    private final WorkFlowActor workFlowActor;
    private final DSPClientConfig dspClientConfig;
    private final WorkflowAuditActor workflowAuditActor;
    private final DataFrameAuditActor dataFrameAuditActor;
    private final AzkabanExecutionAPI azkabanExecutionAPI;
    private final AzkabanProjectHelper azkabanProjectHelper;
    private final MesosMemoryUsageUtils mesosMemoryUsageUtils;
    private final PipelineStepAuditActor pipelineStepAuditActor;
    private final PipelineStepSGAuditActor pipelineStepSGAuditActor;
    private final WorkflowExecutionResponseActor workflowExecutionResponseActor;

    public JobDetailsDTO getJobDetails(Request request) throws DSPSvcException {
        AzkabanJobStatusResponse azkabanJobStatusResponse = azkabanExecutionAPI.getJobStatus(request.getAzkabanExecId(), request.getId());
        Workflow workflow = request.getWorkflowDetails().getWorkflow();
        JobDetailsDTO.WorkflowDetails workflowDetails = JobDetailsDTO.WorkflowDetails.builder().isProd(workflow.getIsProd())
                .workflowName(workflow.getName()).workflowVersion(workflow.getVersion()).build();
        Timestamp endTime = getEndTime(request);

        return JobDetailsDTO.builder()
                .requestId(request.getId())
                .requestStatus(azkabanJobStatusResponse.getStatus().toString())
                .azkabanUrl(azkabanProjectHelper.getAzkabanUrl(Long.parseLong(azkabanJobStatusResponse.getExecid())))
                .startTime(new Date(request.getCreatedAt().getTime()))
                .endTime(Objects.isNull(endTime) ? null : new Date(endTime.getTime()))
                .executionTime(getExecutionTime(request.getCreatedAt(), endTime))
                .workflowDetails(workflowDetails)
                .inputDetails(getInputDetails(request))
                .jobDetails(getJobDetails(request.getId(), request.getWorkflowDetails().getPipelineSteps()))
                .outputPayload(workflowExecutionResponseActor.getWorkflowExecutionPayload(request, request.getRequestStatus(), request.getWorkflowDetails()))
                .previousRunDetails(getPreviousRunDetails(request.getId(), workflowDetails))
                .build();
    }

    private JobDetailsDTO.InputDetails getInputDetails(Request request) {
        return JobDetailsDTO.InputDetails.builder().inputPayload(request.getData())
                .inputDataFramesSize(getInputDataFramesSize(request)).build();
    }

    private Map<String, Map<String, String>> getInputDataFramesSize(Request request) {
        WorkflowDetails workflowDetails = request.getWorkflowDetails();
        Long workflowId = workflowDetails.getWorkflow().getId();
        Map<String /*pipelineStepName*/, Map<String /*dataFrame Name */, String /* DataFrame size*/>> outputMap = new HashMap<>();
        workflowDetails.getPipelineSteps().forEach(pipelineStep -> {
            Set<DataFrameAudit> dataFrameAudits = dataFrameAuditActor.getDataframeAudits(request.getId(), workflowId,
                    pipelineStep.getId());
            Map<String, String> dataFrameSizeMap = new HashMap<>();
            dataFrameAudits.forEach(dataFrameAudit -> dataFrameSizeMap.put(dataFrameAudit.getDataFrame().getName(),
                    byteCountToDisplaySize(dataFrameAudit.getDataframeSize())));
            outputMap.put(pipelineStep.getName(), dataFrameSizeMap);
        });
        return outputMap;
    }

    public static String byteCountToDisplaySize(long bytes) {
        int unit = 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), "KMGTPE".charAt(exp - 1));
    }

    private Map<Integer, Map<String, JobDetailsDTO.PipelineStepDetails>> getJobDetails(Long requestId, List<PipelineStep> pipelineSteps) {
        List<WorkflowAudit> workflowAudits = workflowAuditActor.getWorkflowAudits(requestId);
        Map<Integer, Map<String, JobDetailsDTO.PipelineStepDetails>> logMap = new HashMap<>();
        for (int i = 0; i < workflowAudits.size(); i++) {
            List<PipelineStepSGAudit> pipelineStepSGAudits = pipelineStepSGAuditActor.getPipelineStepSgAudits(workflowAudits.get(i).getWorkflowExecutionId());
            List<PipelineStepAudit> pipelineStepAudits = pipelineStepAuditActor.getPipelineStepAudits(null, workflowAudits.get(i).getWorkflowExecutionId());
            pipelineSteps.sort(Comparator.comparing(PipelineStep::getId));
            Map<String, JobDetailsDTO.PipelineStepDetails> pipelineStepsLogs = new HashMap<>();
            for (PipelineStep pipelineStep : pipelineSteps) {
                pipelineStepsLogs.put(pipelineStep.getName(), JobDetailsDTO.PipelineStepDetails.builder()
                        .partitions(pipelineStep.getPartitions())
                        .sgExecutionDetails(getSGExecutionDetails(pipelineStep.getId(), pipelineStepSGAudits))
                        .workflowExecutionDetails(getWorkflowExecutionDetails(pipelineStep.getId(), pipelineStepAudits)).build());
            }
            logMap.put(i, pipelineStepsLogs);
        }
        return logMap;
    }

    private JobDetailsDTO.SGExecutionDetails getSGExecutionDetails(Long pipelineStepId, List<PipelineStepSGAudit> pipelineStepSGAudits) {
        PipelineStepSGAudit pipelineStepSGAudit = pipelineStepSGAudits.stream().filter(audit ->
                pipelineStepId.equals(audit.getPipelineStep())).findFirst().orElse(null);

        if (Objects.nonNull(pipelineStepSGAudit)) {
            Timestamp endTime = getEndTime(pipelineStepSGAudit.getStatus(), pipelineStepSGAudit.getUpdatedAt());
            return JobDetailsDTO.SGExecutionDetails.builder().status(pipelineStepSGAudit.getStatus().name())
                    .startTime(new Date(pipelineStepSGAudit.getCreatedAt().getTime()))
                    .endTime(Objects.isNull(endTime) ? null : new Date(endTime.getTime()))
                    .executionTime(getExecutionTime(pipelineStepSGAudit.getCreatedAt(), endTime))
                    .stdoutLogs(getMesosLogUrl(pipelineStepSGAudit.getId(), RequestStepType.SG, "stdout"))
                    .stderrLogs(getMesosLogUrl(pipelineStepSGAudit.getId(), RequestStepType.SG, "stderr")).build();
        }
        return null;
    }

    private Timestamp getEndTime(PipelineStepStatus pipelineStepStatus, Timestamp updatedAt) {
        if (pipelineStepStatus.equals(PipelineStepStatus.SUCCESS) || pipelineStepStatus.equals(PipelineStepStatus.FAILED))
            return updatedAt;
        return null;
    }

    private String getExecutionTime(Timestamp startTime, Timestamp endTime) {
        if (Objects.isNull(endTime))
            endTime = Timestamp.from(Instant.now());

        long diff = endTime.getTime() - startTime.getTime();
        long diffSeconds = diff / 1000 % 60;
        long diffMinutes = diff / (60 * 1000) % 60;
        long diffHours = diff / (60 * 60 * 1000) % 24;
        long diffDays = diff / (24 * 60 * 60 * 1000);

        return (diffDays == 0 ? "" : diffDays + " days ") + (diffHours == 0 ? "" : diffHours + " hours ")
                + (diffMinutes == 0 ? "" : diffMinutes + " minutes ") + diffSeconds + " seconds ";
    }

    private List<JobDetailsDTO.WorkflowExecutionDetails> getWorkflowExecutionDetails(Long pipelineStepId, List<PipelineStepAudit> pipelineStepAudits) {
        List<PipelineStepAudit> pipelineStepAuditsForStep = pipelineStepAudits.stream().filter(audit -> pipelineStepId.equals(audit.getPipelineStepId())).collect(Collectors.toList());
        if (pipelineStepAuditsForStep.isEmpty())
            return new ArrayList<>();
        Map<String, List<PipelineStepAudit>> scopeToAuditMap = pipelineStepAuditsForStep.stream().collect(groupingBy(PipelineStepAudit::getScope));
        List<JobDetailsDTO.WorkflowExecutionDetails> workflowExecutionDetails = new ArrayList<>();
        for (Map.Entry<String, List<PipelineStepAudit>> entry : scopeToAuditMap.entrySet()) {
            List<PipelineStepAudit> pipelineStepAuditList = entry.getValue();
            Map<String, Set<String>> partitionDetails = mesosMemoryUsageUtils.getPartitionDetails(entry.getKey());
            pipelineStepAuditList.sort(Comparator.comparing(PipelineStepAudit::getAttempt));
            List<JobDetailsDTO.WorkflowExecutionAttemptDetails> workflowExecutionAttemptDetails = new ArrayList<>();

            for (PipelineStepAudit pipelineStepAudit : pipelineStepAuditList) {
                Timestamp endTime = getEndTime(pipelineStepAudit.getPipelineStepStatus(), pipelineStepAudit.getUpdatedAt());
                workflowExecutionAttemptDetails.add(JobDetailsDTO.WorkflowExecutionAttemptDetails.builder()
                        .attempt(pipelineStepAudit.getAttempt()).status(pipelineStepAudit.getPipelineStepStatus().name())
                        .startTime(new Date(pipelineStepAudit.getCreatedAt().getTime()))
                        .endTime(Objects.isNull(endTime) ? null : new Date(endTime.getTime()))
                        .executionTime(getExecutionTime(pipelineStepAudit.getCreatedAt(), endTime))
                        .stdoutLogs(getMesosLogUrl(pipelineStepAudit.getId(), RequestStepType.WF, "stdout"))
                        .stderrLogs(getMesosLogUrl(pipelineStepAudit.getId(), RequestStepType.WF, "stderr"))
                        .resources(getResources(pipelineStepAudit.getResources()))
                        .memoryUsageDashboard(mesosMemoryUsageUtils.getMemoryUsageDashboard(pipelineStepAudit.getAttempt(), pipelineStepAudit, partitionDetails))
                        .build());
            }
            workflowExecutionDetails.add(JobDetailsDTO.WorkflowExecutionDetails.builder()
                    .partitionDetails(partitionDetails)
                    .workflowExecutionAttemptDetails(workflowExecutionAttemptDetails).build());
        }
        return workflowExecutionDetails;
    }

    private JobDetailsDTO.Resources getResources(Resources resources) {
        return JobDetailsDTO.Resources.builder().cpu(resources.getCpus()).memory(resources.getMemory()).build();
    }

    private String getMesosLogUrl(Long auditId, RequestStepType requestStepType, String logType) {
        String logPrefixPath = String.format(Constants.LOG_PATH_PREFIX, Constants.http,
                dspClientConfig.getHost(), dspClientConfig.getPort(), Constants.LOG_RESOURCE_PREFIX);
        return logPrefixPath + (requestStepType.equals(RequestStepType.SG) ? RequestStepType.SG.name().toLowerCase() + slash : "")
                + auditId + "/log" + questionMark + "log-type=" + logType;
    }

    private String getPartitionValues(Map<String, Set<String>> partitionDetails) {
        StringBuilder partitionValue = new StringBuilder();
        partitionDetails.forEach((key, value1) -> {
            if (key.equals("default_partitions"))
                partitionValue.append("__");
            else {
                value1.forEach(value -> {
                    partitionValue.append("___");
                    partitionValue.append(value);
                    partitionValue.append("___");
                });
            }
        });
        return partitionValue.toString();
    }

    private List<JobDetailsDTO.PreviousRunDetail> getPreviousRunDetails(Long requestId, JobDetailsDTO.WorkflowDetails workflowDetails) {
        List<JobDetailsDTO.PreviousRunDetail> previousRunDetails = new ArrayList<>();
        Workflow workflow = workFlowActor.getWorkflow(workflowDetails.getWorkflowName(),
                workflowDetails.getIsProd(), workflowDetails.getWorkflowVersion());

        if (Objects.isNull(workflow)) {
            String errorMessage = String.format("Workflow not found for following details, name: %s, is_prod: %s, version: %s"
                    , workflowDetails.getWorkflowName(), workflowDetails.getIsProd(), workflowDetails.getWorkflowVersion());
            throw new EntityNotFoundException(Workflow.class.getName(), errorMessage);
        }

        // we will only show last 10 requests
        List<Request> latestRequests = requestActor.getLatestRequests(workflow.getId(), 10);
        latestRequests.stream().filter(request -> !request.getId().equals(requestId)).forEach(request -> {
            Timestamp endTime = getEndTime(request);
            previousRunDetails.add(JobDetailsDTO.PreviousRunDetail.builder()
                    .requestId(request.getId()).status(request.getRequestStatus().name())
                    .startTime(new Date(request.getCreatedAt().getTime()))
                    .endTime(Objects.isNull(endTime) ? null : new Date(endTime.getTime()))
                    .executionTime(getExecutionTime(request.getCreatedAt(), endTime))
                    .inputDetails(getInputDetails(request))
                    .build());
        });
        return previousRunDetails;
    }

    private Timestamp getEndTime(Request request) {
        if (request.getRequestStatus().equals(RequestStatus.COMPLETED)
                || request.getRequestStatus().equals(RequestStatus.FAILED))
            return request.getUpdatedAt();
        return null;
    }
}
