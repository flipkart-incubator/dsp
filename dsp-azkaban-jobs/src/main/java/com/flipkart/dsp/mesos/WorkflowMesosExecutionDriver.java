package com.flipkart.dsp.mesos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.actors.PipelineStepActor;
import com.flipkart.dsp.actors.PipelineStepAuditActor;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.db.entities.PipelineStepEntity;
import com.flipkart.dsp.dto.UpdateEntityDTO;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.misc.Resources;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepResources;
import com.flipkart.dsp.entities.script.ScriptMeta;
import com.flipkart.dsp.entities.sg.core.DataFrameAuditStatus;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.mesos.entities.Job;
import com.flipkart.dsp.mesos.entities.JobGroup;
import com.flipkart.dsp.mesos.framework.DSPMesosFramework;
import com.flipkart.dsp.models.PipelineStepStatus;
import com.flipkart.dsp.models.WorkflowStatus;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.EventAuditUtil;
import com.flipkart.dsp.utils.JsonUtils;
import com.flipkart.dsp.utils.PartitionScopeUtil;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringEscapeUtils;
import org.javatuples.Triplet;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkflowMesosExecutionDriver {
    private static final int BATCH_SIZE = 500;

    private final MiscConfig miscConfig;
    private final ObjectMapper objectMapper;
    private final EventAuditUtil eventAuditUtil;
    private final DSPServiceClient dspServiceClient;
    private final PipelineStepActor pipelineStepActor;
    private final PartitionScopeUtil partitionScopeUtil;
    private final DSPServiceConfig.MesosConfig mesosConfig;
    private final ResourceComputeDriver resourceComputeDriver;
    private final PipelineStepAuditActor pipelineStepAuditActor;
    private final DSPServiceConfig.ScriptExecutionConfig scriptExecutionConfig;


    public boolean execute(WorkflowDetails workflowDetails, int retries, String role,
                           List<Triplet<ConfigPayload, Long, Long>> payloadTripletList, Boolean failFast,
                           Long requestId, PipelineStep pipelineStep) throws DSPCoreException {
        String workflowName = workflowDetails.getWorkflow().getName();
        if (payloadTripletList.isEmpty()) {
            throw new IllegalArgumentException("Payload list cannot be empty.");
        }
        String frameWorkName = workflowName + "_" + pipelineStep.getName() + "_WF";
        String mesosZookeeperAddress = mesosConfig.getZookeeperAddress();
        DSPMesosFramework dspMesosFramework = new DSPMesosFramework(mesosZookeeperAddress, role, frameWorkName, false);
        //Map <pipelineExecutionId,List>: All the ConfigPayloads related to that pipelineId (of single scope)
        Map<String, List<Triplet<ConfigPayload, Long, Long>>> pipelineExecutionIdToTriplet = new HashMap<>();
        Set<Long> pipelineStepID = new HashSet<>();


        // this triplet is <ConfigPayload, trainCSVFileSize, executeCSVFileSize>
        for (Triplet<ConfigPayload, Long, Long> triplet : payloadTripletList) {
            if (pipelineExecutionIdToTriplet.get(triplet.getValue0().getPipelineExecutionId()) != null) {
                pipelineExecutionIdToTriplet.get(triplet.getValue0().getPipelineExecutionId()).add(triplet);
            } else {
                List<Triplet<ConfigPayload, Long, Long>> tripletList = new ArrayList<>();
                tripletList.add(triplet);
                pipelineExecutionIdToTriplet.put(triplet.getValue0().getPipelineExecutionId(), tripletList);
            }
            pipelineStepID.add(triplet.getValue0().getPipelineStepId());
        }

        List<JobGroup> jobGroups = populateJobGroups(workflowDetails, pipelineExecutionIdToTriplet, retries, pipelineStep);
        createWFSubmittedDebugEvent(pipelineStepID, workflowDetails.getWorkflow().getName(), requestId, workflowDetails.getWorkflow().getId());
        log.info("running tasks on mesos");
        return handlePermanentFailedRun(dspMesosFramework.run(jobGroups, failFast), pipelineExecutionIdToTriplet);
    }

    public boolean executeSG(WorkflowDetails workflowDetails, int retries, String role, Long requestId,
                             Long pipelineStepId, String workflowExecutionId, PipelineStep pipelineStep) {
        String workflowName = workflowDetails.getWorkflow().getName();
        String mesosZookeeperAddress = mesosConfig.getZookeeperAddress();
        String frameworkName = workflowName + "_" + pipelineStep.getName() + "_SG";
        DSPMesosFramework dspMesosFramework = new DSPMesosFramework(mesosZookeeperAddress, role, frameworkName, false);
        ScriptMeta scriptMeta = getScriptMeta(pipelineStep);
        List<JobGroup> jobGroups = new ArrayList<>();

//        add new job group here for scatter-gather signal generation
        ArrayList<Job> shufflerJobList = new ArrayList<>();
        ConfigPayload shufflerConfigPayload = generateShufflerConfigPayload(requestId, workflowDetails.getWorkflow().getId(),
                pipelineStepId, pipelineStep.getName(), workflowExecutionId);
        String jobId = shufflerConfigPayload.getPipelineExecutionId() + "#" + shufflerConfigPayload.getPipelineStepId();
        String sgJobName = "sg_"+shufflerConfigPayload.getPipelineStepId();
        String startUpScriptPath = scriptMeta.getStartUpScriptPath().substring(0, scriptMeta.getStartUpScriptPath().lastIndexOf("/") + 1) + "startup-shuffler.sh";
        String shufflerCommand = generateCommand(shufflerConfigPayload, startUpScriptPath, "SignalShuffleApplication");
        Job shufflerJob = new Job(4.0, 4000.0, shufflerCommand, scriptExecutionConfig.getSgDockerImage(),
                MesosConstants.CONTAINER_TYPE, MesosConstants.CONTAINER_NETWORK, mesosConfig.getContainerOptions(), null,
                getMountVolumes(), 0, jobId, sgJobName, false);
        shufflerJobList.add(shufflerJob);
        log.info("added sg job group: " + shufflerJob.getName());
        jobGroups.add(new JobGroup(shufflerJobList));

        // todo: convert to sg debug event
//        createWFSubmittedDebugEvent(pipelineStepId, workflowDetails.getWorkflow().getName(), requestId.toString(), workflowDetails.getWorkflow().getId());
        log.info("running sg on mesos");
        return handleSGFailedRun(dspMesosFramework.run(jobGroups, true), requestId, workflowExecutionId, workflowDetails.getWorkflow().getId());

    }

    private void createWFSubmittedDebugEvent(Set<Long> pipelineStepIDList, String workflowName, Long requestId, Long workflowID) {
        for(Long pipelineStepID : pipelineStepIDList) {
            PipelineStepEntity pipelineStepEntity = pipelineStepActor.getPipelineStepByPipelineStepId(pipelineStepID);
            try {
                PipelineStepResources pipelineStepResources = objectMapper.readValue(pipelineStepEntity.getPipelineStepResources(), PipelineStepResources.class);
                eventAuditUtil.createWFSubmittedDebugEvent(requestId, workflowID, workflowName, new Timestamp(new Date().getTime()), pipelineStepID,
                        pipelineStepResources.getBaseCpu(), pipelineStepResources.getBaseMemory());
            } catch (IOException e) {
                log.error("Error while fetching Resources for PipelineEntity", e);
            }
        }
    }


    private List<JobGroup> populateJobGroups(WorkflowDetails workflowDetails, Map<String, List<Triplet<ConfigPayload, Long, Long>>> pipelineExecutionIdToTriplet,
                                             int retries, PipelineStep pipelineStep) throws DSPCoreException {
        List<JobGroup> jobGroups = new ArrayList<>();
        Map<Job, ConfigPayload> jobConfigPayloadMap = new HashMap<>();

        List<Triplet<ConfigPayload, Long, Long>> pipelineExecutionIdToTripletList = pipelineExecutionIdToTriplet
                .entrySet().stream().flatMap(p -> p.getValue().stream()).collect(Collectors.toList());
        Map<String, Map<Long,ResourceComputeDriver.Resources>> resourcesMap = resourceComputeDriver.computeResources(workflowDetails, pipelineExecutionIdToTripletList);
        ScriptMeta scriptMeta = getScriptMeta(pipelineStep);
        for (Map.Entry<String, List<Triplet<ConfigPayload, Long, Long>>> entry : pipelineExecutionIdToTriplet.entrySet()) {
            List<Triplet<ConfigPayload, Long, Long>> pipelineStepScopeLevelPayloads = entry.getValue();
            ArrayList<Job> jobList = new ArrayList<>();
            for (Triplet<ConfigPayload, Long, Long> triplet : pipelineStepScopeLevelPayloads) {
                ConfigPayload configPayload = triplet.getValue0();
                final String pipelineExecutionId = configPayload.getPipelineExecutionId();
                final long pipelineStepId = configPayload.getPipelineStepId();
                String id = pipelineExecutionId + "#" + pipelineStepId;
                String command = generateCommand(configPayload, scriptMeta.getStartUpScriptPath(), getExecutionDriverName());
                String jobName = partitionScopeUtil.populateJobName(configPayload.getScope());
                ResourceComputeDriver.Resources resources = resourcesMap.get(pipelineExecutionId).get(pipelineStepId);
                Map<String, String> mesosContainerOptions = mesosConfig.getContainerOptions();
                Job job = new Job(resources.getCpus(), resources.getMem(), command, scriptMeta.getImagePath(),
                        MesosConstants.CONTAINER_TYPE, MesosConstants.CONTAINER_NETWORK, mesosContainerOptions, null, getMountVolumes(),
                        retries, id, jobName, false);
                jobConfigPayloadMap.put(job, configPayload);
                jobList.add(job);
            }
            jobGroups.add(new JobGroup(jobList));
        }
        updatePipelineStepAudits(jobConfigPayloadMap, PipelineStepStatus.INITIATED);
        return jobGroups;
    }

    private String getExecutionDriverName() {
        return "WorkFlowMesosApplication";
    }

    private String generateCommand(ConfigPayload configPayload, String startUpScriptPath, String executionDriverName) {
        String executorJarVersion = miscConfig.getExecutorJarVersion();
        startUpScriptPath = startUpScriptPath.replace("__VERSION__", executorJarVersion);
        String commandPrefix = "bash " + startUpScriptPath  + " ";
        return commandPrefix + executionDriverName + " " + System.getProperty(Constants.CONFIG_SVC_BUCKETS_KEY) +
                " \"" + StringEscapeUtils.escapeJava(JsonUtils.DEFAULT.toJson(configPayload)) + "\"";
    }


    private List<Job.Volume> getMountVolumes() {
        List<Job.Volume> mountVolumes = new ArrayList<>();
        for (Map.Entry<String, String> entry : mesosConfig.getMountInfo().entrySet()) {
            //todo: move mount mode to configuration
            mountVolumes.add(new Job.Volume(entry.getKey(), entry.getValue(), MesosConstants.MOUNT_MODE));
        }
        return mountVolumes;
    }

    private void updatePipelineStepAudits(Map<Job, ConfigPayload> jobConfigPayloadMap,
                                          PipelineStepStatus pipelineStepStatus) throws DSPCoreException {
        List<PipelineStepAudit> pipelineStepAudits = new ArrayList<>();
        for (Map.Entry<Job, ConfigPayload> entry : jobConfigPayloadMap.entrySet()) {
            Resources resources = new Resources(entry.getKey().getCpus(), entry.getKey().getMem());
            pipelineStepAudits.add(PipelineStepAudit.builder().logs(null).scope(entry.getValue().getScope())
                    .refreshId(entry.getValue().getRefreshId()).attempt(0).workflowId(entry.getValue().getWorkflowId())
                    .pipelineStepId(entry.getValue().getPipelineStepId()).resources(resources).pipelineStepStatus(pipelineStepStatus)
                    .workflowExecutionId(entry.getValue().getWorkflowExecutionId())
                    .pipelineExecutionId(entry.getValue().getPipelineExecutionId()).build());
        }
        int startIndex = 0;
        int endIndex = BATCH_SIZE;
        int pipelineStepAuditsSize = pipelineStepAudits.size();
        while(startIndex < pipelineStepAuditsSize) {
            if(endIndex > pipelineStepAuditsSize) {
                endIndex = pipelineStepAuditsSize;
            }
            pipelineStepAuditActor.saveBatchAuditEntry(pipelineStepAudits.subList(startIndex, endIndex));
            log.debug("pipelineStepAudits index from " + startIndex + " to " + endIndex);
            startIndex = startIndex + BATCH_SIZE;
            endIndex = endIndex + BATCH_SIZE;
        }
    }

    private ConfigPayload generateShufflerConfigPayload(Long refreshId, Long workflowId, Long pipelineStepId,
                                                        String pipelineStepName, String workflowExecutionId) {
        ConfigPayload shufflerConfigPayload = new ConfigPayload();
        shufflerConfigPayload.setRefreshId(refreshId);
        shufflerConfigPayload.setWorkflowId(workflowId);
        shufflerConfigPayload.setWorkflowExecutionId(workflowExecutionId);
        shufflerConfigPayload.setPipelineExecutionId(UUID.randomUUID().toString());
        shufflerConfigPayload.setPipelineStepId(pipelineStepId);
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("signal_generation", pipelineStepName);
        shufflerConfigPayload.setPartitionValues(partitionMap);

        return shufflerConfigPayload;
    }

    //in case of agent went down or some cluster error when executor not able to update status
    private boolean handlePermanentFailedRun(List<JobGroup> updatedJobs, Map<String, List<Triplet<ConfigPayload, Long, Long>>> pipelineExecutionIdToTriplets) {
        boolean returnStatus = true;
        List<UpdateEntityDTO> failedPipelineExecutions = new ArrayList<>();
        for (JobGroup jobGroup : updatedJobs) {
            for (Job job : jobGroup.getJobList()) {
                if (job.getStatus() != Job.Status.SUCCESSFUL) {
                    String pipelineExecutionId = job.getId().split("#")[0];
                    long pipelineStepId = Long.valueOf(job.getId().split("#")[1]);
                    Triplet<ConfigPayload, Long, Long> triplet1 = pipelineExecutionIdToTriplets.get(pipelineExecutionId).stream()
                            .filter(triplet -> triplet.getValue0().getPipelineStepId() == pipelineStepId).findAny()
                            .orElseThrow(() -> new RuntimeException("No ConfigPayload found for pipelineStepId: " + pipelineStepId + " for PipelineExecutionId: " + pipelineExecutionId));
                    failedPipelineExecutions.add(new UpdateEntityDTO(triplet1.getValue0().getRefreshId(), triplet1.getValue0().getWorkflowExecutionId(),
                            WorkflowStatus.FAILED.name(), triplet1.getValue0().getWorkflowId(), pipelineExecutionId));
                    returnStatus = false;
                }
            }
        }
        log.info("updating workflow audits for failed execution, count: {}", failedPipelineExecutions.size());
        dspServiceClient.updateWorkFlowAuditStatusBatch(failedPipelineExecutions);

        List<UpdateEntityDTO> permanentFailedPipelineExecutions = failedPipelineExecutions.stream().map(failedExecution ->
                new UpdateEntityDTO(failedExecution.getRefreshId(), failedExecution.getWorkflowExecutionId(), WorkflowStatus.PERMANENT_FAILED.name(),
                        failedExecution.getWorkflowId(), failedExecution.getPipelineExecutionId())).collect(Collectors.toList());
        log.info("updating workflow audits for permanent failed execution, count: {}", failedPipelineExecutions.size());
        dspServiceClient.updateWorkFlowAuditStatusBatch(permanentFailedPipelineExecutions);
        return returnStatus;
    }

    private boolean handleSGFailedRun(List<JobGroup> updatedJobs, Long refreshId, String workflowExecutionId, Long workflowId) {
        boolean returnStatus = true;
        List<UpdateEntityDTO> failedPipelineExecutions = new ArrayList<>();
        for (JobGroup jobGroup : updatedJobs) {
            for (Job job : jobGroup.getJobList()) {
                if (job.getStatus() != Job.Status.SUCCESSFUL) {
                    returnStatus = false;
                    log.info("updating workflow audits for failed execution of sg for workflow {}", workflowId);
                    failedPipelineExecutions.add(new UpdateEntityDTO(refreshId, workflowExecutionId,
                            WorkflowStatus.FAILED.name(), workflowId, null));
                    dspServiceClient.updateWorkFlowAuditStatusBatch(failedPipelineExecutions);
                    dspServiceClient.updateDataFrameAuditStatus(refreshId, workflowId, DataFrameAuditStatus.GENERATING_DATAFRAME.name(), DataFrameAuditStatus.FAILED.name());
                }
            }
        }
        return returnStatus;
    }

    private ScriptMeta getScriptMeta(PipelineStep pipelineStep) {
        return dspServiceClient.getScriptMetaById(pipelineStep.getScript().getId());
    }
}
