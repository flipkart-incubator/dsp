package com.flipkart.dsp.mesos;

import com.flipkart.dsp.actors.PipelineStepAuditActor;
import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepResources;
import com.flipkart.dsp.entities.sg.core.Label;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.google.inject.Inject;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Triplet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ResourceComputeDriver {

    private final DSPServiceConfig.MesosConfig mesosConfig;
    private final PipelineStepAuditActor pipelineStepAuditActor;

    @Getter
    @Setter
    @ToString
    public static class Resources {

        private double cpus;
        private double mem;

        public Resources(double cpus, double mem, DSPServiceConfig.MesosConfig mesosConfig) {
            if (cpus == 0 || mem == 0) {
                throw new IllegalArgumentException("Resources cannot be zero!");
            }
            this.cpus = Double.min(mesosConfig.getMaxAgentCpu(), cpus);
            this.mem = Double.min(mesosConfig.getMaxAgentMemory(), mem);
        }
    }

    private Resources computeResources(WorkflowDetails workflowDetails, Triplet<ConfigPayload, Long/*training dataframe size*/, Long/*execution dataframe size*/> triplet) {
        //based on minimum resources +  coefficient based on some heuristic(dataframe size + some other variable)
        ConfigPayload configPayload = triplet.getValue0();
        Optional<PipelineStep> pipelineStepOption = workflowDetails.getPipelineSteps().stream()
                .filter(pipelineStep -> pipelineStep.getId() == configPayload.getPipelineStepId()).findFirst();
        PipelineStepResources pipelineStepResources = null;
        if (pipelineStepOption.isPresent()) {
            PipelineStep pipelineStep = pipelineStepOption.get();
            pipelineStepResources = pipelineStep.getPipelineStepResources();
        } else {
            throw new AzkabanException("Pipeline step " + configPayload.getPipelineStepId() + " not found in workflow Details of config payload.");
        }
        if (pipelineStepResources == null) {
            throw new AzkabanException("Pipeline Step Resources cannot be null for resource computation.");
        }
        Double trainingDataframeSizeInMB = new Double(triplet.getValue1()) / (1024 * 1024);
        Double executionDataframeSizeInMB = new Double(triplet.getValue2()) / (1024 * 1024);

        Double baseCpu = pipelineStepResources.getBaseCpu();
        Double trainingCpuCoefficient = pipelineStepResources.getTrainingCpuCoefficient();
        Double executionCpuCoefficient = pipelineStepResources.getExecutionCpuCoefficient();
        Long baseMemory = pipelineStepResources.getBaseMemory();
        Double trainingMemoryCoefficient = pipelineStepResources.getTrainingMemoryCoefficient();
        Double executionMemoryCoefficient = pipelineStepResources.getExecutionMemoryCoefficient();

        double cpus = baseCpu + trainingDataframeSizeInMB * trainingCpuCoefficient + executionDataframeSizeInMB * executionCpuCoefficient;
        double mem = baseMemory + trainingDataframeSizeInMB * trainingMemoryCoefficient + executionDataframeSizeInMB * executionMemoryCoefficient;

        return new Resources(cpus, mem, mesosConfig);
    }

    public Map<String, Map<Long, Resources>> computeResources(WorkflowDetails workflowDetails,
                                                              List<Triplet<ConfigPayload, @Label("raining dataframe size") Long,
                                                              @Label("execution dataframe size") Long>> tripletList) {
        Map<String, Map<Long, Resources>> resourcesMap = new HashMap<>();
        final ConfigPayload configPayload1 = tripletList.get(0).getValue0();
        Long workflowId = workflowDetails.getWorkflow().getId();
        Long refreshId = configPayload1.getRefreshId();
        Map<String, Map<Long, com.flipkart.dsp.entities.misc.Resources>> latestWorkflowResources = new HashMap<>();
        try {
            latestWorkflowResources = pipelineStepAuditActor.getLatestWorkflowResources(workflowId, refreshId);
        } catch (Exception e) {
            log.warn("Failed to fetch resources consumed from previous run, falling back to co-efficient based resource computation");
        }
        Map<String, Map<Long, com.flipkart.dsp.entities.misc.Resources>> pipelineStepResourcesMap = latestWorkflowResources;
        tripletList.forEach(triplet -> {
            Resources resources = computeResources(workflowDetails, triplet);
            final ConfigPayload configPayload = triplet.getValue0();
            final String scope = configPayload.getScope();
            Long pipelineStepId = configPayload.getPipelineStepId();
            if (pipelineStepResourcesMap.containsKey(scope) && pipelineStepResourcesMap.get(scope).containsKey(pipelineStepId)) {
                com.flipkart.dsp.entities.misc.Resources resourcesFromPreviousRun = pipelineStepResourcesMap.get(scope).get(pipelineStepId);
                if (resourcesFromPreviousRun.getMemory() > resources.mem) {
                    resources.setMem(resourcesFromPreviousRun.getMemory());
                }
            }
            final String pipelineExecutionId = configPayload.getPipelineExecutionId();
            if (resourcesMap.containsKey(pipelineExecutionId)) {
                resourcesMap.get(pipelineExecutionId).put(pipelineStepId, resources);
            } else {
                resourcesMap.put(pipelineExecutionId, new HashMap<Long, Resources>() {
                    {
                        put(pipelineStepId, resources);
                    }
                });
            }
        });
        return resourcesMap;
    }

}
