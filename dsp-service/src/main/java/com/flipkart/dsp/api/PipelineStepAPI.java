package com.flipkart.dsp.api;

import com.flipkart.dsp.actors.ScriptActor;
import com.flipkart.dsp.dao.PipelineStepDAO;
import com.flipkart.dsp.db.entities.PipelineStepEntity;
import com.flipkart.dsp.db.entities.PipelineStepPartitionEntity;
import com.flipkart.dsp.db.entities.WorkflowEntity;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.exceptions.InvalidConfigException;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PipelineStepAPI {
    private static final Long TERMINAL_POINT = -1L;

    private final ScriptActor scriptActor;
    private final PipelineStepDAO pipelineStepDAO;

    void createPipelineSteps(WorkflowEntity workflow, List<PipelineStep> pipelineStepList) {
        Map<String, List<String>> pipelineStepDag = new HashMap<>();
        Map<String, PipelineStep> pipelineStepMap = getPipelineStepMap(pipelineStepList, pipelineStepDag);
        Set<String> pipelineStepAlias = new HashSet<>(pipelineStepMap.keySet());
        isDagDisconnected(pipelineStepDag, pipelineStepAlias);
        Queue<String> queueOfPipelineStep = new LinkedList<>();
        Queue<Long> queueOfParentPipelineStepId = new LinkedList<>();

        List<String> dependentSteps = pipelineStepDag.get(null);
        for (String stepNames : dependentSteps) {
            queueOfPipelineStep.add(stepNames);
            queueOfParentPipelineStepId.add(TERMINAL_POINT);
        }

        while (!queueOfPipelineStep.isEmpty()) {
            String currentStepAlias = queueOfPipelineStep.poll();
            long parentPipelineStepId = queueOfParentPipelineStepId.poll();
            PipelineStep pipelineStepExternal = pipelineStepMap.get(currentStepAlias);
            PipelineStepEntity pipelineStepInternal = getInternalPipelineStep(workflow.getId(), pipelineStepExternal, parentPipelineStepId);
            Long currentPipelineStepId = pipelineStepDAO.persist(pipelineStepInternal).getId();
            pipelineStepExternal.setId(currentPipelineStepId);

            dependentSteps = pipelineStepDag.get(currentStepAlias);
            if (dependentSteps != null)
                for (String stepNames : dependentSteps) {
                    queueOfPipelineStep.add(stepNames);
                    queueOfParentPipelineStepId.add(currentPipelineStepId);
                }
        }
    }

    private Map<String, PipelineStep> getPipelineStepMap(List<PipelineStep> pipelineStepList, Map<String, List<String>> pipelineStepDag) {
        Map<String, PipelineStep> pipelineStepMap = new HashMap<>();

        for (PipelineStep pipelineStepEntity : pipelineStepList) {
            String currentStepAlias = String.valueOf(pipelineStepEntity.getName());
            String previousStepAlias = pipelineStepEntity.getPrevStepName();
            pipelineStepMap.put(currentStepAlias, pipelineStepEntity);
            List<String> currentStepAliasList = pipelineStepDag.containsKey(previousStepAlias) ? pipelineStepDag.get(previousStepAlias) : new ArrayList<>();
            currentStepAliasList.add(currentStepAlias);
            pipelineStepDag.put(previousStepAlias, currentStepAliasList);
        }
        return pipelineStepMap;
    }

    private PipelineStepEntity getInternalPipelineStep(Long workflowId, PipelineStep pipelineStepExternal,
                                                       long parentPipelineStepId) {

        PipelineStepEntity pipelineStepInternal = new PipelineStepEntity();
        if (parentPipelineStepId != TERMINAL_POINT) {
            pipelineStepInternal.setParentPipelineStepEntity(pipelineStepDAO.getPipelineStepById(parentPipelineStepId));
        }
        pipelineStepInternal.setWorkflowId(workflowId);
        if (pipelineStepExternal.getScript() != null) {
            pipelineStepInternal.setScriptEntity(scriptActor.unWrap(pipelineStepExternal.getScript()));
        }
        pipelineStepInternal.setName(pipelineStepExternal.getName());
        pipelineStepInternal.setPipelineStepResources(JsonUtils.DEFAULT.toJson(pipelineStepExternal.getPipelineStepResources()));
        pipelineStepInternal.setPipelineStepPartitionEntities(getPipelineStepPartitions(pipelineStepExternal.getPartitions(), pipelineStepInternal));
        return pipelineStepInternal;
    }

    private List<PipelineStepPartitionEntity> getPipelineStepPartitions(List<String> partitions, PipelineStepEntity pipelineStepEntity) {
        return Objects.isNull(partitions) ? new ArrayList<>() : partitions.stream().map(partition -> PipelineStepPartitionEntity.builder()
                .pipelineStepEntity(pipelineStepEntity).stepPartition(partition).build()).collect(toList());
    }

    private void isDagDisconnected(Map<String, List<String>> pipelineStepDag, Set<String> pipelineStepAlias) {
        List<String> dependentSteps = pipelineStepDag.get(null);

        if (dependentSteps.isEmpty()) {
            throw new InvalidConfigException("Creation of workflow unsuccessful, No start point in PipelineStepEntity");
        } else if (dependentSteps.size() > 1) {
            throw new InvalidConfigException("Creation of workflow unsuccessful, Multiple start point in PipelineStepEntity");
        }

        Queue<String> queueOfPipelineStepAlias = new LinkedList<>(dependentSteps);

        while (!queueOfPipelineStepAlias.isEmpty()) {
            String currentStepAlias = queueOfPipelineStepAlias.poll();
            pipelineStepAlias.remove(currentStepAlias);

            dependentSteps = pipelineStepDag.get(currentStepAlias);
            if (dependentSteps != null)
                queueOfPipelineStepAlias.addAll(dependentSteps);
        }

        if (pipelineStepAlias.size() > 0) {
            throw new InvalidConfigException("Creation of workflow unsuccessful, PipelineStepDag is disconnected");
        }
    }

}
