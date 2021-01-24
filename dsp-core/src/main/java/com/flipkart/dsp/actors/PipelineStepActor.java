package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.PipelineStepDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.PipelineStepEntity;
import com.flipkart.dsp.db.entities.PipelineStepPartitionEntity;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepResources;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@Singleton
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PipelineStepActor implements SGActor<PipelineStepEntity, PipelineStep> {
    private final ScriptActor scriptActor;
    private final PipelineStepDAO pipelineStepDAO;
    private final TransactionLender transactionLender;

    @Override
    public PipelineStepEntity unWrap(PipelineStep pipelineStep) {
        return null;
    }

    @Override
    public PipelineStep wrap(PipelineStepEntity pipelineStepEntity) {
        if (Objects.nonNull(pipelineStepEntity)) {
            PipelineStepEntity parentStep = pipelineStepEntity.getParentPipelineStepEntity();
            return PipelineStep.builder().id(pipelineStepEntity.getId()).name(pipelineStepEntity.getName())
                    .script(scriptActor.wrap(pipelineStepEntity.getScriptEntity()))
                    .prevStepName(parentStep != null ? parentStep.getName() : null)
                    .partitions(getPartitions(pipelineStepEntity.getPipelineStepPartitionEntities()))
                    .pipelineStepResources(JsonUtils.DEFAULT.fromJson(pipelineStepEntity.getPipelineStepResources(), PipelineStepResources.class))
                    .parentPipelineStepId(parentStep != null ? parentStep.getId() : null)
                    .build();
        }
        return null;
    }

    public List<PipelineStep> wrap(List<PipelineStepEntity> pipelineStepEntities) {
        return pipelineStepEntities.stream().map(this::wrap).collect(Collectors.toList());
    }

    private List<String> getPartitions(List<PipelineStepPartitionEntity> pipelineStepPartitionEntities) {
        return pipelineStepPartitionEntities.stream().map(PipelineStepPartitionEntity::getStepPartition).collect(Collectors.toList());
    }

    public PipelineStep getPipelineStepById(Long pipelineStepId) {
        AtomicReference<PipelineStep> atomicReference = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(wrap(pipelineStepDAO.getPipelineStepById(pipelineStepId)));
            }
        });
        return atomicReference.get();
    }

    public List<PipelineStep> getPipelineStepsByWorkflowId(long workflowId) {
        return wrap(pipelineStepDAO.getPipelineStepsByWorkflowId(workflowId));
    }

    public PipelineStepEntity getPipelineStepByPipelineStepId(Long pipelineStepId) {
        AtomicReference<PipelineStepEntity> pipelineStepAtomicReference = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                pipelineStepAtomicReference.set(pipelineStepDAO.getPipelineStepById(pipelineStepId));
            }
        });
        return pipelineStepAtomicReference.get();
    }

}
