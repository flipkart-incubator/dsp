package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.PipelineStepDAO;
import com.flipkart.dsp.dao.PipelineStepRuntimeConfigDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.PipelineStepRuntimeConfigEntity;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig;
import com.flipkart.dsp.entities.run.config.RunConfig;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 */
@Slf4j
@Singleton
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PipelineStepRuntimeConfigActor implements SGActor<PipelineStepRuntimeConfigEntity, PipelineStepRuntimeConfig> {
    private final PipelineStepDAO pipelineStepDAO;
    private final TransactionLender transactionLender;
    private final PipelineStepRuntimeConfigDAO pipelineStepRuntimeConfigDAO;

    @Override
    public PipelineStepRuntimeConfigEntity unWrap(com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig runtimeConfig) {
        if (Objects.nonNull(runtimeConfig)) {
            return PipelineStepRuntimeConfigEntity.builder().workflowExecutionId(runtimeConfig.getWorkflowExecutionId())
                    .pipelineExecutionId(runtimeConfig.getPipelineExecutionId()).scope(runtimeConfig.getScope())
                    .pipelineStepEntity(pipelineStepDAO.getPipelineStepById(runtimeConfig.getPipelineStepId()))
                    .runConfig(JsonUtils.DEFAULT.toJson(runtimeConfig.getRunConfig())).ts(new Timestamp(System.currentTimeMillis())).build();
        }
        return null;
    }

    @Override
    public PipelineStepRuntimeConfig wrap(PipelineStepRuntimeConfigEntity runtimeConfig) {
        if (Objects.nonNull(runtimeConfig)) {
            return PipelineStepRuntimeConfig.builder().id(runtimeConfig.getId()).scope(runtimeConfig.getScope())
                    .workflowExecutionId(runtimeConfig.getWorkflowExecutionId()).ts(runtimeConfig.getTs())
                    .pipelineStepId(runtimeConfig.getPipelineStepEntity().getId())
                    .pipelineExecutionId(runtimeConfig.getPipelineExecutionId())
                    .runConfig(JsonUtils.DEFAULT.fromJson(runtimeConfig.getRunConfig(), RunConfig.class)).build();
        }
        return null;
    }

    public long save(PipelineStepRuntimeConfig pipelineStepRuntimeConfig) {
        AtomicReference<Long> id = new AtomicReference<>(0L);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                id.set(pipelineStepRuntimeConfigDAO.persist(unWrap(pipelineStepRuntimeConfig)).getId());
            }
        }, "Creation of PipelineStepRuntimeConfigEntity unsuccessful");
        return id.get();
    }

    public PipelineStepRuntimeConfig getPipelineStepRuntimeConfig(String pipelineExecutionId, Long pipelineStepId) throws DSPCoreException {
        AtomicReference<com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(wrap(pipelineStepRuntimeConfigDAO.get(pipelineExecutionId, pipelineStepId)));
            }
        });
        if (Objects.isNull(atomicReference.get()))
            throw new DSPCoreException("No config with execution id : " + pipelineExecutionId + " and pipelineStepId : " + pipelineStepId + " found.");
        return atomicReference.get();
    }

    public List<PipelineStepRuntimeConfig> getPipelineStepRuntimeConfigs(String workflowExecutionId) {
        AtomicReference<List<PipelineStepRuntimeConfig>> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(pipelineStepRuntimeConfigDAO.getByWorkflowExecutionId(workflowExecutionId).stream().map(p -> wrap(p))
                        .collect(Collectors.toList()));
            }
        });
        return atomicReference.get();
    }

    public List<PipelineStepRuntimeConfig> getPipelineStepRuntimeConfigsByScope(String workflowExecutionId, String scope) {
        AtomicReference<List<PipelineStepRuntimeConfig>> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(pipelineStepRuntimeConfigDAO.getByWorkflowExecutionIdAndScope(workflowExecutionId, scope)
                        .stream().map(p -> wrap(p)).collect(Collectors.toList()));
            }
        });
        return atomicReference.get();
    }
}
