package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.PipelineStepSGAuditDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.PipelineStepSGAuditEntity;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepSGAudit;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.models.PipelineStepStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PipelineStepSGAuditActor implements SGActor<PipelineStepSGAuditEntity, PipelineStepSGAudit> {
    private final TransactionLender transactionLender;
    private final PipelineStepSGAuditDAO pipelineStepSGAuditDAO;

    @Override
    public PipelineStepSGAuditEntity unWrap(PipelineStepSGAudit dto) {
        if (Objects.nonNull(dto))
            return PipelineStepSGAuditEntity.builder().id(dto.getId()).logs(dto.getLogs())
                    .refreshId(dto.getRefreshId()).pipelineStep(dto.getPipelineStep()).status(dto.getStatus().name())
                    .pipelineExecutionId(dto.getPipelineExecutionId()).workflowExecutionId(dto.getWorkflowExecutionId()).build();
        return null;
    }

    @Override
    public PipelineStepSGAudit wrap(PipelineStepSGAuditEntity entity) {
        if (Objects.nonNull(entity))
            return PipelineStepSGAudit.builder().id(entity.getId()).logs(entity.getLogs()).refreshId(entity.getRefreshId())
                    .createdAt(entity.getCreatedAt()).updatedAt(entity.getUpdatedAt())
                    .pipelineStep(entity.getPipelineStep()).status(PipelineStepStatus.valueOf(entity.getStatus()))
                    .pipelineExecutionId(entity.getPipelineExecutionId()).workflowExecutionId(entity.getWorkflowExecutionId()).build();
        return null;
    }

    public Long saveAuditEntry(PipelineStepSGAudit pipelineStepSGAudit) throws DSPCoreException {
        AtomicReference<PipelineStepSGAuditEntity> audit = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                audit.set(pipelineStepSGAuditDAO.saveOrUpdate(unWrap(pipelineStepSGAudit)));
            }
        }, "Creation of PipelineStepEntity Audit unsuccessful");
        return audit.get().getId();
    }

    public PipelineStepSGAuditEntity getById(Long id) {
        return pipelineStepSGAuditDAO.get(id);
    }

    public List<PipelineStepSGAudit> getPipelineStepSgAudits(String workflowExecutionId) {
        AtomicReference<List<PipelineStepSGAuditEntity>> listAtomicReference = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                listAtomicReference.set(pipelineStepSGAuditDAO.getPipelineStepSGAudits(workflowExecutionId));
            }
        }, "Creation of PipelineStepEntity Audit unsuccessful");
        return listAtomicReference.get().stream().map(this::wrap).collect(Collectors.toList());
    }
}
