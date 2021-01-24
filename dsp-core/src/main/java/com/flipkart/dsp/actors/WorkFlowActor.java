package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.DataFrameDAO;
import com.flipkart.dsp.dao.PipelineStepDAO;
import com.flipkart.dsp.dao.WorkflowDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.db.entities.PipelineStepEntity;
import com.flipkart.dsp.db.entities.WorkflowEntity;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.sg.DataFrame;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkFlowActor implements SGActor<WorkflowEntity, Workflow> {

    private final UserActor userActor;
    private final WorkflowDAO workflowDAO;
    private final DataFrameDAO dataFrameDAO;
    private final DataFrameActor dataFrameActor;
    private final PipelineStepDAO pipelineStepDAO;
    private final TransactionLender transactionLender;
    private final WorkFlowMetaActor workFlowMetaActor;
    private final PipelineStepActor pipelineStepActor;

    @Override
    public WorkflowEntity unWrap(Workflow dto) {
        if (Objects.nonNull(dto)) {
            Set<DataFrameEntity> dataFrameEntities = dto.getDataFrames().stream()
                    .map(dataFrame -> dataFrameDAO.getDataFrame(dataFrame.getId())).collect(toSet());
            WorkflowEntity workflowEntity = WorkflowEntity.builder().name(dto.getName()).workflowGroupName(dto.getWorkflowGroupName())
                    .description(dto.getDescription()).dataFrames(dataFrameEntities).retries(dto.getRetries())
                    .version(dto.getVersion()).workflowMetaEntity(workFlowMetaActor.unWrap(dto.getWorkflowMeta()))
                    .defaultOverrides(dto.getDefaultOverrides()).subscriptionId(dto.getSubscriptionId())
                    .createdBy(userActor.getUserByName(dto.getCreatedBy())).isProd(dto.getIsProd()).build();
            if (dto.getParentWorkflowId() != null)
                workflowEntity.setParentWorkFlow(workflowDAO.get(dto.getParentWorkflowId()));
            return workflowEntity;
        }
        return null;
    }

    @Override
    public Workflow wrap(WorkflowEntity entity) {
        if (Objects.nonNull(entity)) {
            Long parentWorkflowId = isNull(entity.getParentWorkFlow()) ? null : entity.getParentWorkFlow().getId();
            Set<DataFrame> dataFrames = entity.getDataFrames().stream().map(dataFrameActor::wrap).collect(toSet());
            return Workflow.builder().id(entity.getId()).name(entity.getName()).description(entity.getDescription())
                    .parentWorkflowId(parentWorkflowId).retries(entity.getRetries()).dataFrames(dataFrames).isProd(entity.isProd())
                    .version(entity.getVersion()).defaultOverrides(entity.getDefaultOverrides())
                    .workflowMeta(workFlowMetaActor.wrap(entity.getWorkflowMetaEntity())).subscriptionId(entity.getSubscriptionId())
                    .workflowGroupName(entity.getWorkflowGroupName()).createdBy(entity.getCreatedBy().getUserId()).build();
        }
        return null;
    }

    public WorkflowDetails getWorkflowDetailsById(Long id) {
        AtomicReference<WorkflowDetails> atomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                Workflow workflow = wrap(workflowDAO.get(id));
                if (Objects.nonNull(workflow)) {
                    List<PipelineStepEntity> pipelineStepsInternalEntity = pipelineStepDAO.getPipelineStepsByWorkflowId(workflow.getId());
                    atomicReference.set(WorkflowDetails.builder().workflow(workflow)
                            .pipelineSteps(pipelineStepsInternalEntity.stream().map(pipelineStepActor::wrap).collect(toList())).build());
                }
            }
        }, "Error while getting workflowDetails for Id: " + id);
        return atomicReference.get();
    }

    public Workflow getWorkFlowById(Long id) {
        AtomicReference<WorkflowEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(workflowDAO.get(id));
            }
        }, "Error while getting workflow for Id: " + id);
        return wrap(atomicReference.get());
    }

    public WorkflowEntity save(Workflow workflow) {
        AtomicReference<WorkflowEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(workflowDAO.persist(unWrap(workflow)));
            }
        });
        return atomicReference.get();
    }

    public Long getWorkFlowCount(Long dataFrameId) {
        AtomicReference<Long> workflowAtomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                workflowAtomicReference.set(workflowDAO.getWorkFlowCount(dataFrameId));
            }
        });
        return workflowAtomicReference.get();
    }

    public List<Workflow> getWorkflow(String workflowName, String workflowGroupName, boolean isProd) {
        AtomicReference<List<WorkflowEntity>> atomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(workflowDAO.getWorkflow(workflowName, workflowGroupName, isProd, null));
            }
        });
        return atomicReference.get().stream().map(this::wrap).collect(toList());
    }

    private List<Workflow> getWorkFlowsBySubscriptionId(String subscriptionId) {
        AtomicReference<List<WorkflowEntity>> atomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(workflowDAO.getWorkFlowsBySubscriptionId(subscriptionId));
            }
        });
        return atomicReference.get().stream().map(this::wrap).collect(toList());
    }

    public List<WorkflowDetails> getWorkFlowDetailsBySubscriptionId(String subscriptionId) {
        List<WorkflowDetails> workflowDetails = new ArrayList<>();
        List<Workflow> workFlows = getWorkFlowsBySubscriptionId(subscriptionId);
        workFlows.forEach(workflow -> {
            List<PipelineStep> pipelineSteps = pipelineStepActor.getPipelineStepsByWorkflowId(workflow.getId());
            workflowDetails.add(WorkflowDetails.builder().workflow(workflow).pipelineSteps(pipelineSteps).build());
        });
        return workflowDetails;
    }

    public List<String> getAllDistinctWorkFlowNames() {
        AtomicReference<List<String>> listAtomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                listAtomicReference.set(workflowDAO.getAllDistinctWorkFlowNames());
            }
        });
        return listAtomicReference.get();
    }

    public Workflow getWorkflow(String workflowName, boolean isProd, String version) {
        AtomicReference<List<WorkflowEntity>> atomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(workflowDAO.getWorkflow(workflowName, null, isProd, version));
            }
        });
        if (atomicReference.get().isEmpty())
            return null;
        return wrap(atomicReference.get().get(0));
    }

}
