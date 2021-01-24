package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.WorkflowAuditDAO;
import com.flipkart.dsp.dao.WorkflowDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.WorkflowAuditEntity;
import com.flipkart.dsp.entities.misc.ExecutionDetails;
import com.flipkart.dsp.entities.workflow.WorkflowAudit;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.WorkflowStatus;
import com.flipkart.dsp.utils.MetricsRegistryHelper;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.flipkart.dsp.models.WorkflowStatus.*;
import static com.flipkart.dsp.utils.TimeUtils.convertMillisToHMmSs;
import static com.google.common.collect.Iterables.isEmpty;
import static java.util.stream.Collectors.toList;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkflowAuditActor implements SGActor<WorkflowAuditEntity, WorkflowAudit> {
    private final WorkflowDAO workflowDAO;
    private final WorkflowAuditDAO workflowAuditDAO;
    private final TransactionLender transactionLender;
    private final MetricsRegistryHelper metricsRegistryHelper;
    private final PipelineStepAuditActor pipelineStepAuditActor;

    @Override
    public WorkflowAuditEntity unWrap(WorkflowAudit dto) {
        if (Objects.nonNull(dto)) {
            return WorkflowAuditEntity.builder().refreshId(dto.getRefreshId()).workflowStatus(dto.getWorkflowStatus())
                    .workflowEntity(workflowDAO.get(dto.getWorkflowId()))
                    .workflowExecutionId(dto.getWorkflowExecutionId()).build();
        }
        return null;
    }

    @Override
    public WorkflowAudit wrap(WorkflowAuditEntity entity) {
        if (Objects.nonNull(entity)) {
            return WorkflowAudit.builder().refreshId(entity.getRefreshId()).workflowStatus(entity.getWorkflowStatus())
                    .workflowId(entity.getWorkflowEntity().getId()).workflowExecutionId(entity.getWorkflowExecutionId())
                    .createdAt(entity.getCreatedAt()).updatedAt(entity.getUpdatedAt()).build();
        }
        return null;
    }

    public void createWorkflowAudit(Long refreshId, Long workflowId, String workflowExecutionId) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() throws DSPCoreException {
                WorkflowAudit workflowAudit = WorkflowAudit.builder().refreshId(refreshId).workflowId(workflowId)
                .workflowExecutionId(workflowExecutionId).workflowStatus(WorkflowStatus.STARTED).build();
                workflowAuditDAO.persist(unWrap(workflowAudit));
            }
        }, "Error while saving workflow Audit ");
    }

    public void update(Long refreshId, Long workflowId, String workflowExecutionId, WorkflowStatus currentStatus) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                WorkflowAuditEntity workflowAuditEntity = workflowAuditDAO.getWorkflowAudit(refreshId, workflowId, workflowExecutionId);
                if (Objects.nonNull(workflowAuditEntity)) {
                    if (validateStateTransition(currentStatus, workflowAuditEntity.getWorkflowStatus())) {
                        workflowAuditEntity.setWorkflowStatus(currentStatus);
                        workflowAuditDAO.persist(workflowAuditEntity);
                        pushMetricsForWorkflowAudit(currentStatus.toString(), workflowExecutionId, refreshId);
                    }
                }
            }
        }, "Update of WorkflowAudit unsuccessful: ");
    }

    // Invalid Transition
    // case1: if status is already aborted
    // case2: if status is not failed and require PERMANENT_FAILED workflowStatus updating
    private boolean validateStateTransition(WorkflowStatus currentStatus, WorkflowStatus existingStatus) {
        return !existingStatus.equals(WorkflowStatus.ABORTED) && ((!currentStatus.equals(PERMANENT_FAILED)) || existingStatus.equals(FAILED));
    }

    private void pushMetricsForWorkflowAudit(String status, String workflowExecutionId, Long refreshId) throws DSPCoreException {
        if (WorkflowStatus.valueOf(status).equals(FAILED))
            metricsRegistryHelper.getMeterInstance(WorkflowAuditDAO.class, "pipelineFailure").mark();
        markPipelineStepHistogram(workflowExecutionId);
        if (workflowAuditDAO.getNumOfRunningWorkflowAudits(workflowExecutionId) == 0) {
            markHistogram("workFlowExecutionTime", getExecutionTimeForWorkflow(workflowExecutionId));
            if (workflowAuditDAO.getNumOfRunningWorkflowAudits(refreshId) == 0) {
                markHistogram("refreshIdE2eExecutionTime", getExecutionTimeForRefreshId(refreshId).getExecutionTime());
            }
        }
    }

    private void markPipelineStepHistogram(String workflowExecutionId) {
        ExecutionDetails executionDetails = pipelineStepAuditActor.getExecutionDetailsForPipelineStep(workflowExecutionId);
        if (Objects.nonNull(executionDetails))
            markHistogram("pipelineStepExecutionTime_" + executionDetails.getPipelineStepId(), executionDetails.getExecutionTime());
    }

    public Long getExecutionTimeForWorkflow(String workflowExecutionId) throws DSPCoreException {
        WorkflowAuditEntity workflowAuditEntity = workflowAuditDAO.getWorkflowAuditByWorkflowExecutionId(workflowExecutionId);
        if (Objects.isNull(workflowAuditEntity))
            throw new DSPCoreException("No workflowAudit entry found for workflowAudit " + workflowExecutionId);
        Date maxUpdatedAt = new Date();
        if (workflowAuditEntity.getWorkflowStatus().equals(FAILED) || workflowAuditEntity.getWorkflowStatus().equals(SUCCESS)
                || workflowAuditEntity.getWorkflowStatus().equals(ABORTED) || workflowAuditEntity.getWorkflowStatus().equals(PERMANENT_FAILED))
            maxUpdatedAt = workflowAuditEntity.getUpdatedAt();
        return maxUpdatedAt.getTime() - workflowAuditEntity.getCreatedAt().getTime();
    }

    public ExecutionDetails getExecutionTimeForRefreshId(Long refreshId) {
        List<WorkflowAuditEntity> workflowAuditEntities = workflowAuditDAO.getWorkflowAuditsByRequestId(refreshId);
        if (workflowAuditEntities.size() == 0)
            throw new DSPCoreException("No workflow audits found for refresh_id : " + refreshId);
        long successfulAuditCount = workflowAuditEntities.stream().filter(workflowAuditEntity -> workflowAuditEntity.getWorkflowStatus().equals(SUCCESS)).count();
        long failedAuditCount = workflowAuditEntities.stream().filter(workflowAuditEntity -> workflowAuditEntity.getWorkflowStatus()
                .equals(FAILED) && workflowAuditEntity.getWorkflowStatus().equals(PERMANENT_FAILED)).count();
        long executionTime = getMaxUpdatedAt(successfulAuditCount, failedAuditCount, workflowAuditEntities) - getMinCreatedAt(workflowAuditEntities);
        String status = getStatus(successfulAuditCount, failedAuditCount);
        return ExecutionDetails.builder().executionTime(executionTime).executionTimeStr(convertMillisToHMmSs(executionTime)).status(status).build();
    }

    private long getMaxUpdatedAt(long successfulAuditCount, long failedAuditCount, List<WorkflowAuditEntity> workflowAuditEntities) {
        if (successfulAuditCount == 0 && failedAuditCount == 0)
            return new Date().getTime(); // No Step is completed or failed so returning current date
        workflowAuditEntities.sort(Comparator.comparing(WorkflowAuditEntity::getUpdatedAt).reversed());
        return workflowAuditEntities.get(0).getUpdatedAt().getTime();
    }

    private long getMinCreatedAt(List<WorkflowAuditEntity> workflowAuditEntities) {
        workflowAuditEntities.sort(Comparator.comparing(WorkflowAuditEntity::getCreatedAt));
        return workflowAuditEntities.get(0).getCreatedAt().getTime();
    }

    private String getStatus(long successfulAuditCount, long failedAuditCount) {
        return successfulAuditCount == 0 && failedAuditCount == 0 ? "RUNNING" : successfulAuditCount != 0 ? "SUCCESS" : "FAILED";
    }

    private void markHistogram(String qualifier, Long executionTime) {
        metricsRegistryHelper.getHistogramInstance(WorkflowAuditActor.class, qualifier).update(executionTime);
    }

    public boolean isWorkflowSuccessful(String workflowExecutionId) {
        List<WorkflowAuditEntity> workflowAuditEntities = workflowAuditDAO.getWorkflowAuditsByWorkflowExecId(workflowExecutionId);
        long successCount = workflowAuditEntities.stream().filter(workflowAuditEntity -> workflowAuditEntity.getWorkflowStatus().equals(SUCCESS)).count();
        return !isEmpty(workflowAuditEntities) && successCount > 0;
    }

    public List<WorkflowAudit> getWorkflowAudits(String workflowExecutionId) {
        AtomicReference<List<WorkflowAuditEntity>> listAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                listAtomicReference.set(workflowAuditDAO.getWorkflowAuditsByWorkflowExecId(workflowExecutionId));
            }
        }, "Error while getting workflow_audits for workflow_execution_id: " + workflowExecutionId);
        return listAtomicReference.get().stream().map(this::wrap).collect(toList());
    }

    public void markWorkflowAuditsAborted(String workflowExecutionId) {
        AtomicReference<Integer> abortedPipelineExecIds = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                List<WorkflowAuditEntity> audits = workflowAuditDAO.getWorkflowAuditsByWorkflowExecId(workflowExecutionId);
                audits.forEach(audit -> {
                    audit.setWorkflowStatus(WorkflowStatus.ABORTED);
                    workflowAuditDAO.persist(audit);
                });
                abortedPipelineExecIds.set(audits.size());
            }
        });
        log.warn("Aborted {} pipelineExecutionIds for workflow execution Id {}", abortedPipelineExecIds.get(), workflowExecutionId);
    }

    public String getLatestWorkflowExecutionId(Long refreshId, Long workflowId) {
        AtomicReference<WorkflowAuditEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(workflowAuditDAO.getLatestWorkflowAudit(refreshId, workflowId));
            }
        });
        if (Objects.isNull(atomicReference.get())) return null;
        return atomicReference.get().getWorkflowExecutionId();
    }

    public WorkflowStatus getWorkflowStatus(String workflowExecutionId, RequestStatus requestStatus) {
        List<WorkflowAuditEntity> workflowAuditEntities = workflowAuditDAO.getWorkflowAuditsByWorkflowExecId(workflowExecutionId);
        if (isEmpty(workflowAuditEntities)) return RequestStatus.FAILED.equals(requestStatus) ? FAILED : STARTED;

        boolean anyStepFailed = workflowAuditEntities.stream().anyMatch(workflowAuditEntity -> workflowAuditEntity.getWorkflowStatus().equals(PERMANENT_FAILED));
        long runningCount = workflowAuditEntities.stream().filter(workflowAuditEntity -> !workflowAuditEntity.getWorkflowStatus().equals(SUCCESS)).count();
        return anyStepFailed ? FAILED : runningCount > 0 ? RUNNING : SUCCESS;
    }

    public List<WorkflowAudit> getWorkflowAudits(Long refreshId) {
        AtomicReference<List<WorkflowAuditEntity>> listAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                listAtomicReference.set(workflowAuditDAO.getWorkflowAuditsByRequestId(refreshId));
            }
        });
        List<WorkflowAuditEntity> workflowAuditEntities = listAtomicReference.get();
        workflowAuditEntities.sort(Comparator.comparing(WorkflowAuditEntity::getId));
        return workflowAuditEntities.stream().map(this::wrap).collect(Collectors.toList());
    }
}
