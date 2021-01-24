package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.PipelineStepAuditDAO;
import com.flipkart.dsp.dao.RequestDAO;
import com.flipkart.dsp.dao.WorkflowAuditDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.PipelineStepAuditEntity;
import com.flipkart.dsp.db.entities.RequestEntity;
import com.flipkart.dsp.entities.misc.ExecutionDetails;
import com.flipkart.dsp.entities.misc.Resources;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.exceptions.PipelineStepMissingException;
import com.flipkart.dsp.models.PipelineStepStatus;
import com.flipkart.dsp.utils.JsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PipelineStepAuditActor implements SGActor<PipelineStepAuditEntity, PipelineStepAudit> {
    private final RequestDAO requestDAO;
    private final WorkflowAuditDAO workflowAuditDAO;
    private final TransactionLender transactionLender;
    private final PipelineStepAuditDAO pipelineStepAuditDAO;

    @Override
    public PipelineStepAuditEntity unWrap(PipelineStepAudit pipelineStepAudit) {
        return PipelineStepAuditEntity.builder().id(pipelineStepAudit.getId()).logs(pipelineStepAudit.getLogs())
                .scope(pipelineStepAudit.getScope()).refreshId(pipelineStepAudit.getRefreshId())
                .attempt(pipelineStepAudit.getAttempt()).workflowId(pipelineStepAudit.getWorkflowId())
                .pipelineStepId(pipelineStepAudit.getPipelineStepId())
                .resources(JsonUtils.DEFAULT.toJson(pipelineStepAudit.getResources()))
                .pipelineStepStatus(pipelineStepAudit.getPipelineStepStatus())
                .workflowExecutionId(pipelineStepAudit.getWorkflowExecutionId())
                .pipelineExecutionId(pipelineStepAudit.getPipelineExecutionId()).build();
    }

    @Override
    public PipelineStepAudit wrap(PipelineStepAuditEntity pipelineStepAuditEntity) {
        return PipelineStepAudit.builder().id(pipelineStepAuditEntity.getId()).logs(pipelineStepAuditEntity.getLogs())
                .scope(pipelineStepAuditEntity.getScope()).refreshId(pipelineStepAuditEntity.getRefreshId())
                .attempt(pipelineStepAuditEntity.getAttempt()).workflowId(pipelineStepAuditEntity.getWorkflowId())
                .pipelineStepId(pipelineStepAuditEntity.getPipelineStepId())
                .createdAt(pipelineStepAuditEntity.getCreatedAt()).updatedAt(pipelineStepAuditEntity.getUpdatedAt())
                .resources(JsonUtils.DEFAULT.fromJson(pipelineStepAuditEntity.getResources(), Resources.class))
                .pipelineStepStatus(pipelineStepAuditEntity.getPipelineStepStatus())
                .workflowExecutionId(pipelineStepAuditEntity.getWorkflowExecutionId())
                .pipelineExecutionId(pipelineStepAuditEntity.getPipelineExecutionId())
                .build();
    }

    ExecutionDetails getExecutionDetailsForPipelineStep(String workflowExecutionId) throws DSPCoreException {
        List<PipelineStepAudit> pipelineStepAudits = getPipelineStepAudits(null, null, null,
                null, workflowExecutionId);
        if (pipelineStepAudits.size() == 0)
            return null;
        pipelineStepAudits.sort(Comparator.comparing(PipelineStepAudit::getAttempt).reversed());
        Long executionTime = getExecutionTime(pipelineStepAudits.get(0));
        return ExecutionDetails.builder().executionTime(executionTime).pipelineStepId(pipelineStepAudits.get(0).getPipelineStepId())
                .status(pipelineStepAudits.get(0).getPipelineStepStatus().name()).build();
    }

    private Long getExecutionTime(PipelineStepAudit pipelineStepAudit) {
        Date maxUpdatedAt = (pipelineStepAudit.getPipelineStepStatus().equals(PipelineStepStatus.FAILED)
                || pipelineStepAudit.getPipelineStepStatus().equals(PipelineStepStatus.SUCCESS)) ? pipelineStepAudit.getUpdatedAt() : new Date();
        return maxUpdatedAt.getTime() - pipelineStepAudit.getCreatedAt().getTime();
    }

    public void saveBatchAuditEntry(List<PipelineStepAudit> pipelineStepAuditList) throws DSPCoreException {
        for (PipelineStepAudit pipelineStepAudit : pipelineStepAuditList) {
            saveAuditEntry(pipelineStepAudit);
        }
    }

    public PipelineStepAudit saveAuditEntry(PipelineStepAudit pipelineStepAudit) throws DSPCoreException {
        AtomicReference<PipelineStepAuditEntity> atomicReference = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(pipelineStepAuditDAO.save(unWrap(pipelineStepAudit)));
            }
        }, "Creation of PipelineStepEntity Audit unsuccessful. ");
        return wrap(atomicReference.get());
    }

    public PipelineStepAudit getPipelineStepAuditsById(Long pipelineStepAuditId) {
        AtomicReference<PipelineStepAudit> atomicReference = new AtomicReference<>();
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(wrap(pipelineStepAuditDAO.get(pipelineStepAuditId)));
            }
        });
        return atomicReference.get();
    }

    public ExecutionDetails getExecutionDetailsForPipelineStep(Long pipelineStepId, String pipelineExecutionId) throws DSPCoreException {
        List<PipelineStepAudit> pipelineStepAudits = getPipelineStepAudits(null, null, pipelineStepId, pipelineExecutionId, null);
        if (pipelineStepAudits.size() == 0)
            throw new DSPCoreException("No pipelineStepAudit entry found for workflowExecutionId for pipelineStepId "
                    + pipelineStepId + " and pipelineExecutionId " + pipelineExecutionId);
        pipelineStepAudits.sort(Comparator.comparing(PipelineStepAudit::getAttempt).reversed());
        Long executionTime = getExecutionTime(pipelineStepAudits.get(0));
        return ExecutionDetails.builder().executionTime(executionTime).executionTimeStr(executionTime.toString())
                .status(pipelineStepAudits.get(0).getPipelineStepStatus().name()).build();
    }

    public List<PipelineStepAudit> getPipelineStepAudits(Integer attempt, Long refreshId, Long pipelineStepId,
                                                          String pipelineExecutionId, String workflowExecutionId) {
        AtomicReference<List<PipelineStepAuditEntity>> atomicReference = new AtomicReference<>();
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(pipelineStepAuditDAO.getPipelineStepAudits(attempt, refreshId, pipelineStepId,
                        pipelineExecutionId, workflowExecutionId));
            }
        });
        return atomicReference.get().stream().map(this::wrap).collect(Collectors.toList());
    }

    public List<PipelineStepAudit> getPipelineStepAudits(Long refreshId) {
        return getPipelineStepAudits(null, refreshId, null, null, null);
    }

    public List<PipelineStepAudit> getPipelineStepAudits(Long pipelineStepId, String workflowExecutionId) {
        return getPipelineStepAudits(null, null, pipelineStepId, null,  workflowExecutionId);
    }

    public Map<String, Map<Long, Resources>> getLatestWorkflowResources(Long workflowId, Long refreshId) {
        AtomicReference<List<PipelineStepAuditEntity>> pipelineStepAuditListAtomicRef = new AtomicReference<>();
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                RequestEntity requestEntity = requestDAO.get(refreshId);
                Optional<RequestEntity> latestSuccessRequest = requestDAO.getLatestSuccessFullRequest(null, requestEntity.getWorkflowId());
                if (latestSuccessRequest.isPresent()) {
                    String workflowExecutionId = workflowAuditDAO.getLatestWorkflowExecutionId(workflowId, latestSuccessRequest.get().getId());
                    List<PipelineStepAuditEntity> pipelineStepAuditEntityList = pipelineStepAuditDAO.getPipelineStepAuditsByWorkflowExecutionId(workflowExecutionId);
                    pipelineStepAuditListAtomicRef.set(pipelineStepAuditEntityList);
                }
            }
        });

        if (pipelineStepAuditListAtomicRef.get() == null)
            throw new IllegalArgumentException("No Latest Successful Request found.");
        return preparePipelineStepMap(pipelineStepAuditListAtomicRef.get());
    }

    private Map<String, Map<Long, Resources>> preparePipelineStepMap(List<PipelineStepAuditEntity> pipelineStepAuditList) {
        Map<String, Map<Long, Resources>> pipelineResourceMap = new HashMap<>();
        pipelineStepAuditList.forEach(pipelineStepAudit -> {
            final String scope = pipelineStepAudit.getScope();
            final long pipelineStepId = pipelineStepAudit.getPipelineStepId();
            final Resources resources = JsonUtils.DEFAULT.fromJson(pipelineStepAudit.getResources(), Resources.class);
            if (pipelineResourceMap.containsKey(scope)) {
                pipelineResourceMap.get(scope).put(pipelineStepId, resources);
            } else {
                Map<Long, Resources> resourcesMap = new HashMap<Long, Resources>() {
                    {
                        put(pipelineStepId, resources);
                    }
                };
                pipelineResourceMap.put(scope, resourcesMap);
            }
        });
        return pipelineResourceMap;
    }

    public Map<PipelineStepStatus, Long> getPipelineStepAuditStatusMap(String workflowExecId) {
        List<PipelineStepAudit> pipelineStepAudits = getPipelineStepAudits(null, null, null, null, workflowExecId);
        if (pipelineStepAudits.size() == 0) throw new PipelineStepMissingException("No pipelineStepAudit found for Workflow Execution Id " + workflowExecId);

        Map<String, PipelineStepStatus> pipelineStepStatusMap = new HashMap<>();
        for (PipelineStepAudit pipelineStepAudit : pipelineStepAudits) {
            if (pipelineStepStatusMap.containsKey(pipelineStepAudit.getPipelineExecutionId()) &&
                    pipelineStepAudit.getPipelineStepStatus().equals(PipelineStepStatus.SUCCESS)) {
                pipelineStepStatusMap.put(pipelineStepAudit.getPipelineExecutionId(), PipelineStepStatus.SUCCESS);
            } else {
                pipelineStepStatusMap.put(pipelineStepAudit.getPipelineExecutionId(), pipelineStepAudit.getPipelineStepStatus());
            }
        }

        Map<PipelineStepStatus, Long> statusToCount = new HashMap<>();
        pipelineStepStatusMap.forEach((k, v) -> statusToCount.put(v, statusToCount.containsKey(v) ? statusToCount.get(v) + 1 : 1L));
        return statusToCount;
    }
}
