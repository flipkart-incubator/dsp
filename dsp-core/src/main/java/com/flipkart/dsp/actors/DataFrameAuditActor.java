package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.*;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.*;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameAuditStatus;
import com.flipkart.dsp.entities.sg.dto.*;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.models.Dataframe;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.flipkart.dsp.entities.sg.core.DataFrameAuditStatus.COMPLETED;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 */

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataFrameAuditActor implements SGActor<DataFrameAuditEntity, DataFrameAudit> {
    private final RequestDAO requestDAO;
    private final WorkflowDAO workflowDAO;
    private final DataFrameActor dataFrameActor;
    private final PipelineStepDAO pipelineStepDAO;
    private final DataFrameAuditDAO dataFrameAuditDAO;
    private final RequestDataframeAuditDAO requestDataframeAuditDAO;
    private final TransactionLender transactionLender;

    @Override
    public DataFrameAuditEntity unWrap(DataFrameAudit dto) {
        if (Objects.nonNull(dto)) {
            return DataFrameAuditEntity.builder().runId(dto.getRunId()).dataFrameEntity(dataFrameActor.unWrap(dto.getDataFrame()))
                    .status(dto.getDataFrameAuditStatus()).payload(dto.getPayload()).dataFrameConfig(dto.getDataFrameConfig())
                    .dataframeSize(dto.getDataframeSize()).overrideAuditId(dto.getOverrideAuditId())
                    .logAuditId(dto.getLogAuditId()).partitions(dto.getPartitions()).build();
        }
        return null;
    }

    @Override
    public DataFrameAudit wrap(DataFrameAuditEntity entity) {
        if (Objects.nonNull(entity)) {
            return DataFrameAudit.builder().runId(entity.getRunId()).dataframeSize(entity.getDataframeSize())
                    .overrideAuditId(entity.getOverrideAuditId())
                    .dataFrame(dataFrameActor.wrap(entity.getDataFrameEntity())).payload(entity.getPayload())
                    .dataFrameConfig(entity.getDataFrameConfig()).dataFrameAuditStatus(entity.getStatus())
                    .logAuditId(entity.getLogAuditId()).partitions(entity.getPartitions()).build();
        }
        return null;
    }

    public Map<String, Long> getDataFrameRunIds(List<Long> dataFrameIds) throws DSPCoreException {
        List<DataFrameAuditEntity> dataFrameAudits = dataFrameAuditDAO.getDataFrameAudits(dataFrameIds, COMPLETED);
        if (dataFrameAudits.size() == 0)
            throw new DSPCoreException("No DataFrame Audits found for dataFrameIds: " + dataFrameIds);

        dataFrameAudits.sort(Comparator.comparing(DataFrameAuditEntity::getUpdatedAt).reversed());
        Map<String, Long> dataFrameRunIdMap = new HashMap<>();
        for (Long dataFrameId : dataFrameIds) {
            DataFrameAuditEntity dataFrameAudit = dataFrameAudits.stream().filter(entity -> entity.getDataFrameEntity()
                    .getId().equals(dataFrameId)).findFirst().orElse(null);
            if (Objects.isNull(dataFrameAudit))
                throw new DSPCoreException("No Successful DataFrameAudit Found for id: " + dataFrameId);
            dataFrameRunIdMap.put(dataFrameAudit.getDataFrameEntity().getName(), dataFrameAudit.getRunId());
        }
        return dataFrameRunIdMap;
    }

    public DataFrameAudit persist(DataFrameAudit dataframeAudit) {
        AtomicReference<DataFrameAuditEntity> dataFrameAuditAtomicReference = new AtomicReference<>(null);

        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                dataFrameAuditAtomicReference.set(dataFrameAuditDAO.persist(unWrap(dataframeAudit)));
            }
        }, "Error While persisting DataFrame Audit. Entity: " + JsonUtils.DEFAULT.toJson(dataframeAudit));
        DataFrameAuditEntity dataFrameAuditEntity = unWrap(dataframeAudit);
        return wrap(dataFrameAuditAtomicReference.get());
    }

    public DataFrameAudit getDataFrameAuditById(Long runId) {
        AtomicReference<DataFrameAuditEntity> dataFrameAuditAtomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                dataFrameAuditAtomicReference.set(dataFrameAuditDAO.get(runId));
            }
        }, "Error while getting dataFrame Audit by Id. runId :" + runId);

        return wrap(dataFrameAuditAtomicReference.get());
    }

    public DataFrameAudit getDataFrameAudit(Long dataFrameId, Long dataframeOverrideAuditId, String partitions) {
        AtomicReference<DataFrameAuditEntity> auditEntityAtomicReference = new AtomicReference<>();
        String errorMessage = "Failed to fetch dataFrame audit entry for dataframeId " + dataFrameId +
                (Objects.nonNull(dataframeOverrideAuditId) ? ", dataframeOverrideAuditId " + dataframeOverrideAuditId : "") +
                (Objects.nonNull(partitions) ? ", partitions " + partitions : "") + ".";
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                auditEntityAtomicReference.set(dataFrameAuditDAO.getDataFrameAudit(dataframeOverrideAuditId, dataFrameId, partitions));
            }
        }, errorMessage);
        return wrap(auditEntityAtomicReference.get());
    }

    Optional<DataFrameAuditEntity> getLatestSuccessfulRunIdForDataFrame(Long dataframeId) {
        AtomicReference<Optional<DataFrameAuditEntity>> dataFrameAuditAtomicReference = new AtomicReference<>(null);

        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                dataFrameAuditAtomicReference.set(dataFrameAuditDAO.getLatestDataFrameAudit(dataframeId, COMPLETED));
            }
        });
        return dataFrameAuditAtomicReference.get();
    }

    public Set<DataFrameAudit> getDataframeAudits(Long requestId, Long workflowId, Long pipelineStepId) {
        AtomicReference<RequestEntity> requestAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                requestAtomicReference.set(requestDAO.get(requestId));
            }
        });
        RequestEntity requestEntity = requestAtomicReference.get();
        List<DataFrameAudit> dataFrameAudits = requestEntity.getRequestDataframeAudits().stream()
                .filter(r -> (r.getWorkflowEntity().getId().equals(workflowId) && r.getPipelineStepEntity().getId().equals(pipelineStepId)))
                .map(RequestDataframeAuditEntity::getDataFrameAuditEntity)
                .map(this::wrap)
                .collect(toList());

        return deduplicateDataframeAudits(dataFrameAudits);
    }

    private Set<DataFrameAudit> deduplicateDataframeAudits(List<DataFrameAudit> dataFrameAudits) {
        Map<String, DataFrameAudit> dataFrameAuditMap = new HashMap<>();
        dataFrameAudits.forEach((d) -> dataFrameAuditMap.put(d.getDataFrame().getName(), d));
        return new HashSet<>(dataFrameAuditMap.values());
    }

    public Map<String, List<String>> saveRequestDataFrameAudits(Long requestId, Long workflowId, Set<DataFrameAudit> dataFrameAudits, Long pipelineStepId) {
        AtomicReference<Map<String, List<String>>> dataFramePartitions = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() throws Exception {
                RequestEntity requestEntity = requestDAO.get(requestId);
                List<DataFrameAuditEntity> dataFrameAuditEntities = dataFrameAudits
                        .stream().map(d -> dataFrameAuditDAO.get(d.getRunId())).collect(toList());
                Long executionLogId = dataFrameAudits.iterator().next().getLogAuditId();
                dataFrameAuditDAO.updateLogId(dataFrameAuditEntities, executionLogId);
                final Map<Long, DataFrameAuditEntity> dataFrameAuditEntityMap = dataFrameAuditEntities.stream()
                        .collect(toMap(DataFrameAuditEntity::getRunId, Function.identity()));
                WorkflowEntity workflowEntity = workflowDAO.get(workflowId);
                PipelineStepEntity pipelineStepEntity = pipelineStepDAO.getPipelineStepById(pipelineStepId);

                List<RequestDataframeAuditEntity> requestDataframeAudits = dataFrameAuditEntities
                        .stream().map(auditEntity -> new RequestDataframeAuditEntity(requestEntity, auditEntity,
                                workflowEntity, pipelineStepEntity))
                        .collect(toList());
                List<RequestDataframeAuditEntity> duplicateRequestDataframeAudits = new ArrayList<>();
                requestEntity.getRequestDataframeAudits().forEach(requestDataframeAudit -> {
                    final Long dataframeRunId = requestDataframeAudit.getDataFrameAuditEntity().getRunId();
                    if (dataFrameAuditEntityMap.containsKey(dataframeRunId)) {
                        duplicateRequestDataframeAudits.add(requestDataframeAudit);
                    }
                });
                Map<String, List<String>> dataframePartitionMapping = getDataframePartition(requestDataframeAudits);

                requestDataframeAudits.removeAll(duplicateRequestDataframeAudits);
                requestEntity.getRequestDataframeAudits().addAll(requestDataframeAudits);
                requestDAO.persist(requestEntity);
                dataFramePartitions.set(dataframePartitionMapping);
            }
        });
        return dataFramePartitions.get();
    }

    private Map<String, List<String>> getDataframePartition(List<RequestDataframeAuditEntity> requestDataframeAudits) {
        try {
            Map<String, List<String>> dataframePartitionMapping = new HashMap<>();
            for (RequestDataframeAuditEntity requestDataframeAudit : requestDataframeAudits) {
                SGUseCasePayload payload = requestDataframeAudit.getDataFrameAuditEntity().getPayload();
                if (payload != null) {
                    Map<List<DataFrameKey>, Set<String>> dataframes = payload.getDataframes();
                    List<String> partitions = new ArrayList<>();
                    dataframes.forEach((key, value) -> {
                        key.forEach(dataFrameKey -> {
                            if (dataFrameKey instanceof DataFrameMultiKey) {
                                partitions.add(((DataFrameMultiKey) dataFrameKey).getValues().toString());
                            } else if (dataFrameKey instanceof DataFrameUnaryKey) {
                                partitions.add(((DataFrameUnaryKey) dataFrameKey).getValue());
                            } else if (dataFrameKey instanceof DataFrameBinaryKey) {
                                partitions.add(((DataFrameBinaryKey) dataFrameKey).getFirstValue() + "#" +
                                        ((DataFrameBinaryKey) dataFrameKey).getSecondValue());
                            }
                        });
                    });
                    dataframePartitionMapping.put(requestDataframeAudit.getDataFrameAuditEntity().getDataFrameEntity().getName(),
                            partitions);
                }
            }
            return dataframePartitionMapping;
        } catch (Exception e) {
            log.error("Unable to find partitions for dataframe " + e.getMessage());
        }
        return null;
    }

    public void updateDataFrameAuditStatus(Long requestId, Long workflowId, DataFrameAuditStatus currentStatus, DataFrameAuditStatus newStatus) {
        List<DataFrameAuditEntity> dataFrameAuditEntityList = requestDataframeAuditDAO.getDataFrameAudit(requestId, workflowId);
        dataFrameAuditEntityList.stream()
                .filter(dataFrameAuditEntity -> dataFrameAuditEntity.getStatus().equals(currentStatus))
                .forEach(dataFrameAuditEntity -> {
                    dataFrameAuditEntity.setStatus(newStatus);
                    dataFrameAuditDAO.persist(dataFrameAuditEntity);
                });
    }
}
