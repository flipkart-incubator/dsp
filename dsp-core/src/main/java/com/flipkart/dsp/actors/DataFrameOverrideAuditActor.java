package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.DataFrameOverrideAuditDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.DataFrameOverrideAuditEntity;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideState;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataFrameOverrideAuditActor implements SGActor<DataFrameOverrideAuditEntity, DataFrameOverrideAudit> {
    private final TransactionLender transactionLender;
    private final DataFrameOverrideAuditDAO dataFrameOverrideAuditDAO;

    @Override
    public DataFrameOverrideAuditEntity unWrap(DataFrameOverrideAudit dto) {
        if (Objects.nonNull(dto))
            return DataFrameOverrideAuditEntity.builder().id(dto.getId()).requestId(dto.getRequestId())
                    .workflowId(dto.getWorkflowId()).dataframeId(dto.getDataframeId()).isDeleted(dto.getIsDeleted())
                    .inputDataId(dto.getInputDataId()).inputMetadata(dto.getInputMetadata()).outputMetadata(dto.getOutputMetadata())
                    .state(dto.getState()).dataFrameOverrideType(dto.getDataFrameOverrideType())
                    .expiresAt(dto.getExpiresAt()).purgePolicyId(dto.getPurgePolicyId()).build();
        return null;
    }

    @Override
    public DataFrameOverrideAudit wrap(DataFrameOverrideAuditEntity entity) {
        if (Objects.nonNull(entity))
            return DataFrameOverrideAudit.builder().id(entity.getId()).requestId(entity.getRequestId())
                    .workflowId(entity.getWorkflowId()).dataframeId(entity.getDataframeId())
                    .isDeleted(entity.getIsDeleted()).inputDataId(entity.getInputDataId())
                    .inputMetadata(entity.getInputMetadata()).outputMetadata(entity.getOutputMetadata())
                    .state(entity.getState()).dataFrameOverrideType(entity.getDataFrameOverrideType())
                    .expiresAt(entity.getExpiresAt()).purgePolicyId(entity.getPurgePolicyId()).build();
        return null;
    }

    public List<DataFrameOverrideAuditEntity> getByRequestId(Long requestId) {
        AtomicReference<List<DataFrameOverrideAuditEntity>> dataframeOverrideAuditAtomicReference = new AtomicReference<>();
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() throws Exception {
                dataframeOverrideAuditAtomicReference.set(dataFrameOverrideAuditDAO.getByRequestId(requestId));
            }
        }, "Error while querying for DataFrame Audits for requestId: " + requestId);
        return dataframeOverrideAuditAtomicReference.get();
    }

    public void updateStartedAudits(Long requestId) {
        AtomicReference<List<DataFrameOverrideAuditEntity>> dataframeOverrideAuditAtomicReference = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() throws Exception {
                dataframeOverrideAuditAtomicReference.set(dataFrameOverrideAuditDAO.getStartedByRequestId(requestId));
            }
        }, "Error while querying for DataFrame Audits for requestId: " + requestId);
        List<DataFrameOverrideAuditEntity> dataFrameOverrideAuditEntities = dataframeOverrideAuditAtomicReference.get();
        for (DataFrameOverrideAuditEntity entity : dataFrameOverrideAuditEntities){
            entity.setState(DataFrameOverrideState.FAILED);
        }
        save(dataFrameOverrideAuditEntities);
    }

    public void save(List<DataFrameOverrideAuditEntity> dataFrameOverrideAuditEntityList) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() throws Exception {
                dataFrameOverrideAuditEntityList.forEach(dataFrameOverrideAuditDAO::persist);
            }
        }, "Error while saving DataFrame Override Audits.");
    }


    public DataFrameOverrideAudit save(DataFrameOverrideAudit dataframeOverrideAudit) {
        AtomicReference<DataFrameOverrideAuditEntity> dataframeOverrideAuditAtomicReference = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() throws Exception {
                dataframeOverrideAuditAtomicReference.set(dataFrameOverrideAuditDAO.persist(unWrap(dataframeOverrideAudit)));
            }
        }, "Error while saving DataFrameOverride Audit");
        return wrap(dataframeOverrideAuditAtomicReference.get());
    }

    public DataFrameOverrideAudit getDataFrameOverrideAudit(Long requestId, Long dataFrameId,
                                                            String inputDataId, DataFrameOverrideType dataFrameOverrideType) {
        AtomicReference<DataFrameOverrideAuditEntity> dataframeOverrideAuditAtomicReference = new AtomicReference<>();
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                dataframeOverrideAuditAtomicReference.set(dataFrameOverrideAuditDAO.getDataFrameOverrideAudit(requestId,
                        inputDataId, dataFrameId, dataFrameOverrideType));
            }
        }, "Error while getting DataFrame Override Audit id for dataFrameId: " + dataFrameId);
        return wrap(dataframeOverrideAuditAtomicReference.get());
    }


    public DataFrameOverrideAudit getDataFrameOverrideAudit(Long dataFrameOverrideAuditId) {
        AtomicReference<DataFrameOverrideAuditEntity> dataframeOverrideAuditAtomicReference = new AtomicReference<>();
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                dataframeOverrideAuditAtomicReference.set(dataFrameOverrideAuditDAO.get(dataFrameOverrideAuditId));
            }
        }, "Error while getting DataFrame Override Audit id for id: " + dataFrameOverrideAuditId);

        return wrap(dataframeOverrideAuditAtomicReference.get());
    }
}
