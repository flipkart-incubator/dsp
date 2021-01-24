package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.RequestStepAuditDAO;
import com.flipkart.dsp.dao.RequestStepDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.RequestStepAuditEntity;
import com.flipkart.dsp.db.entities.RequestStepEntity;
import com.flipkart.dsp.entities.enums.RequestStepAuditStatus;
import com.flipkart.dsp.entities.request.RequestStep;
import com.flipkart.dsp.entities.request.RequestStepAudit;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.Collectors.toList;

@Slf4j
@Singleton
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RequestStepAuditActor implements SGActor<RequestStepAuditEntity, RequestStepAudit> {
    private final RequestStepDAO requestStepDAO;
    private final RequestStepActor requestStepActor;
    private final TransactionLender transactionLender;
    private final RequestStepAuditDAO requestStepAuditDAO;

    @Override
    public RequestStepAuditEntity unWrap(RequestStepAudit requestStepAudit) {
        if (Objects.nonNull(requestStepAudit)) {
            RequestStepEntity requestStepEntity = requestStepDAO.get(requestStepAudit.getRequestStepId());
            return RequestStepAuditEntity.builder().id(requestStepAudit.getId()).metaData(requestStepAudit.getMetaData())
                    .requestStepEntity(requestStepEntity).status(requestStepAudit.getRequestStepAuditStatus()).build();
        }
        return null;
    }

    @Override
    public RequestStepAudit wrap(RequestStepAuditEntity requestStepAuditEntity) {
        if (Objects.nonNull(requestStepAuditEntity)) {
            return RequestStepAudit.builder().id(requestStepAuditEntity.getId()).metaData(requestStepAuditEntity.getMetaData())
                    .requestStepId(requestStepAuditEntity.getRequestStepEntity().getId())
                    .requestStepAuditStatus(requestStepAuditEntity.getStatus())
                    .requestStepType(requestStepAuditEntity.getRequestStepEntity().getRequestStepType()).build();
        }
        return null;
    }

    public void createRequestStepAudit(RequestStep requestStep, RequestStepAuditStatus status, String metaData) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                RequestStepAuditEntity requestStepAuditEntity = RequestStepAuditEntity.builder().metaData(metaData)
                        .requestStepEntity(requestStepActor.unWrap(requestStep)).status(status).build();
                requestStepAuditDAO.persist(requestStepAuditEntity);
            }
        });
    }

    public RequestStepAudit createRequestStepAudit(RequestStepAudit requestStepAudit) {
        AtomicReference<RequestStepAuditEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(requestStepAuditDAO.persist(unWrap(requestStepAudit)));
            }
        });
        return wrap(atomicReference.get());
    }

    public RequestStepAudit getRequestStepAuditById(Long requestStepAuditId) {
        AtomicReference<RequestStepAudit> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(wrap(requestStepAuditDAO.get(requestStepAuditId)));
            }
        });
        return atomicReference.get();
    }

    public RequestStepAudit getRequestStepAuditByJobName(Long requestId, String jobName) {
        final AtomicReference<RequestStepAuditEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                Long requestStepId = requestStepDAO.getLatestRequestStepByJobName(requestId, jobName).getId();
                atomicReference.set(requestStepAuditDAO.getLatestSuccessfulAuditForRequestStep(requestStepId));
            }
        });
        return wrap(atomicReference.get());
    }

    void updateRequestStepAuditMeta(Long requestStepAuditId, String metaData) {
        final AtomicReference<RequestStepAuditEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                RequestStepAuditEntity requestStepAuditEntity = requestStepAuditDAO.get(requestStepAuditId);
                if (Objects.nonNull(requestStepAuditEntity)) {
                    requestStepAuditEntity.setMetaData(metaData);
                    atomicReference.set(requestStepAuditDAO.persist(requestStepAuditEntity));
                }
            }
        }, "failed while updating");
        assert !Objects.isNull(atomicReference.get()) : "Failed to update audit, No requestStepAudit found for id: " + requestStepAuditId;
    }

    public List<RequestStepAudit> getRequestStepAudits(Long requestId) {
        AtomicReference<List<RequestStepAudit>> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                List<RequestStepEntity> requestStepEntities = requestStepDAO.getAllStepIdsForRequest(requestId);
                List<Long> requestStepIds = requestStepEntities.stream().map(RequestStepEntity::getId).collect(toList());
                List<RequestStepAudit> requestStepAudits = requestStepAuditDAO.getAllAuditsForRequest(requestStepIds)
                        .stream().map(requestStepAudit -> wrap(requestStepAudit)).collect(toList());
                atomicReference.set(requestStepAudits);
            }
        });
        return atomicReference.get();
    }

    public RequestStepAuditStatus getLatestRequestStepAuditStatus(Long requestStepId) {
        AtomicReference<List<RequestStepAuditEntity>> listAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                listAtomicReference.set(requestStepAuditDAO.getAllAuditsForRequestStep(requestStepId));
            }
        });
        if (listAtomicReference.get().size() == 0)
            throw new DSPCoreException("No Audits found for requestStepId: " + requestStepId);
        return listAtomicReference.get().get(0).getStatus();
    }
}
