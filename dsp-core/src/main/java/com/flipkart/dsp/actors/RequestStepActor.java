package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.RequestDAO;
import com.flipkart.dsp.dao.RequestStepAuditDAO;
import com.flipkart.dsp.dao.RequestStepDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.RequestEntity;
import com.flipkart.dsp.db.entities.RequestStepEntity;
import com.flipkart.dsp.entities.enums.RequestStepType;
import com.flipkart.dsp.entities.request.RequestStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RequestStepActor implements SGActor<RequestStepEntity, RequestStep> {

    private final RequestDAO requestDAO;
    private final RequestStepDAO requestStepDAO;
    private final TransactionLender transactionLender;
    private final RequestStepAuditDAO requestStepAuditDAO;

    @Override
    public RequestStepEntity unWrap(RequestStep dto) {
        if (Objects.nonNull(dto)) {
            return RequestStepEntity.builder().id(dto.getId()).requestStepType(dto.getRequestStepType())
                    .requestEntity(requestDAO.get(dto.getRequestId())).jobName(dto.getJobName()).build();
        }
        return null;
    }

    @Override
    public RequestStep wrap(RequestStepEntity requestStepEntity) {
        if (Objects.nonNull(requestStepEntity)) {
            return RequestStep.builder().id(requestStepEntity.getId()).requestStepType(requestStepEntity.getRequestStepType())
                    .requestId(requestStepEntity.getRequestEntity().getId())
                    .jobName(requestStepEntity.getJobName()).build();
        }
        return null;
    }

    public RequestStep createRequestStep(Long requestId, RequestStepType requestStepType, String jobName) {
        AtomicReference<RequestStepEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                RequestEntity requestEntity = requestDAO.get(requestId);
                RequestStepEntity requestStepEntity = RequestStepEntity.builder().requestEntity(requestEntity)
                        .requestStepType(requestStepType).jobName(jobName).build();
                atomicReference.set(requestStepDAO.persist(requestStepEntity));
            }
        });
        return wrap(atomicReference.get());
    }

    public RequestStep getRequestStep(Long requestStepId) {
        AtomicReference<RequestStepEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(requestStepDAO.get(requestStepId));
            }
        }, "Error while getting requestStepId: " + requestStepId);
        return wrap(atomicReference.get());
    }


}
