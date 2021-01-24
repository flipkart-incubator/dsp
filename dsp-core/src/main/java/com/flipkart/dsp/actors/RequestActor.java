package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.*;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.*;
import com.flipkart.dsp.entities.enums.RequestStepAuditStatus;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RequestActor implements SGActor<RequestEntity, Request> {
    private final UserActor userActor;
    private final RequestDAO requestDAO;
    private final RequestStepDAO requestStepDAO;
    private final TransactionLender transactionLender;
    private final RequestStepAuditDAO requestStepAuditDAO;

    @Override
    public RequestEntity unWrap(Request dto) {
        if (Objects.nonNull(dto)) {
            return RequestEntity.builder().id(dto.getId()).requestId(dto.getRequestId())
                    .workflowId(dto.getWorkflowId()).data(dto.getData()).azkabanExecId(dto.getAzkabanExecId())
                    .requestStatus(dto.getRequestStatus()).callbackUrl(dto.getCallbackUrl())
                    .workflowDetailsSnapshot(JsonUtils.DEFAULT.toJson(dto.getWorkflowDetails()))
                    .isNotified(dto.getIsNotified()).userEntity(userActor.getUserByName(dto.getTriggeredBy())).build();
        }
        return null;
    }

    @Override
    public Request wrap(RequestEntity entity) {
        if (Objects.nonNull(entity)) {
            String userId = Objects.isNull(entity.getUserEntity()) ? null : entity.getUserEntity().getUserId();
            return Request.builder().id(entity.getId()).requestId(entity.getRequestId())
                    .workflowId(entity.getWorkflowId()).triggeredBy(userId)
                    .azkabanExecId(entity.getAzkabanExecId()).callbackUrl(entity.getCallbackUrl())
                    .createdAt(entity.getCreatedAt()).updatedAt(entity.getUpdatedAt())
                    .isNotified(entity.getIsNotified()).data(entity.getData()).requestStatus(entity.getRequestStatus())
                    .workflowDetails(JsonUtils.DEFAULT.fromJson(entity.getWorkflowDetailsSnapshot(), WorkflowDetails.class)).build();
        }
        return null;
    }

    public Request getRequest(Long requestId) {
        AtomicReference<RequestEntity> requestAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                requestAtomicReference.set(requestDAO.get(requestId));
            }
        }, "Error while getting Request for id: " + requestId);
        return wrap(requestAtomicReference.get());
    }

    public List<Request> getActiveRequests(RequestStatus requestStatus, Integer maxSize) {
        AtomicReference<List<RequestEntity>> listAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                listAtomicReference.set(requestDAO.getRequestsWithStatus(requestStatus, maxSize));
            }
        }, "Error while getting Requests for status: " + requestStatus.name());
        return listAtomicReference.get().stream().map(this::wrap).collect(Collectors.toList());
    }

    public void updateRequestStatus(Request request, RequestStatus requestStatus) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                RequestEntity requestEntity = requestDAO.get(request.getId());
                if (requestStatus.equals(RequestStatus.FAILED))
                    failActiveAudits(request.getId());
                requestEntity.setRequestStatus(requestStatus);
                requestDAO.persist(requestEntity);
            }
        });
    }

    private void failActiveAudits(Long requestId) {
        List<RequestStepEntity> requestStepEntities = requestStepDAO.getAllStepIdsForRequest(requestId);
        for (RequestStepEntity requestStepEntity : requestStepEntities) {
            //sorted by updated_at desc
            List<RequestStepAuditEntity> requestStepAuditEntities = requestStepAuditDAO.getAllAuditsForRequestStep(requestStepEntity.getId());
            if (requestStepAuditEntities.size() != 0
                    && !requestStepAuditEntities.get(0).getStatus().equals(RequestStepAuditStatus.SUCCESSFUL)) {
                RequestStepAuditEntity requestStepAuditEntity = RequestStepAuditEntity.builder().status(RequestStepAuditStatus.FAILED)
                        .requestStepEntity(requestStepEntity).build();
                requestStepAuditDAO.persist(requestStepAuditEntity);
            }
        }
    }

    public RequestEntity save(RequestEntity entity) {
        final AtomicReference<RequestEntity> requestAtomicReference = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                requestAtomicReference.set(requestDAO.persist(entity));
            }
        });
        return requestAtomicReference.get();
    }

    public void save(Request request) {
        save(unWrap(request));
    }

    void updateRequestNotificationStatus(Long requestId, RequestStatus requestStatus, Object workflowExecutionPayload) {
        final AtomicReference<RequestEntity> requestAtomicReference = new AtomicReference<>(null);
        assert requestAtomicReference.get() != null : "Update of RequestEntity unsuccessful";
    }

    private void updateRequestOverride(ExecuteWorkflowRequest executeRequest, Map<String, Long> tableRefreshMap) {
        if (executeRequest.getRequestOverride() == null) return;
        Map<String/*dataframeId*/, DataframeOverride> dataframeOverrideMap = executeRequest.getRequestOverride().getDataframeOverrideMap();
        for (String dataframe : dataframeOverrideMap.keySet()) {
            DataframeOverride dataframeOverride = dataframeOverrideMap.get(dataframe);
            if (dataframeOverride == null) continue;
        }
    }

    public RequestEntity getLatestSuccessfulRequest(Long requestId) {
        AtomicReference<Optional<RequestEntity>> requestAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                requestAtomicReference.set(requestDAO.getLatestSuccessFullRequest(requestId, null));
            }
        });
        return !requestAtomicReference.get().isPresent() ? null : requestAtomicReference.get().get();
    }

    public List<Request> getLatestRequests(Long workflowId, Integer limit) {
        AtomicReference<List<RequestEntity>> listAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                listAtomicReference.set(requestDAO.getLatestRequests(workflowId, limit));
            }
        }, "Error while getting Requests for workflowId: " + workflowId);
        return listAtomicReference.get().stream().map(this::wrap).collect(Collectors.toList());
    }
}
