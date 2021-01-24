package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.RequestEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.models.RequestStatus;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.sql.Timestamp;
import java.util.*;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;


/**
 */
public class RequestDAO extends AbstractDAO<RequestEntity> {

    @Inject
    public RequestDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<RequestEntity> getRequestsWithStatus(RequestStatus requestStatus, Integer maxSize) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("requestStatus").paramValues(requestStatus).predicateType(EQUAL).build());
        return createQuery(RequestEntity.class, sqlParams).setMaxResults(maxSize).list();
    }

    public RequestEntity updateRequestNotificationStatus(Long requestId, Boolean isNotified, RequestStatus requestStatus,
                                                         String response, String varadhiResponse) {
        RequestEntity requestEntity = get(requestId);
        if (requestEntity != null) {
            requestEntity.setRequestStatus(requestStatus);
            requestEntity.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
            requestEntity.setIsNotified(isNotified);
            requestEntity.setResponse(response);
            requestEntity.setVaradhiResponse(varadhiResponse);
            return persist(requestEntity);
        }
        return null;
    }

    public Optional<RequestEntity> getLatestSuccessFullRequest(Long requestId, Long workflowId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("id").paramValues(requestId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("workflowId").paramValues(workflowId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("requestStatus").paramValues(RequestStatus.COMPLETED).predicateType(EQUAL).build());
        Map<String, String> orderMap = new HashMap<>();
        orderMap.put("id", "desc");
        List<RequestEntity> requestEntities = createQuery(RequestEntity.class, sqlParams, orderMap).setMaxResults(1).list();
        return requestEntities.isEmpty() ? Optional.empty() : Optional.of(requestEntities.get(0));
    }

    public List<RequestEntity> getLatestRequests(Long workflowId, Integer limit) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("workflowId").paramValues(workflowId).predicateType(EQUAL).build());
        Map<String, String> orderMap = new HashMap<>();
        orderMap.put("id", "desc");
        return createQuery(RequestEntity.class, sqlParams, orderMap).setMaxResults(limit).list();
    }

}
